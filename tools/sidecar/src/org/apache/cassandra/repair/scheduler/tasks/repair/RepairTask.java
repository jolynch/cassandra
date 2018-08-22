/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.repair.scheduler.tasks.repair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.NotificationListener;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.scheduler.TaskDaoManager;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairStatusDao;
import org.apache.cassandra.repair.scheduler.entity.LocalRepairState;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.entity.TableRepairRangeContext;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.apache.cassandra.repair.scheduler.tasks.BaseTask;
import org.joda.time.DateTime;

/**
 * Repair Controller is the glue between TaskManager and RepairDaos, Cassandra Interaction classes.
 * This class is the control plane for repair scheduler. Repair Controller holds all the definitions
 * for State Machine (Refer to CASSANDRA-14346). This is the backbone for repair scheduler.
 */
public class RepairTask extends BaseTask
{
    private static final Logger logger = LoggerFactory.getLogger(RepairTask.class);

    public RepairTask(TaskSchedulerContext context, TaskDaoManager daoManager)
    {
        super(context, daoManager);

        logger.debug("RepairTask Initialized.");
    }

    /**
     * Given that no other nodes in the cluster are running repair (that is checked by isSequenceRunningOnCluster.
     * can this particular node start repairing? We cannot proceed at this time if:
     *  <ul>
     *    <li> 1. The local cassandra is currently running repairs (e.g. we crashed or a manual repair was run)</li>
     *    <li> 2. Repair is paused on this cluster </li>
     *  </ul>
     * @param taskId RepairId to check the status for
     * @return true or false indicating whether repair can run or not
     */
    public boolean canRunTask(int taskId)
    {
        // Check if Repair is running on current node? If Yes: Return False, (Check from JMX)
        if (cassInteraction.isRepairRunning(false))
        {
            return false;
        }
        return !isSequencePausedOnCluster(taskId);
    }

    /**
     * Checks if the current node is next in repair, is already repairing, or is done with repair.
     * It queries repair_sequence(node level status) table to see the status.
     * It also aborts the repair on stuck instance when it is not next in the sequence.
     * @param repairId RepairId to check
     * @return Returns the repair sequence object if it is next, currently running, or already done sequence
     */
    public Optional<RepairSequence> amINextInSequenceOrDone(int repairId)
    {
        SortedSet<RepairSequence> nonTerminalRepairSequence = getMatchingRepairSequences(
        repairId, rs -> !rs.getStatus().isTerminal());
        SortedSet<RepairSequence> finishedRepairSequences = getMatchingRepairSequences(
        repairId, rs -> rs.getStatus() == TaskStatus.FINISHED);

        String localHostId = cassInteraction.getLocalHostId();

        Optional<RepairSequence> myComplete = finishedRepairSequences.stream()
                                                                     .filter(rs -> StringUtils.isNotBlank(rs.getNodeId())
                                                                                   && rs.getNodeId().equalsIgnoreCase(localHostId))
                                                                     .findFirst();
        if (myComplete.isPresent())
        {
            return myComplete;
        }

        Optional<RepairSequence> next = nonTerminalRepairSequence.stream().findFirst();
        // Am I the next node in the sequence to be repaired
        if (next.isPresent() &&
            StringUtils.isNotBlank(next.get().getNodeId()) &&
            next.get().getNodeId().equalsIgnoreCase(localHostId))
        {
            TaskStatus myStatus = next.get().getStatus();
            if (!myStatus.isPaused() && !myStatus.isCancelled())
            {
                return next;
            }
            else
            {
                logger.info("Would repair but unable to start since I am paused or cancelled", next);
            }
        }

        getStuckSequence(repairId).ifPresent(this::abortTaskOnStuckSequence);

        return Optional.empty();
    }

    /**
     * Marks repair started on one instance
     * @param repairId repair id
     * @param seq node / instance information
     */
    public void prepareForTaskOnNode(int repairId, RepairSequence seq)
    {
        daoManager.getRepairSequenceDao().markRepairStartedOnInstance(repairId, seq.getSeq());
    }

    /**
     * Cancels repair on current node. If the given instance information is not current node,
     * it updates the sequence and status tables as cancelled
     * @param taskId repair id
     * @param reason The reason for canceling the task
     * @param seq node information
     * @return true or false indicating the repair cancel status
     */
    public boolean cancelTaskOnNode(int taskId, String reason, RepairSequence seq)
    {
        boolean cancelled;
        RepairSchedulerMetrics.instance.incNumTimesRepairCancelled();

        if (seq.getNodeId().equalsIgnoreCase(cassInteraction.getLocalHostId()))
            cassInteraction.cancelAllRepairs();

        // Cancel on status and sequence data items
        cancelled = daoManager.getRepairSequenceDao().cancelRepairOnNode(taskId, seq.getSeq());
        cancelled &= daoManager.getRepairStatusDao().markRepairCancelled(taskId, seq.getNodeId());

        return cancelled;
    }

    /**
     * Gets repair neighboring nodes/ tokens/ instances for a given keyspace. Note that every keyspace might
     * have a different neighbors depending on their token range data.
     * @param keyspace Name of the keyspace
     * @return Neighoring instances
     */
    private Set<String> getPrimaryRepairNeighborEndpoints(String keyspace)
    {

        String localEndpoint = cassInteraction.getLocalEndpoint();

        Map<Range<Token>, List<String>> tokenRangesToEndpointMap = cassInteraction.getRangeToEndpointMap(keyspace);

        return tokenRangesToEndpointMap.entrySet().stream()
                                       .filter(tr -> tr.getValue().contains(localEndpoint))
                                       .map(tr -> tr.getValue().get(0))
                                       .distinct().collect(Collectors.toSet());
    }

    /**
     * Gets repair neighboring nodes/ tokens/ instances for a given keyspace and range. With virtual nodes each
     * range within each keyspace might have different neighbors. Before repairing each range we check health
     * of that range, which is where this method comes in.
     *
     * Note that unlike getPrimaryRepairNeighborEndpoints, this method does not only consider primary replica nodes,
     * (the ones responsible for repairing that range, it considers all replicas.
     *
     * @param keyspace Name of the keyspace
     * @param range The token range to get neighbors for
     *
     * @return Neighoring instances
     */
    private Set<String> getRepairNeighborEndpoints(String keyspace, Range<Token> range)
    {

        String localEndpoint = cassInteraction.getLocalEndpoint();

        Map<Range<Token>, List<String>> tokenRangesToEndpointMap = cassInteraction.getRangeToEndpointMap(keyspace);

        if (tokenRangesToEndpointMap.containsKey(range))
        {
            return new HashSet<>(tokenRangesToEndpointMap.get(range));
        }
        else
        {
            logger.warn("Range {} is not exactly contained in the token map for {}, falling back to whole keyspace",
                        range, keyspace);
            return tokenRangesToEndpointMap.entrySet().stream()
                                           .filter(tr -> tr.getValue().contains(localEndpoint))
                                           .flatMap(tr -> tr.getValue().stream())
                                           .distinct().collect(Collectors.toSet());
        }
    }

    /**
     * Checks if all the neighbors are healthy or not, this is used before starting the repair
     * @param keyspace keyspace specific neighbors are different, hence keyspace is needed
     * @return true/ false indicating the health
     */
    boolean areNeighborsHealthy(String keyspace, Range<Token> range)
    {
        Map<String, String> simpleEndpointStates = cassInteraction.getSimpleStates();

        Set<String> neighbors = getRepairNeighborEndpoints(keyspace, range);

        // TODO (vchella|2017-12-14): Consider other node status other than UP and DOWN for health check
        boolean healthy = neighbors.stream()
                                   .allMatch(endpoint -> simpleEndpointStates.getOrDefault(endpoint, "DOWN")
                                                                             .equalsIgnoreCase("UP"));
        if (!healthy)
            logger.warn("At least one neighbor out of {} is DOWN", neighbors);

        return healthy;
    }

    /**
     * Returns the neighboring _primary_ replicas of this local node. These are host ids which must successfully
     * run repair before we can e.g. run post repair hooks (since this local node may receive data from them)
     *
     * @return Set of node ids. NodeId in this context are HOST-IDs
     */
    private Set<String> getRepairNeighborNodeIds()
    {
        List<String> allKeyspaces = context.localSession().getCluster().getMetadata()
                                           .getKeyspaces()
                                           .stream()
                                           .map(KeyspaceMetadata::getName)
                                           .collect(Collectors.toList());
        Set<String> myNeighboringNodeIds = new HashSet<>();
        //HostId as a NodeID, have to map node ids to endpoints
        Map<String, String> endpointToHostIdMap = cassInteraction.getEndpointToHostIdMap();

        allKeyspaces.forEach(keyspace -> {
            Set<String> primaryEndpoints = getPrimaryRepairNeighborEndpoints(keyspace);

            Set<String> primaryEndpointHostIds = primaryEndpoints.stream()
                                                                 .map(endpointToHostIdMap::get)
                                                                 .collect(Collectors.toSet());
            myNeighboringNodeIds.addAll(primaryEndpointHostIds);
        });

        return myNeighboringNodeIds;
    }

    /**
     * Gets neighbors which are not completed their repair according to repair_sequence table
     * @param repairId Repair Id to check the status
     * @return List of Nodes which have not completed their repairs
     */
    private Set<String> getIncompleteRepairNeighbors(int repairId)
    {
        SortedSet<RepairSequence> repairSequence = daoManager.getRepairSequenceDao().getRepairSequence(repairId);
        Set<String> allRepairedNodes = repairSequence.stream()
                                                     .map(RepairSequence::getNodeId)
                                                     .collect(Collectors.toSet());
        Set<String> finishedNodes = repairSequence.stream()
                                                  .filter(repairSeq -> repairSeq.getStatus().isCompleted())
                                                  .map(RepairSequence::getNodeId)
                                                  .collect(Collectors.toSet());

        Set<String> myNeighboringNodeIds = getRepairNeighborNodeIds();
        myNeighboringNodeIds.removeAll(finishedNodes);

        // Remove any new neighbors which were not there when repair sequence was generated
        myNeighboringNodeIds.removeIf(ep -> !allRepairedNodes.contains(ep));
        return myNeighboringNodeIds;
    }

    /**
     * Check if repair is completed on entire cluster, Check if it has the Check if we are already running post repair hook, if not check if we just completed, if not check if it has failed and retried for more than 3 times
     * if not assume we are ready for post repair hook
     *
     * @param repairId Repair Id
     * @return true/ false indicating the readiness fore post repair hook
     */

    public boolean amIReadyForPostTaskHook(int repairId)
    {
        logger.info("Checking to see if this node is ready for post repair hook for repairId: {}", repairId);

        if (!isPostRepairHookCompleteOnCluster(repairId, cassInteraction.getLocalHostId()))
        {
            //TODO: Check if it has failed and retried for more than e.g. 3 times
            logger.debug("Post repair hook has not run on this node for repairId: {}", repairId);
            Set<String> incompleteRepairNeighbors = getIncompleteRepairNeighbors(repairId);
            if (incompleteRepairNeighbors.size() == 0)
            {
                logger.info("Repair is completed on all neighboring nodes, can run repair hook");
                return true;
            }
            else
            {
                logger.info("Repair hook cannot start yet, waiting on: {}", incompleteRepairNeighbors);
            }
        }
        else
        {
            logger.debug("Repair hook has already completed on this node for repairId: {}", repairId);
        }

        return false;
    }

    private boolean isPostRepairHookCompleteOnCluster(int repairId, String nodeId)
    {
        logger.info("Checking to see if this node's post repair hook is complete or not. RepairId: {}, nodeId: {}", repairId, nodeId);

        RepairMetadata repairStatus = daoManager.getRepairHookDao().getLocalRepairHookStatus(repairId, nodeId);
        if (repairStatus != null && repairStatus.getStatus() != null)
        {
            return repairStatus.getStatus().isCompleted();
        }
        return false;
    }

    boolean isPostRepairHookCompleteOnCluster(int repairId)
    {
        logger.info("Checking to see if post repair hook is complete on entire cluster for this RepairId: {}", repairId);

        Set<String> repairedNodes = daoManager.getRepairSequenceDao().getRepairSequence(repairId).stream()
                                              .map(RepairSequence::getNodeId)
                                              .collect(Collectors.toSet());

        Set<String> finishedHookNodes = daoManager.getRepairHookDao().getLocalRepairHookStatus(repairId).stream()
                                                  .filter(rm -> rm.getStatus().isCompleted())
                                                  .map(RepairMetadata::getNodeId)
                                                  .collect(Collectors.toSet());

        boolean isComplete = finishedHookNodes.containsAll(repairedNodes);
        logger.debug("Post repair hook status for entire cluster for this RepairId: {}, Status-isComplete: {}", repairId, isComplete);

        return isComplete;
    }

    List<TableTaskConfig> prepareForRepairHookOnNode(RepairSequence sequence)
    {
        List<TableTaskConfig> repairHookEligibleTables = daoManager.getRepairConfigDao()
                                                                   .getAllRepairEnabledTables(sequence.getScheduleName())
                                                                   .stream()
                                                                   .filter(TableTaskConfig::shouldRunPostTaskHook)
                                                                   .collect(Collectors.toList());
        RepairSchedulerMetrics.instance.incNumTimesRepairHookStarted();
        daoManager.getRepairHookDao().markLocalPostRepairHookStarted(sequence.getRepairId());

        logger.info("C* Load before starting post repair hook(s) for repairId {}: {}",
                    sequence.getRepairId(), cassInteraction.getLocalLoadString());

        return repairHookEligibleTables;
    }

    /**
     * Based on hook results, updates hook status table accordingly, logs appropriate information
     * @param sequence Local instance information
     * @param hookSuccess RepairHook statuses
     */
    void cleanupAfterHookOnNode(RepairSequence sequence, Map<String, Boolean> hookSuccess)
    {
        if (hookSuccess.values().stream().allMatch(v -> v))
        {
            RepairSchedulerMetrics.instance.incNumTimesRepairHookCompleted();
            daoManager.getRepairHookDao().markLocalPostRepairHookEnd(sequence.getRepairId(),
                                                                     TaskStatus.FINISHED, hookSuccess);
        }
        else
        {
            logger.error("Failed to run post repair hook(s) for repairId: {}", sequence.getRepairId());

            RepairSchedulerMetrics.instance.incNumTimesRepairHookFailed();
            daoManager.getRepairHookDao().markLocalPostRepairHookEnd(sequence.getRepairId(),
                                                                     TaskStatus.FAILED, hookSuccess);
        }

        logger.info("C* Load after completing post repair hook(s) for repairId {}: {}",
                    sequence.getRepairId(), cassInteraction.getLocalLoadString());
    }

    /**
     * Returns tables that are eligible for repair during this repairId. Note that this method
     * does **not** include tables that are already done for this repairId. If you want all repair
     * eligible tables use getRepairEligibleTables
     *
     * @param repairId Repair Id
     * @return Table Repair configrations
     */
    List<TableTaskConfig> getTablesForRepair(int repairId, String scheduleName)
    {
        // Get all tables available in this cluster to be repaired based on config and discovery
        List<TableTaskConfig> tableConfigList = daoManager.getRepairConfigDao().getAllRepairEnabledTables(scheduleName);
        Set<String> keyspaces = tableConfigList.stream()
                                               .map(TableTaskConfig::getKeyspace).collect(Collectors.toSet());

        Function<String, List<Range<Token>>> conv = keyspace -> cassInteraction.getTokenRanges(keyspace, true);
        Map<String, List<Range<Token>>> primaryRangesByKeyspace = keyspaces.stream()
                                                                           .collect(Collectors.toMap(k -> k, conv));

        // Get if any tables repair history for this repairId
        List<RepairMetadata> repairHistory = daoManager.getRepairStatusDao()
                                                       .getRepairHistory(repairId,
                                                                         cassInteraction.getLocalHostId());

        // Get tables that have some repair already scheduled/done for this repairId
        Map<String, List<RepairMetadata>> repairsByKsTb = repairHistory.stream()
                                                                       .collect(Collectors.groupingBy(RepairMetadata::getKsTbName));

        Map<String, Boolean> alreadyRepaired = repairsByKsTb.keySet().stream()
                                                            .collect(Collectors.toMap(Function.identity(), v -> false));

        // For each table, populate alreadyRepaired based on the ranges
        repairsByKsTb.forEach((ksTbName, rangeRepairs) -> {
            List<Range<Token>> completedRanges = rangeRepairs.stream()
                                                             .filter(rm -> rm.getStatus().isTerminal())
                                                             .map(rm -> cassInteraction.tokenRangeFromStrings(rm.getStartToken(), rm.getEndToken()))
                                                             .collect(Collectors.toList());
            // use Range.sort instead of .sorted above because this Range.sort sorts by left instead of right token.
            Range.sort(completedRanges);

            List<Range<Token>> matchingRanges = primaryRangesByKeyspace.getOrDefault(ksTbName.split("\\.")[0], new LinkedList<>());

            boolean anyRangeMissing = false;
            for (Range<Token> range : matchingRanges)
            {
                OptionalInt start = IntStream.range(0, completedRanges.size())
                                             .filter(i -> completedRanges.get(i).left.equals(range.left))
                                             .findFirst();
                if (start.isPresent())
                {
                    Range<Token> highestCompleted = completedRanges.get(start.getAsInt());
                    // FIXME(josephl|2017-09-15): this might be way too slow, hopefully it's ok
                    for (int i = start.getAsInt(); i < completedRanges.size() - 1; i++)
                    {
                        if (!completedRanges.get(i).right.equals(completedRanges.get(i + 1).left))
                        {
                            break;
                        }
                        if (highestCompleted.right.equals(range.right))
                        {
                            break;
                        }
                        highestCompleted = completedRanges.get(i + 1);
                    }

                    if (!highestCompleted.right.equals(range.right))
                    {
                        anyRangeMissing = true;
                        break;
                    }
                }
                else
                {
                    anyRangeMissing = true;
                    break;
                }
            }
            if (!anyRangeMissing)
            {
                alreadyRepaired.put(ksTbName, true);
            }
        });

        // Filter completed tables from repairing, since they were already repaired
        return tableConfigList.stream()
                              .filter(tc -> !alreadyRepaired.getOrDefault(tc.getKsTbName(), false))
                              .filter(tc -> !primaryRangesByKeyspace.get(tc.getKeyspace()).isEmpty())
                              .collect(Collectors.toList());
    }

    /**
     * Get RangeTokens for repairId and TableConfig
     * @param repairId Repair Id
     * @param tableConfig Table configuration
     * @return Range of Tokens
     */
    List<Range<Token>> getRangesForRepair(int repairId, TableTaskConfig tableConfig)
    {
        List<Range<Token>> subRangeTokens = new ArrayList<>();
        List<RepairMetadata> repairHistory = daoManager.getRepairStatusDao()
                                                       .getRepairHistory(repairId,
                                                                         tableConfig.getKeyspace(), tableConfig.getName(),
                                                                         cassInteraction.getLocalHostId());

        // Use of isTerminal is important here, if we lost notifications on a range repair, we want to re-repair
        // it again to try to make sure it actually finished.
        List<Range<Token>> completedRanges = repairHistory.stream()
                                                          .filter(rm -> rm.getStatus().isTerminal())
                                                          .map(rm -> cassInteraction.tokenRangeFromStrings(rm.getStartToken(), rm.getEndToken()))
                                                          .collect(Collectors.toList());

        // use Range.sort instead of .sorted above because this Range.sort sorts by left instead of right token.
        Range.sort(completedRanges);

        TableRepairRangeContext rangeContext = cassInteraction.getTableRepairRangeContext(tableConfig.getKeyspace(),
                                                                                          tableConfig.getName());
        for (Range<Token> range : rangeContext.primaryRanges)
        {
            List<Range<Token>> newSubRanges;
            // Do not split incremental repairs as the anti-compaction can overwhelm vnode clusters
            // Also there should be no need to split incremental repairs
            // TODO: For the very first incremental repair we may still want to split?
            if (tableConfig.isIncremental())
                newSubRanges = Collections.singletonList(range);
            else
                newSubRanges = cassInteraction.getTokenRangeSplits(rangeContext, range,
                                                                   tableConfig.getRepairOptions().getSplitStrategy());

            OptionalInt start = IntStream.range(0, completedRanges.size())
                                         .filter(i -> completedRanges.get(i).left.equals(range.left))
                                         .findFirst();

            if (start.isPresent())
            {
                Range<Token> highestCompleted = completedRanges.get(start.getAsInt());
                for (int i = start.getAsInt(); i < completedRanges.size() - 1; i++)
                {
                    if (!completedRanges.get(i).right.equals(completedRanges.get(i + 1).right))
                        break;
                    highestCompleted = completedRanges.get(i + 1);
                }
                // Lambda scoping ...
                final Range<Token> actualHighest = highestCompleted;
                // Keep any ranges that start after the highest range's _start_ (they may overlap)
                subRangeTokens.addAll(
                newSubRanges.stream()
                            .filter(rr -> rr.left.compareTo(actualHighest.left) > 0)
                            .collect(Collectors.toList())
                );
            }
            else
            {
                subRangeTokens.addAll(newSubRanges);
            }
        }

        return subRangeTokens;
    }

    /**
     * Publishes heartbeat to repair sequence table. This is critical for instance to indicate that it is active
     * @param repairId Repair ID
     * @param seq Instance information
     */
    void publishHeartBeat(int repairId, int seq)
    {
        if (cassInteraction.isCassandraHealthy())
        {
            daoManager.getRepairSequenceDao().updateHeartBeat(repairId, seq);
        }
        else
        {
            logger.warn("Cassandra instance is not healthy, skipping repair heart beat for repairId: {}, seq: {}", repairId, seq);
        }
    }

    /**
     * Cleans up after a repair to ensure clean state for the next run
     * <p>
     * 1. Cancels any outstanding repairs on this node to get it back to a clean state. Typically there will
     * be no running repairs
     * 2. Transitions any non completed repairs to notification lost
     * 3. Cleans up any leaked jmx listeners (yes this happens)
     * <p>
     * Note that we wait until all repair activity on this cassandra node is done before attempting this
     */
    void cleanupAfterRepairOnNode(LocalRepairState repairState, TaskStatus finalState)
    {
        List<RepairMetadata> repairHistory = daoManager.getRepairStatusDao()
                                                       .getRepairHistory(repairState.repairId,
                                                                         repairState.sequence.getNodeId());

        for (RepairMetadata rm : repairHistory)
        {
            if (rm.getStatus() == null || !rm.getStatus().isCompleted())
            {
                rm.setLastEvent("Notification Lost Reason", "Finished repair but lost RepairRunner");
                rm.setEndTime(DateTime.now().toDate());
                rm.setStatus(TaskStatus.NOTIFS_LOST);
                daoManager.getRepairStatusDao().markRepairStatusChange(rm);
            }
        }

        Set<NotificationListener> outstandingListeners = cassInteraction.getOutstandingRepairNotificationListeners();
        if (outstandingListeners.size() > 0)
        {
            logger.warn("Detected {} leaked RepairRunner listerners, cleaning them up now!");
            outstandingListeners.forEach(cassInteraction::removeRepairNotificationListener);
        }

        daoManager.getRepairSequenceDao().markNodeDone(repairState.repairId, repairState.sequence.getSeq(), finalState);
    }

    IRepairStatusDao getRepairStatusDao()
    {
        return daoManager.getRepairStatusDao();
    }
}
