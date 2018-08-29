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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.scheduler.TaskDaoManager;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskTableStatusDao;
import org.apache.cassandra.repair.scheduler.entity.LocalTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.entity.TableRepairRangeContext;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.apache.cassandra.repair.scheduler.tasks.BaseTask;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * Repair Controller is the glue between TaskManager and RepairDaos, Cassandra Interaction classes.
 * This class is the control plane for repair scheduler. Repair Controller holds all the definitions
 * for State Machine (Refer to CASSANDRA-14346). This is the backbone for repair scheduler.
 */
public class RepairTask extends BaseTask
{
    private static final Logger logger = LoggerFactory.getLogger(RepairTask.class);
    private final Set<Future> repairFutures = ConcurrentHashMap.newKeySet();

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
    boolean canRunRepair(int taskId)
    {
        // Check if Repair is running on current node? If Yes: Return False, (Check from JMX)
        if (cassInteraction.isRepairRunning(false))
        {
            return false;
        }
        return !isSequencePausedOnCluster(taskId);
    }

    public TaskStatus runTask(LocalTaskStatus state)
    {
        int repairId = state.taskId;
        String scheduleName = state.sequence.getScheduleName();

        List<TableTaskConfig> tablesToRepair = getTablesForRepair(repairId, scheduleName);

        Map<String, Integer> tableRetries = new HashMap<>();
        Queue<TableTaskConfig> tablesToRepairQueue = new LinkedBlockingQueue<>(tablesToRepair.size());
        tablesToRepairQueue.addAll(tablesToRepair);
        Map<String, List<Range<Token>>> failedRanges = new ConcurrentHashMap<>();
        boolean error = false;

        while (!tablesToRepairQueue.isEmpty())
        {
            boolean needsRetry = true;
            TableTaskConfig tableToRepair = tablesToRepairQueue.poll();
            if (tableToRepair == null || !tableToRepair.isRepairEnabled())
                continue;

            failedRanges.putIfAbsent(tableToRepair.getKsTbName(), new ArrayList<>());

            if (canRunRepair(repairId))
            {
                List<Range<Token>> tableFailedRanges = null;
                try
                {
                    tableFailedRanges = executeRepair(repairId, tableToRepair);
                }
                catch (InterruptedException e)
                {
                    needsRetry = true;
                }
                failedRanges.get(tableToRepair.getKsTbName()).addAll(tableFailedRanges);
                needsRetry = !tableFailedRanges.isEmpty();
            }

            if (needsRetry)
            {
                tableRetries.compute(tableToRepair.getKsTbName(), (key, value) -> value == null ? 1 : 1 + value);
                int secondsToSleep = 10;
                try
                {
                    Thread.sleep(secondsToSleep * 1000);
                }
                catch (InterruptedException ignored) { }
                int totalRetries = tableRetries.values().stream().mapToInt(v->v).sum();
                if ((secondsToSleep * totalRetries) > context.getConfig().getRepairProcessTimeoutInS())
                {
                    logger.error("Timed out waiting for running repair or pause to finish, exiting state machine");
                    error = true;
                    break;
                }
                if (tableRetries.get(tableToRepair.getKsTbName()) < 3)
                    tablesToRepairQueue.offer(tableToRepair);
            }

        }

        int numFailedRanges = failedRanges.values().stream()
                                          .mapToInt(List::size).sum();
        error = error || numFailedRanges > 0;

        TaskStatus finalStatus = error ? TaskStatus.FAILED : TaskStatus.FINISHED;
        logger.debug("Ensuring that all repairs are cleaned up before marking instance complete, finishing with {}",
                     finalStatus);

        logger.info("All tables repaired, {} ranges failed. Exiting", numFailedRanges);

        return finalStatus;
    }

    /**
     * Cancels repair on current node. If the given instance information is not current node,
     * it updates the sequence and status tables as cancelled
     * @param taskId repair id
     * @param reason The reason for canceling the task
     * @param seq node information
     * @return true or false indicating the repair cancel status
     */
    public boolean cancelTaskOnNode(int taskId, String reason, TaskSequence seq)
    {
        boolean cancelled;
        RepairSchedulerMetrics.instance.incNumTimesRepairCancelled();

        if (seq.getNodeId().equalsIgnoreCase(cassInteraction.getLocalHostId()))
            cassInteraction.cancelAllRepairs();

        // Cancel on status and sequence data items
        cancelled = daoManager.getTaskSequenceDao().cancelRepairOnNode(taskId, seq.getSeq());
        cancelled &= daoManager.getTableStatusDao().markTaskCancelled(taskId, seq.getNodeId());

        // This has to be synchronized because another thread might be in the middle of scheduling futures. All
        // methods which generate repairFutures also lock on this and check if the repair is paused before
        // scheduling the repair.
        synchronized (repairFutures)
        {
            repairFutures.stream().filter(Objects::nonNull)
                         .forEach(future -> future.cancel(true));
        }

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
        SortedSet<TaskSequence> taskSequence = daoManager.getTaskSequenceDao().getRepairSequence(repairId);
        Set<String> allRepairedNodes = taskSequence.stream()
                                                   .map(TaskSequence::getNodeId)
                                                   .collect(Collectors.toSet());
        Set<String> finishedNodes = taskSequence.stream()
                                                .filter(repairSeq -> repairSeq.getStatus().isCompleted())
                                                .map(TaskSequence::getNodeId)
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
     * @param taskId Repair Id
     * @return true/ false indicating the readiness fore post repair hook
     */
    public boolean amIReadyForPostTaskHook(int taskId)
    {
        logger.info("Checking to see if this node is ready for post repair hook for taskId: {}", taskId);

        if (!isPostRepairHookCompleteOnCluster(taskId, cassInteraction.getLocalHostId()))
        {
            //TODO: Check if it has failed and retried for more than e.g. 3 times
            logger.debug("Post repair hook has not run on this node for taskId: {}", taskId);
            Set<String> incompleteRepairNeighbors = getIncompleteRepairNeighbors(taskId);
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
            logger.debug("Repair hook has already completed on this node for taskId: {}", taskId);
        }

        return false;
    }

    private boolean isPostRepairHookCompleteOnCluster(int repairId, String nodeId)
    {
        logger.info("Checking to see if this node's post repair hook is complete or not. RepairId: {}, nodeId: {}", repairId, nodeId);

        TaskMetadata repairStatus = daoManager.getTaskHookDao().getLocalTaskHookStatus(repairId, nodeId);
        if (repairStatus != null && repairStatus.getStatus() != null)
        {
            return repairStatus.getStatus().isCompleted();
        }
        return false;
    }

    /**
     * Returns tables that are eligible for repair during this taskId. Note that this method
     * does **not** include tables that are already done for this taskId. If you want all repair
     * eligible tables use getRepairEligibleTables
     *
     * @param repairId Repair Id
     * @return Table Repair configrations
     */
    List<TableTaskConfig> getTablesForRepair(int repairId, String scheduleName)
    {
        // Get all tables available in this cluster to be repaired based on config and discovery
        List<TableTaskConfig> tableConfigList = daoManager.getTableConfigDao().getAllTaskEnabledTables(scheduleName);
        Set<String> keyspaces = tableConfigList.stream()
                                               .map(TableTaskConfig::getKeyspace).collect(Collectors.toSet());

        Function<String, List<Range<Token>>> conv = keyspace -> cassInteraction.getTokenRanges(keyspace, true);
        Map<String, List<Range<Token>>> primaryRangesByKeyspace = keyspaces.stream()
                                                                           .collect(Collectors.toMap(k -> k, conv));

        // Get if any tables repair history for this taskId
        List<TaskMetadata> repairHistory = daoManager.getTableStatusDao()
                                                     .getTaskHistory(repairId,
                                                                     cassInteraction.getLocalHostId());

        // Get tables that have some repair already scheduled/done for this taskId
        Map<String, List<TaskMetadata>> repairsByKsTb = repairHistory.stream()
                                                                     .collect(Collectors.groupingBy(TaskMetadata::getKsTbName));

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
     * Get RangeTokens for taskId and TableConfig
     * @param repairId Repair Id
     * @param tableConfig Table configuration
     * @return Range of Tokens
     */
    List<Range<Token>> getRangesForRepair(int repairId, TableTaskConfig tableConfig)
    {
        List<Range<Token>> subRangeTokens = new ArrayList<>();
        List<TaskMetadata> repairHistory = daoManager.getTableStatusDao()
                                                     .getTaskHistory(repairId,
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
     * Actually executes repair, automatically handling split ranges and repair types (e.g. we don't do splits
     * for incremental repair, only for full range repair). No matter what kind of repair we are doing we
     * do maximum parallelism here.
     * @param repairId
     * @param tableConfig
     * @return A List of _failed_ token ranges. If the list is empty that means all ranges were repaired.
     */
    private List<Range<Token>> executeRepair(int repairId, TableTaskConfig tableConfig) throws InterruptedException
    {
        // Includes both multiple ranges (vnodes) or full repair using subranges.
        // Also automatically handles splits for full vs no splits for incremental
        final List<Range<Token>> repairRanges = getRangesForRepair(repairId, tableConfig);
        if (repairRanges.size() == 0)
            return Collections.emptyList();

        final repairOptions = new RepairOptions(tableConfig.getTaskConfig());

        int tableWorkers = tableConfig.getRepairOptions().getNumWorkers();
        final LinkedBlockingQueue<Range<Token>> pendingRepairRanges = new LinkedBlockingQueue<>(repairRanges);
        final LinkedBlockingQueue<Range<Token>> runningRepairRanges = new LinkedBlockingQueue<>(tableWorkers);
        final LinkedBlockingQueue<Range<Token>> failedRepairRanges = new LinkedBlockingQueue<>();
        final AtomicInteger numTodo = new AtomicInteger(repairRanges.size());
        final Condition globalProgress = new SimpleCondition();


        // Handle the special case where the number of ranges is less than the number of
        // parallel range repairs we want to complete, or e.g. incremental where we only get one range when
        // there are no vnodes.
        final int numToStart = Math.min(repairRanges.size(), tableConfig.getRepairOptions().getNumWorkers());

        for (int i = 0; i < numToStart; i++)
        {
            if (pendingRepairRanges.peek() != null)
                runningRepairRanges.offer(pendingRepairRanges.poll());
        }

        while ((pendingRepairRanges.size() + runningRepairRanges.size() > 0)) {
            final Range<Token> subRange = runningRepairRanges.take();

            final ListenableFuture<Boolean> future;

            // Repair might get paused _mid_ repair so we have to check if the cluster is paused
            // on every RepairRunner generation
            synchronized (repairFutures) {
                if (isSequencePausedOnCluster(repairId))
                {
                    List<Range<Token>> ranges = new ArrayList<>(pendingRepairRanges);
                    ranges.addAll(runningRepairRanges);
                    ranges.addAll(failedRepairRanges);
                    return ranges;
                }

                final RepairRunner repairRunner = new RepairRunner(repairId, tableConfig, subRange,
                                                                   context, RepairSchedulerMetrics.instance,
                                                                   this, globalProgress);

                logger.info("Scheduling Repair for [{}] on range {}", tableConfig.getKsTbName(), subRange);

                future = localTaskThreadpool.submit(repairRunner);
                repairFutures.add(future);
            }

            Futures.addCallback(future, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    repairFutures.remove(future);
                    if (result) {
                        logger.info("Repair succeeded on range {}", subRange);
                    } else {
                        logger.error("Repair failed on range {}", subRange);
                        failedRepairRanges.offer(subRange);
                    }
                    doNextRepair();
                }

                @Override
                public void onFailure(Throwable t) {
                    repairFutures.remove(future);
                    logger.error("Repair failed (exception) on range {}: {}", subRange, t);
                    if (t instanceof Exception) {
                        failedRepairRanges.offer(subRange);
                        doNextRepair();
                    }
                }

                private void doNextRepair() {
                    numTodo.decrementAndGet();
                    // Signal to the outside spinwait that we've made progress
                    // Note that this plus the signalAlls from the RepairRunners
                    // themselves are needed to ensure we make progress below
                    globalProgress.signalAll();
                    Range<Token> nextRange = pendingRepairRanges.poll();
                    if (nextRange != null) {
                        runningRepairRanges.offer(nextRange);
                    }
                }
            }, MoreExecutors.directExecutor());
        }

        boolean timeout = false;
        while (numTodo.get() > 0 && !timeout)
        {
            logger.debug("Progress {}/{} done", numTodo.get(), repairRanges.size());
            if (!globalProgress.await(tableConfig.getRepairOptions().getSecondsToWait(), TimeUnit.SECONDS))
            {
                timeout = true;
            }
        }

        if (failedRepairRanges.size() > 0 || timeout) {
            logger.error("Repair failed on table : {}, [timeout: {}, {} seconds]",
                         tableConfig.getKsTbName(), timeout, tableConfig.getRepairOptions().getSecondsToWait());
            if (failedRepairRanges.size() > 0) {
                logger.error("FAILED {} ranges: [{}]", failedRepairRanges.size(), failedRepairRanges);
            }
            if (timeout) {
                // Timeout, we should cancel repair so as to not overload the cluster
                cassInteraction.cancelAllRepairs();
                List<Range<Token>> ranges = new ArrayList<>(pendingRepairRanges);
                ranges.addAll(runningRepairRanges);
                ranges.addAll(failedRepairRanges);
                return ranges;
            }
        } else {
            logger.info("Done with Repair on table: {}!", tableConfig.getKsTbName());
        }

        return new ArrayList<>(failedRepairRanges);
    }

    ITaskTableStatusDao getRepairStatusDao()
    {
        return daoManager.getTableStatusDao();
    }
}
