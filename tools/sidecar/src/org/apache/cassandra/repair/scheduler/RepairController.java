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
package org.apache.cassandra.repair.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.management.NotificationListener;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairStatusDao;
import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalRepairState;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;
import org.apache.cassandra.repair.scheduler.entity.TableRepairRangeContext;
import org.apache.cassandra.repair.scheduler.hooks.IRepairHook;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.joda.time.DateTime;

/**
 * Repair Controller is the glue between RepairManager and RepairDaos, Cassandra Interaction classes.
 * This class is the control plane for repair scheduler. Repair Controller holds all the definitions
 * for State Machine (Refer to CASSANDRA-14346). This is the backbone for repair scheduler.
 */
public class RepairController
{
    private static final Logger logger = LoggerFactory.getLogger(RepairController.class);

    private final RepairDaoManager daoManager;
    private final RepairSchedulerContext context;
    private final CassandraInteraction cassInteraction;

    public RepairController(RepairSchedulerContext context, RepairDaoManager daoManager)
    {
        cassInteraction = context.getCassInteraction();
        this.context = context;
        this.daoManager = daoManager;

        logger.debug("RepairController Initialized.");
    }

    /**
     * Given that no other nodes in the cluster are running repair (that is checked by isRepairRunningOnCluster.
     * can this particular node start repairing? We cannot proceed at this time if:
     *  <ul>
     *    <li> 1. The local cassandra is currently running repairs (e.g. we crashed or a manual repair was run)</li>
     *    <li> 2. Repair is paused on this cluster </li>
     *  </ul>
     * @param repairId RepairId to check the status for
     * @return true or false indicating whether repair can run or not
     */
    boolean canRunRepair(int repairId)
    {
        // Check if Repair is running on current node? If Yes: Return False, (Check from JMX)
        if (cassInteraction.isRepairRunning())
        {
            return false;
        }
        return !isRepairPausedOnCluster(repairId);
    }

    /**
     * Checks if the repair is paused on the cluster for a given RepairId
     * @param repairId RepairId to check the status for
     * @return true/ false indicating the repair pause status
     */
    boolean isRepairPausedOnCluster(int repairId)
    {
        try
        {
            // Check if repair is paused at either cluster level or node level, if so return false
            Optional<ClusterRepairStatus> crs = daoManager.getRepairProcessDao().getClusterRepairStatus(repairId);
            Optional<RepairSequence> nrs = daoManager.getRepairSequenceDao().getMyNodeStatus(repairId);

            if ((crs.isPresent() && crs.get().getRepairStatus().isPaused())
                || (nrs.isPresent() && nrs.get().getStatus().isPaused()))
            {
                logger.debug("Either cluster level or node level repair is paused, hence not running repair");
                return true;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in getting repair status from repair_status table", e);
        }
        return false;
    }

    /**
     * Checks if the repair is running on the cluster for a given repair id
     * @param repairId RepairId to check the status
     * @return true/ false indicating the repair status
     */
    boolean isRepairRunningOnCluster(int repairId)
    {
        // Check underlying data store to see if there is any repair process running in the entire cluster
        SortedSet<RepairSequence> repairSequence = daoManager.getRepairSequenceDao().getRepairSequence(repairId);
        Optional<RepairSequence> anyRunning = repairSequence.stream().filter(rs -> rs.getStatus().isStarted()).findFirst();

        if (anyRunning.isPresent())
        {
            logger.debug("Repair is already running on this cluster on - {}", anyRunning.get());
            return true;
        }

        // Check repair_status table to see if any table level repairs (lowest repair tracking table) are running or not
        List<RepairMetadata> repairHistory = daoManager.getRepairStatusDao().getRepairHistory(repairId);
        for (RepairMetadata row : repairHistory)
        {
            if (!row.getStatus().isCompleted())
            {
                logger.debug("Repair is already running on this cluster on - {}", row);
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if the repair sequence generation is stuck on the cluster.
     * Stuck sequence generation is defined as, if the cluster process status table has an entry with the repair Id
     * in started state and no sequence is generated in process_timeout_seconds seconds in repair_sequence table is
     * defined as stuck.
     * @param repairId RepairId to check the status for
     * @return true/ false indicating the repair stuck status
     */
    boolean isRepairSequenceGenerationStuckOnCluster(int repairId)
    {
        try
        {
            Optional<ClusterRepairStatus> crs = getClusterRepairStatus();
            int minutes = (int) TimeUnit.MINUTES.convert(context.getConfig().getRepairProcessTimeoutInS(), TimeUnit.SECONDS);
            if (crs.isPresent() &&
                crs.get().getStartTime().before(DateTime.now().minusMinutes(minutes).toDate()) &&
                daoManager.getRepairSequenceDao().getRepairSequence(repairId).size() == 0)
            {
                return true;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception occurred in checking the stuck status on cluster", e);
        }
        return false;
    }

    /**
     * Gets repair nodes where repair is stuck. Stuck repair is defined as, As per the design,
     * every node in state (F) is constantly heartbeats to their row in the repair_sequence table
     * as it makes progress in repairing, if any node is updating their heartbeat for more than process_timeout_seconds
     * then that is defined as a stuck repair node.
     * @param repairId RepairId to check the status of stuck nodes
     * @return RepairNodes which are stuck
     */
    Optional<RepairSequence> getStuckRepairNodes(int repairId)
    {
        logger.info("Getting latest in progress heartbeat for repair Id: {}", repairId);
        try
        {
            SortedSet<RepairSequence> repairSeqSet = daoManager.getRepairSequenceDao().getRepairSequence(repairId);
            long minutes = TimeUnit.MINUTES.convert(context.getConfig().getRepairProcessTimeoutInS(), TimeUnit.SECONDS);
            for (RepairSequence repairSequence : repairSeqSet)
            {
                if (repairSequence.getStatus().isStarted() && repairSequence.isLastHeartbeatBeforeMin(minutes))
                {
                    logger.debug("Found at least one latest in-progress heartbeat whose last sent time is before {} minutes ago - [{}]",
                                 minutes, repairSequence);
                    return Optional.of(repairSequence);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in finding if repair is stuck on cluster for repair Id:{}", repairId, e);
        }

        return Optional.empty();
    }

    /**
     * Cleans cluster repair status from repair process table
     * @param repairId RepairId to abort the repair for
     */
    void abortRepairOnCluster(int repairId)
    {
        logger.warn("Aborting Stuck Repair on Cluster for repairId {}", repairId);
        daoManager.getRepairProcessDao().deleteClusterRepairStatus(repairId);
    }

    /**
     * Pauses repair on cluster. It updates the repair_process table which is the key table for global
     * repair status at cluster level
     * @param repairId RepairId to pause the repair
     */
    void pauseRepairOnCluster(int repairId)
    {
        Optional<ClusterRepairStatus> crs = getClusterRepairStatus(repairId);
        if (crs.isPresent())
        {
            ClusterRepairStatus status = crs.get();
            status.setPauseTime(DateTime.now().toDate())
                  .setRepairStatus(RepairStatus.PAUSED);
            daoManager.getRepairProcessDao().updateClusterRepairStatus(status);
            logger.warn("Pausing repair on cluster for repairId {}", repairId);
        }
    }

    /**
     * Aborts repair on current node, if the sequence to be aborted is not the current node, update
     * the status table
     * @param sequence RepairSequence to abort the repair
     */
    private void abortRepairOnStuckSequence(RepairSequence sequence)
    {
        logger.warn("Aborting Stuck Repair on {}", sequence.getNodeId());
        RepairSchedulerMetrics.instance.incNumTimesRepairStuck();
        cancelRepairOnNode(sequence.getRepairId(), sequence, "Stuck sequence");
    }

    /**
     * Gets the repair sequences from meta store matching the given predicate
     * @param repairId Repair Id to get the sequences from
     * @param include predicate to filter for in repair sequences
     * @return Set of repair sequences
     */
    private SortedSet<RepairSequence> getMatchingRepairSequences(int repairId, Predicate<RepairSequence> include)
    {
        SortedSet<RepairSequence> matchingRepairSequences = new TreeSet<>();
        SortedSet<RepairSequence> repairSeqSet = daoManager.getRepairSequenceDao().getRepairSequence(repairId);

        repairSeqSet.stream()
                    .filter(include)
                    .forEach(matchingRepairSequences::add);

        logger.debug("Found {} matching repair node sequences for repairId: {}", matchingRepairSequences.size(), repairId);
        return matchingRepairSequences;
    }

    /**
     * Checks if the repair hook exists for a schedule on current cluster
     * @param scheduleName schedule name
     * @return true / false
     */
    private boolean isPostRepairHookExists(String scheduleName)
    {
        List<TableRepairConfig> tableConfigs = daoManager.getRepairConfigDao().getAllRepairEnabledTables(scheduleName);
        return tableConfigs.stream().anyMatch(TableRepairConfig::shouldRunPostRepairHook);
    }

    /**
     * Checks if the current node is next in repair or done with repair.
     * It queries repair_sequence(node level status) table to see the status.
     * It also aborts the repair on stuck instance if it is failed to get leadership
     * when it is next in the sequence.
     * @param repairId RepairId to check
     * @return Returns the repair sequence object if it is next or already done
     */
    Optional<RepairSequence> amINextInRepairOrDone(int repairId)
    {
        SortedSet<RepairSequence> notStartedRepairs = getMatchingRepairSequences(
        repairId, rs -> rs.getStatus() == RepairStatus.NOT_STARTED);
        SortedSet<RepairSequence> finishedRepairSequences = getMatchingRepairSequences(
        repairId, rs -> rs.getStatus() == RepairStatus.FINISHED);

        String localHostId = cassInteraction.getLocalHostId();

        Optional<RepairSequence> myComplete = finishedRepairSequences.stream()
                                                                     .filter(rs -> StringUtils.isNotBlank(rs.getNodeId())
                                                                                   && rs.getNodeId().equalsIgnoreCase(localHostId))
                                                                     .findFirst();
        if (myComplete.isPresent())
        {
            return myComplete;
        }

        Optional<RepairSequence> next = notStartedRepairs.stream().findFirst();
        // Am I the next node in the sequence to be repaired
        if (next.isPresent() &&
            StringUtils.isNotBlank(next.get().getNodeId()) &&
            next.get().getNodeId().equalsIgnoreCase(localHostId))
        {
            RepairStatus myStatus = next.get().getStatus();
            if (!myStatus.isPaused() && !myStatus.isCancelled())
            {
                return next;
            }
            else
            {
                logger.info("Would repair but unable to get repair leadership since I am paused or cancelled", next);
            }
        }

        getStuckRepairNodes(repairId).ifPresent(this::abortRepairOnStuckSequence);

        return Optional.empty();
    }

    /**
     * Checks if all nodes finished their repair or not by querying repair_sequence (node level status)
     * table.
     * @param repairId RepairId to check the status for
     * @return true/ false to inform the repair done status on cluster
     */
    boolean isRepairDoneOnCluster(int repairId)
    {
        getStuckRepairNodes(repairId).ifPresent(this::abortRepairOnStuckSequence);

        SortedSet<RepairSequence> repairSeq = daoManager.getRepairSequenceDao().getRepairSequence(repairId);

        long finishedRepairsCnt = repairSeq.stream().filter(rs -> rs.getStatus().isCompleted()).count();

        // Have to check for > 0 in case of a race on this check before a node has a chance to generate
        // the repair sequence elsewhere.
        if (repairSeq.size() == finishedRepairsCnt && repairSeq.size() > 0)
        {
            return true;
        }
        else
        {
            logger.info("Not all nodes completed their repair for repairId: {}", repairId);
        }
        return false;
    }

    /**
     * Gets the currently running repair id
     * @return repair id
     */
    public int getRepairId()
    {
        int repairId = -1;
        try
        {
            Optional<ClusterRepairStatus> crs = getClusterRepairStatus();
            if (crs.isPresent() && crs.get().getRepairStatus().isStarted())
                repairId = crs.get().getRepairId();
        }
        catch (Exception e)
        {
            logger.error("Failed to retrieve cluster status, failing fast", e);
        }

        return repairId;
    }

    /**
     * Gets current/ latest repair status on the current cluster
     * @return cluster repair status
     */
    public Optional<ClusterRepairStatus> getClusterRepairStatus()
    {
        return daoManager.getRepairProcessDao().getClusterRepairStatus();
    }

    /**
     * Gets the cluster repair status for a given repair id
     * @param repairId repair id
     * @return cluster repair status
     */
    private Optional<ClusterRepairStatus> getClusterRepairStatus(int repairId)
    {
        return daoManager.getRepairProcessDao().getClusterRepairStatus(repairId);
    }

    /**
     * Populates endpoint/ nodes sequence map. This is done by only one node in the cluster.
     * This is the most important step in starting the repair on the cluster, this happens as soon as
     * one of the nodes able to claim repair id.
     * @param repairId Repair Id to generate the sequence for
     * @param scheduleName schedule name
     */
    void populateEndpointSequenceMap(int repairId, String scheduleName)
    {
        List<RepairHost> hostsToRepair = getRepairHosts();
        daoManager.getRepairConfigDao().getRepairConfigs(scheduleName);
        daoManager.getRepairSequenceDao().persistEndpointSeqMap(repairId, scheduleName, hostsToRepair);
    }

    /**
     * Gets RepairHosts from C* JMX, this has the information of node id, rack, region etc.,
     * @return List if repairable hosts
     */
    private List<RepairHost> getRepairHosts()
    {
        Map<String, String> endpointToHostId = cassInteraction.getEndpointToHostIdMap();

        Set<Host> allHosts = context.localSession().getCluster().getMetadata().getAllHosts();

        return allHosts.stream()
                       .map(RepairHost::new)
                       .sorted(Comparator.comparing(RepairHost::getFirstToken))
                       .collect(Collectors.toList());
    }

    /**
     * Marks repair started on one instance
     * @param repairId repair id
     * @param seq node / instance information
     */
    void prepareForRepairOnNode(int repairId, RepairSequence seq)
    {
        daoManager.getRepairSequenceDao().markRepairStartedOnInstance(repairId, seq.getSeq());
    }

    /**
     * Cancels repair on current node. If the given instance information is not current node,
     * it updates the sequence and status tables as cancelled
     * @param repairId repair id
     * @param seq node information
     * @return true or false indicating the repair cancel status
     */
    boolean cancelRepairOnNode(int repairId, RepairSequence seq, String reason)
    {
        boolean cancelled;
        RepairSchedulerMetrics.instance.incNumTimesRepairCancelled();

        if (seq.getNodeId().equalsIgnoreCase(cassInteraction.getLocalHostId()))
            cassInteraction.cancelAllRepairs();

        // Cancel on status and sequence data items
        cancelled = daoManager.getRepairSequenceDao().cancelRepairOnNode(repairId, seq.getSeq());
        cancelled &= daoManager.getRepairStatusDao().markRepairCancelled(repairId, seq.getNodeId());

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
     * This is the only coordination place in the repair scheduler, all nodes tries to call this method and expect
     * a true as a return value, but whoever gets true back will go head for sequence generation and starts their repair
     * @param proposedRepairId Proposed Repair Id
     * @return true if this node is able to acquire a lock on repair_ process table based on proposed repair id
     */
    boolean attemptClusterRepairStart(int proposedRepairId)
    {
        return daoManager.getRepairProcessDao().acquireRepairInitLock(proposedRepairId);
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

    boolean amIReadyForPostRepairHook(int repairId)
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

    List<TableRepairConfig> prepareForRepairHookOnNode(RepairSequence sequence)
    {
        List<TableRepairConfig> repairHookEligibleTables = daoManager.getRepairConfigDao()
                                                                     .getAllRepairEnabledTables(sequence.getScheduleName())
                                                                     .stream()
                                                                     .filter(TableRepairConfig::shouldRunPostRepairHook)
                                                                     .collect(Collectors.toList());
        RepairSchedulerMetrics.instance.incNumTimesRepairHookStarted();
        daoManager.getRepairHookDao().markLocalPostRepairHookStarted(sequence.getRepairId());

        logger.info("C* Load before starting post repair hook(s) for repairId {}: {}",
                    sequence.getRepairId(), cassInteraction.getLocalLoadString());

        return repairHookEligibleTables;
    }

    /**
     * Calls Hook's run method
     * @param hook Hook to run
     * @param tableConfig table configuration as needed for repair hook
     * @return true/ false indicating the response of this method
     */
    boolean runHook(IRepairHook hook, TableRepairConfig tableConfig)
    {
        try
        {
            hook.run(cassInteraction, tableConfig);
        }
        catch (Exception e)
        {
            String msg = String.format("Error running hook %s on table %s", hook, tableConfig);
            logger.error(msg, e);
            return false;
        }
        return true;
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
                                                                     RepairStatus.FINISHED, hookSuccess);
        }
        else
        {
            logger.error("Failed to run post repair hook(s) for repairId: {}", sequence.getRepairId());

            RepairSchedulerMetrics.instance.incNumTimesRepairHookFailed();
            daoManager.getRepairHookDao().markLocalPostRepairHookEnd(sequence.getRepairId(),
                                                                     RepairStatus.FAILED, hookSuccess);
        }

        logger.info("C* Load after completing post repair hook(s) for repairId {}: {}",
                    sequence.getRepairId(), cassInteraction.getLocalLoadString());
    }

    /**
     * Repairable tables grouped by schedule name.
     *
     * @return Map of Schedule_name, List of TableRepairConfigrations
     */

    Map<String, List<TableRepairConfig>> getRepairableTablesBySchedule()
    {
        Map<String, List<TableRepairConfig>> repairableTablesBySchedule = new HashMap<>();
        for (String schedule : daoManager.getRepairConfigDao().getAllRepairSchedules())
        {
            repairableTablesBySchedule.put(schedule, daoManager.getRepairConfigDao().getAllRepairEnabledTables(schedule));
        }
        return repairableTablesBySchedule;
    }

    /**
     * Returns tables that are eligible for repair during this repairId. Note that this method
     * does **not** include tables that are already done for this repairId. If you want all repair
     * eligible tables use getRepairEligibleTables
     *
     * @param repairId Repair Id
     * @return Table Repair configrations
     */
    List<TableRepairConfig> getTablesForRepair(int repairId, String scheduleName)
    {
        // Get all tables available in this cluster to be repaired based on config and discovery
        List<TableRepairConfig> tableConfigList = daoManager.getRepairConfigDao().getAllRepairEnabledTables(scheduleName);
        Set<String> keyspaces = tableConfigList.stream()
                                               .map(TableRepairConfig::getKeyspace).collect(Collectors.toSet());

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
    List<Range<Token>> getRangesForRepair(int repairId, TableRepairConfig tableConfig)
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
                                                          .sorted()
                                                          .collect(Collectors.toList());

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
    void cleanupAfterRepairOnNode(LocalRepairState repairState)
    {

        List<RepairMetadata> repairHistory = daoManager.getRepairStatusDao()
                                                       .getRepairHistory(repairState.repairId, repairState.sequence.getNodeId());

        for (RepairMetadata rm : repairHistory)
        {
            if (rm.getStatus() == null || !rm.getStatus().isCompleted())
            {
                rm.setLastEvent("Notification Lost Reason", "Finished repair but lost RepairRunner");
                rm.setEndTime(DateTime.now().toDate());
                rm.setStatus(RepairStatus.NOTIFS_LOST);
                daoManager.getRepairStatusDao().markRepairStatusChange(rm);
            }
        }

        Set<NotificationListener> outstandingListeners = cassInteraction.getOutstandingRepairNotificationListeners();
        if (outstandingListeners.size() > 0)
        {
            logger.warn("Detected {} leaked RepairRunners, cleaning them up now!");
            outstandingListeners.forEach(cassInteraction::removeRepairNotificationListener);
        }

        daoManager.getRepairSequenceDao().markRepairFinishedOnNode(repairState.repairId, repairState.sequence.getSeq());
    }

    void cancelRepairOnNode()
    {
        cassInteraction.cancelAllRepairs();
    }

    /**
     * Moves the cluster repair process either from STARTED to REPAIR_HOOK_RUNNING or
     * moves REPAIR_HOOK_RUNNING to FINISHED if all hooks are done. If the cluster is now FINISHED
     *
     * @param repairState LocalRepairState
     * @return true indicates the cluster is now FINISHED. false indicates the cluster is
     * either already REPAIR_HOOK_RUNNING (and is not done), is PAUSED, or is already FINISHED.
     */
    boolean finishClusterRepair(LocalRepairState repairState)
    {
        int repairId = repairState.repairId;
        Optional<ClusterRepairStatus> clusterStatus = daoManager.getRepairProcessDao()
                                                                .getClusterRepairStatus(repairId);

        if (!clusterStatus.isPresent())
            throw new IllegalStateException("Cluster status can't be missing at this stage");

        RepairStatus clusterRepairStatus = clusterStatus.get().getRepairStatus();

        if (clusterRepairStatus == RepairStatus.REPAIR_HOOK_RUNNING ||
            isPostRepairHookExists(repairState.sequence.getScheduleName()))
        {

            if (clusterRepairStatus == RepairStatus.STARTED)
            {
                daoManager.getRepairProcessDao().updateClusterRepairStatus(
                new ClusterRepairStatus().setRepairId(repairId)
                                         .setRepairStatus(RepairStatus.REPAIR_HOOK_RUNNING)
                );
            }
            // TODO: timeout the repair hook if it gets stuck ...
            else if (clusterRepairStatus == RepairStatus.REPAIR_HOOK_RUNNING && isPostRepairHookCompleteOnCluster(repairId))
            {
                daoManager.getRepairProcessDao().markClusterRepairFinished(repairId);
                return true;
            }
        }
        else
        {
            logger.info("Since repair is done on all nodes and there are no post repair hooks scheduled, marking cluster repair as FINISHED");
            daoManager.getRepairProcessDao().markClusterRepairFinished(repairId);
            return true;
        }
        return false;
    }

    public IRepairStatusDao getRepairStatusDao()
    {
        return daoManager.getRepairStatusDao();
    }

    public IRepairConfigDao getRepairConfigDao()
    {
        return daoManager.getRepairConfigDao();
    }
}
