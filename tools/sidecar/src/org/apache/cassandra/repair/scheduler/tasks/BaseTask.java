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

package org.apache.cassandra.repair.scheduler.tasks;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import org.apache.cassandra.repair.scheduler.TaskDaoManager;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalRepairState;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.hooks.IRepairHook;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.joda.time.DateTime;

public abstract class BaseTask implements IManagementTask
{
    protected final TaskDaoManager daoManager;
    protected final TaskSchedulerContext context;
    protected final CassandraInteraction cassInteraction;

    private static final Logger logger = LoggerFactory.getLogger(BaseTask.class);

    public BaseTask(TaskSchedulerContext context, TaskDaoManager daoManager)
    {
        cassInteraction = context.getCassInteraction();
        this.context = context;
        this.daoManager = daoManager;
    }

    /**
     * Tables subject to maintenance grouped by schedule name.
     *
     * @return Map of Schedule_name, List of TableRepairConfigrations
     */

    public Map<String, List<TableTaskConfig>> getTasksBySchedule()
    {
        Map<String, List<TableTaskConfig>> repairableTablesBySchedule = new HashMap<>();
        for (String schedule : daoManager.getRepairConfigDao().getAllRepairSchedules())
        {
            repairableTablesBySchedule.put(schedule, daoManager.getRepairConfigDao().getAllRepairEnabledTables(schedule));
        }
        return repairableTablesBySchedule;
    }

    /**
     * Gets the currently running repair id
     * @return repair id
     */
    public int getTaskId()
    {
        int repairId = -1;
        try
        {
            Optional<ClusterTaskStatus> crs = getClusterTaskStatus();
            if (crs.isPresent() && crs.get().getTaskStatus().isStarted())
                repairId = crs.get().getTaskId();
        }
        catch (Exception e)
        {
            logger.error("Failed to retrieve cluster status, failing fast", e);
        }

        return repairId;
    }

    /**
     * Checks if all nodes finished their repair or not by querying repair_sequence (node level status)
     * table. Also responsible for ensuring that stuck sequences get cancelled.
     * @param repairId RepairId to check the status for
     * @return true/ false to inform the repair done status on cluster
     */
    public boolean isTaskDoneOnCluster(int repairId)
    {
        getStuckSequence(repairId).ifPresent(this::abortTaskOnStuckSequence);

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
     * Gets current/ latest repair status on the current cluster
     * @return cluster repair status
     */
    public Optional<ClusterTaskStatus> getClusterTaskStatus()
    {
        return daoManager.getRepairProcessDao().getClusterRepairStatus();
    }

    /**
     * Gets the cluster repair status for a given repair id
     * @param repairId repair id
     * @return cluster repair status
     */
    protected Optional<ClusterTaskStatus> getClusterRepairStatus(int repairId)
    {
        return daoManager.getRepairProcessDao().getClusterRepairStatus(repairId);
    }

    /**
     * This is the only coordination place in the repair scheduler, all nodes tries to call this method and expect
     * a true as a return value, but whoever gets true back will go head for sequence generation and starts their repair
     * @param proposedRepairId Proposed Repair Id
     * @return true if this node is able to acquire a lock on repair_ process table based on proposed repair id
     */
    public boolean attemptClusterTaskStart(int proposedRepairId)
    {
        return daoManager.getRepairProcessDao().acquireRepairInitLock(proposedRepairId);
    }

    /**
     * Populates endpoint/ nodes sequence map. This is done by only one node in the cluster.
     * This is the most important step in starting the repair on the cluster, this happens as soon as
     * one of the nodes able to claim repair id.
     * @param repairId Repair Id to generate the sequence for
     * @param scheduleName schedule name
     */
    public void populateTaskSequence(int repairId, String scheduleName)
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
        Set<Host> allHosts = context.localSession().getCluster().getMetadata().getAllHosts();

        return allHosts.stream()
                       .map(RepairHost::new)
                       .sorted(Comparator.comparing(RepairHost::getFirstToken))
                       .collect(Collectors.toList());
    }

    /**
     * Checks if the repair is paused on the cluster for a given RepairId
     * @param taskId RepairId to check the status for
     * @return true/ false indicating the repair pause status
     */
    public boolean isSequencePausedOnCluster(int taskId)
    {
        try
        {
            // Check if repair is paused at either cluster level or node level, if so return false
            Optional<ClusterTaskStatus> crs = daoManager.getRepairProcessDao().getClusterRepairStatus(taskId);
            Optional<RepairSequence> nrs = daoManager.getRepairSequenceDao().getMyNodeStatus(taskId);

            if ((crs.isPresent() && crs.get().getTaskStatus().isPaused())
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
     * Checks if the repair is running on the cluster _not on this node_ for a given repair id
     * @param taskId RepairId to check the status
     * @return true/ false indicating the repair status
     */
    public boolean isSequenceRunningOnCluster(int taskId)
    {
        // Check underlying data store to see if there is any repair process running in the entire cluster
        SortedSet<RepairSequence> repairSequence = daoManager.getRepairSequenceDao().getRepairSequence(taskId);
        Optional<RepairSequence> anyRunning = repairSequence.stream().filter(rs -> rs.getStatus().isStarted()).findFirst();
        String localId = cassInteraction.getLocalHostId();

        if (anyRunning.isPresent() && !localId.equalsIgnoreCase(anyRunning.get().getNodeId()))
        {
            logger.debug("Repair is already running on this cluster on - {}", anyRunning.get());
            return true;
        }

        // Check repair_status table to see if any table level repairs (lowest repair tracking table) are running or not
        List<RepairMetadata> repairHistory = daoManager.getRepairStatusDao().getRepairHistory(taskId);
        for (RepairMetadata row : repairHistory)
        {
            if (!row.getStatus().isCompleted() && !localId.equalsIgnoreCase(row.getNodeId()))
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
     * @param taskId RepairId to check the status for
     * @return true/ false indicating the repair stuck status
     */
    public boolean isSequenceGenerationStuckOnCluster(int taskId)
    {
        try
        {
            Optional<ClusterTaskStatus> crs = getClusterTaskStatus();
            int minutes = (int) TimeUnit.MINUTES.convert(context.getConfig().getRepairProcessTimeoutInS(), TimeUnit.SECONDS);
            if (crs.isPresent() &&
                crs.get().getStartTime().before(DateTime.now().minusMinutes(minutes).toDate()) &&
                daoManager.getRepairSequenceDao().getRepairSequence(taskId).size() == 0)
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
     * @param taskId RepairId to check the status of stuck nodes
     * @return RepairNodes which are stuck
     */
    public Optional<RepairSequence> getStuckSequence(int taskId)
    {
        logger.info("Getting latest in progress heartbeat for repair Id: {}", taskId);
        try
        {
            SortedSet<RepairSequence> repairSeqSet = daoManager.getRepairSequenceDao().getRepairSequence(taskId);
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
            logger.error("Exception in finding if repair is stuck on cluster for repair Id:{}", taskId, e);
        }

        return Optional.empty();
    }

    /**
     * Cleans cluster repair status from repair process table
     * @param taskId RepairId to abort the repair for
     */
    public void abortTaskOnCluster(int taskId)
    {
        logger.warn("Aborting Stuck Repair on Cluster for repairId {}", taskId);
        daoManager.getRepairProcessDao().deleteClusterRepairStatus(taskId);
    }

    /**
     * Pauses repair on cluster. It updates the repair_process table which is the key table for global
     * repair status at cluster level
     * @param taskId RepairId to pause the repair
     */
    public void pauseTaskOnCluster(int taskId)
    {
        Optional<ClusterTaskStatus> crs = getClusterRepairStatus(taskId);
        if (crs.isPresent())
        {
            ClusterTaskStatus status = crs.get();
            status.setPauseTime(DateTime.now().toDate())
                  .setTaskStatus(TaskStatus.PAUSED);
            daoManager.getRepairProcessDao().updateClusterRepairStatus(status);
            logger.warn("Pausing repair on cluster for repairId {}", taskId);
        }
    }

    /**
     * Aborts repair on current node, if the sequence to be aborted is not the current node, update
     * the status table
     * @param sequence RepairSequence to abort the repair
     */
    public void abortTaskOnStuckSequence(RepairSequence sequence)
    {
        logger.warn("Aborting Stuck Repair on {}", sequence.getNodeId());
        RepairSchedulerMetrics.instance.incNumTimesRepairStuck();
        cancelTaskOnNode(sequence.getRepairId(), "Stuck sequence", sequence);
    }

    /**
     * Moves the cluster repair process either from STARTED to HOOK_RUNNING or
     * moves HOOK_RUNNING to FINISHED if all hooks are done. If the cluster is now FINISHED
     *
     * @param repairState LocalRepairState
     * @return true indicates the cluster is now FINISHED. false indicates the cluster is
     * either already HOOK_RUNNING (and is not done), is PAUSED, or is already FINISHED.
     */
    public boolean finishClusterRepair(LocalRepairState repairState)
    {
        int repairId = repairState.repairId;
        Optional<ClusterTaskStatus> clusterStatus = daoManager.getRepairProcessDao()
                                                              .getClusterRepairStatus(repairId);

        if (!clusterStatus.isPresent())
            throw new IllegalStateException("Cluster status can't be missing at this stage");

        TaskStatus clusterTaskStatus = clusterStatus.get().getTaskStatus();

        if (clusterTaskStatus == TaskStatus.HOOK_RUNNING ||
            isPostTaskHookExists(repairState.sequence.getScheduleName()))
        {

            if (clusterTaskStatus == TaskStatus.STARTED)
            {
                daoManager.getRepairProcessDao().updateClusterRepairStatus(
                clusterStatus.get().setTaskId(repairId)
                             .setTaskStatus(TaskStatus.HOOK_RUNNING)
                );
            }
            // TODO: timeout the repair hook if it gets stuck ...
            // TODO: timeout the repair hook if it gets stuck ...
            else if (clusterTaskStatus == TaskStatus.HOOK_RUNNING &&
                     isPostRepairHookCompleteOnCluster(repairId))
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

    /**
     * Gets the repair sequences from meta store matching the given predicate
     * @param repairId Repair Id to get the sequences from
     * @param include predicate to filter for in repair sequences
     * @return Set of repair sequences
     */
    protected SortedSet<RepairSequence> getMatchingRepairSequences(int repairId, Predicate<RepairSequence> include)
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
    private boolean isPostTaskHookExists(String scheduleName)
    {
        List<TableTaskConfig> tableConfigs = daoManager.getRepairConfigDao().getAllRepairEnabledTables(scheduleName);
        return tableConfigs.stream().anyMatch(TableTaskConfig::shouldRunPostTaskHook);
    }

    /**
     * Calls Hook's run method
     * @param hook Hook to run
     * @param tableConfig table configuration as needed for repair hook
     * @return true/ false indicating the response of this method
     */
    public boolean runHook(IRepairHook hook, TableTaskConfig tableConfig)
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

    public IRepairConfigDao getRepairConfigDao()
    {
        return daoManager.getRepairConfigDao();
    }
}
