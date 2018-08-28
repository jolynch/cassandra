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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.repair.scheduler.TaskDaoManager;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.joda.time.DateTime;

public class TaskLifecycle
{
    private static final Logger logger = LoggerFactory.getLogger(TaskLifecycle.class);

    protected final TaskDaoManager daoManager;
    protected final TaskSchedulerContext context;
    protected final CassandraInteraction cassInteraction;

    public TaskLifecycle(TaskSchedulerContext context, TaskDaoManager daoManager)
    {
        cassInteraction = context.getCassInteraction();
        this.context = context;
        this.daoManager = daoManager;
    }

    /**
     * Gets current/ latest repair status on the current cluster
     * @return cluster repair status
     */
    public Optional<ClusterTaskStatus> getClusterTaskStatus()
    {
        return daoManager.getTaskProcessDao().getClusterTaskStatus();
    }

    /**
     * This is the only coordination place in the repair scheduler, all nodes tries to call this method and expect
     * a true as a return value, but whoever gets true back will go head for sequence generation and starts their repair
     * @param proposedRepairId Proposed Repair Id
     * @return true if this node is able to acquire a lock on repair_ process table based on proposed repair id
     */
    public boolean attemptClusterTaskStart(int proposedRepairId)
    {
        return daoManager.getTaskProcessDao().acquireTaskInitLock(proposedRepairId);
    }

    /**
     * Marks repair started on one instance
     * @param repairId repair id
     * @param seq node / instance information
     */
    public void prepareForTaskOnNode(int repairId, TaskSequence seq)
    {
        daoManager.getRepairSequenceDao().markTaskStartedOnInstance(repairId, seq.getSeq());
    }

    /**
     */
    public void cleanupAfterTaskOnNode(TaskSequence sequence, TaskStatus status)
    {
        daoManager.getRepairSequenceDao().markNodeDone(sequence.getTaskId(), sequence.getSeq(), status);
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
            Optional<ClusterTaskStatus> crs = daoManager.getTaskProcessDao().getClusterTaskStatus(taskId);
            Optional<TaskSequence> nrs = daoManager.getRepairSequenceDao().getMyNodeStatus(taskId);

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
     * Gets the cluster repair status for a given repair id
     * @param repairId repair id
     * @return cluster repair status
     */
    protected Optional<ClusterTaskStatus> getClusterRepairStatus(int repairId)
    {
        return daoManager.getTaskProcessDao().getClusterTaskStatus(repairId);
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
            daoManager.getTaskProcessDao().updateClusterTaskStatus(status);
            logger.warn("Pausing repair on cluster for taskId {}", taskId);
        }
    }

    /**
     * Publishes heartbeat to repair sequence table. This is critical for instance to indicate that it is active
     * @param repairId Repair ID
     * @param seq Instance information
     */
    public void publishHeartBeat(int repairId, int seq)
    {
        if (cassInteraction.isCassandraHealthy())
        {
            daoManager.getRepairSequenceDao().updateHeartBeat(repairId, seq);
        }
        else
        {
            logger.warn("Cassandra instance is not healthy, skipping repair heart beat for taskId: {}, seq: {}", repairId, seq);
        }
    }

    /**
     * Moves the cluster repair process either from STARTED to HOOK_RUNNING or
     * moves HOOK_RUNNING to FINISHED if all hooks are done. If the cluster is now FINISHED
     *
     * @param taskState LocalTaskStatus
     * @return true indicates the cluster is now FINISHED. false indicates the cluster is
     * either already HOOK_RUNNING (and is not done), is PAUSED, or is already FINISHED.
     */
    public boolean finishClusterTask(LocalTaskStatus taskState)
    {
        int repairId = taskState.taskId;
        Optional<ClusterTaskStatus> clusterStatus = daoManager.getTaskProcessDao()
                                                              .getClusterTaskStatus(repairId);

        if (!clusterStatus.isPresent())
            throw new IllegalStateException("Cluster status can't be missing at this stage");

        TaskStatus clusterTaskStatus = clusterStatus.get().getTaskStatus();

        if (clusterTaskStatus == TaskStatus.HOOK_RUNNING ||
            isPostTaskHookExists(taskState.sequence.getScheduleName()))
        {

            if (clusterTaskStatus == TaskStatus.STARTED)
            {
                daoManager.getTaskProcessDao().updateClusterTaskStatus(
                clusterStatus.get().setTaskId(repairId)
                             .setTaskStatus(TaskStatus.HOOK_RUNNING)
                );
            }
            // TODO: timeout the repair hook if it gets stuck ...
            else if (clusterTaskStatus == TaskStatus.HOOK_RUNNING &&
                     isPostTaskHookCompleteOnCluster(repairId))
            {
                daoManager.getTaskProcessDao().markClusterTaskFinished(repairId);
                return true;
            }
        }
        else
        {
            logger.info("Since repair is done on all nodes and there are no post repair hooks scheduled, marking cluster repair as FINISHED");
            daoManager.getTaskProcessDao().markClusterTaskFinished(repairId);
            return true;
        }
        return false;
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

    /**************
     POST TASK HOOKS
    */
    boolean isPostTaskHookCompleteOnCluster(int taskId)
    {
        logger.info("Checking to see if post task hook is complete on entire cluster for this taskId: {}", taskId);

        Set<String> repairedNodes = daoManager.getRepairSequenceDao().getRepairSequence(taskId).stream()
                                              .map(TaskSequence::getNodeId)
                                              .collect(Collectors.toSet());

        Set<String> finishedHookNodes = daoManager.getRepairHookDao().getLocalTaskHookStatus(taskId).stream()
                                                  .filter(rm -> rm.getStatus().isCompleted())
                                                  .map(TaskMetadata::getNodeId)
                                                  .collect(Collectors.toSet());

        boolean isComplete = finishedHookNodes.containsAll(repairedNodes);
        logger.debug("Post repair hook status for entire cluster for this RepairId: {}, Status-isComplete: {}", taskId, isComplete);

        return isComplete;
    }
}
