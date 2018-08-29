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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.NodeStatus;
import org.apache.cassandra.repair.scheduler.entity.TaskSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.hooks.ITaskHook;
import org.apache.cassandra.repair.scheduler.hooks.TaskHookManager;
import org.apache.cassandra.repair.scheduler.tasks.BaseTask;
import org.apache.cassandra.repair.scheduler.tasks.IManagementTask;
import org.apache.cassandra.repair.scheduler.tasks.TaskLifecycle;
import org.joda.time.DateTime;

public class TaskManager
{
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final TaskSchedulerContext context;
    Map<String, IManagementTask> tasks;
    private final AtomicBoolean hasTaskInitiated = new AtomicBoolean(false);
    private final AtomicBoolean hasHookInitiated = new AtomicBoolean(false);

    // This changes every time that a task is chosen to execute
    private IManagementTask currentTask;
    private final TaskLifecycle taskLifecycle;

    /**
     * Gets Task from class name
     *
     * @param taskClassName class name for the distributed task hook, it could be fully qualified class name or
     *                            simple class name
     * @return RepairHook if the classname matches with any class path
     */
    @SuppressWarnings("unchecked")
    static Optional<IManagementTask> constructTask(String taskClassName, TaskSchedulerContext context)
    {
        try
        {
            if (!taskClassName.contains("."))
                taskClassName = "org.apache.cassandra.repair.scheduler.tasks." + taskClassName;

            Class<? extends BaseTask> klass = (Class<? extends BaseTask>) Class.forName(taskClassName);
            Constructor<? extends BaseTask> constructor = klass.getConstructor(TaskSchedulerContext.class, TaskDaoManager.class);
            return Optional.of(constructor.newInstance(context, new TaskDaoManager(context)));
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException ex)
        {
            logger.error("Could not instantiate ManagementTask: [{}] in classpath, hence using No-Op/ null implementation", taskClassName);
            return Optional.empty();
        }
    }

    @VisibleForTesting
    public TaskManager(TaskSchedulerContext context)
    {
        this.context = context;
        TaskSchedulerConfig config = context.getConfig();

        for (String schedule: config.getScheduleConfigs().keySet())
        {
            Optional<IManagementTask> task = constructTask(config.getTaskName(schedule), context);
            task.ifPresent(managementTask -> tasks.put(config.getTaskName(schedule), managementTask));
        }

        // For the manager to interact with the task lifecyle
        taskLifecycle = new TaskLifecycle(context, new TaskDaoManager(context));

        logger.info("TaskManager Initialized.");
    }

    /**
     * The heart of the fully decentralized repair scheduling algorithm
     *
     * This method kicks off the state machine as described in the design document attached to
     * CASSANDRA-14346. An external entity must call this repeatedly to move repair forward.
     *
     * TODO: Draw the diagram using ASCII art here
     *
     * @return A positive taskId if the state machine was entered or -1 otherwise
     */
    public int runRepairOnCluster()
    {
        int taskId = -1;
        Timer heartBeatTimer = null;
        LocalTaskStatus localState = null;
        boolean ranTask, ranHooks;
        ranTask = ranHooks = false;

        try {
            taskId = assessCurrentTask();
            if (taskId > 0)
            {
                localState = assessCurrentTask(taskId);
                logger.info("Found local repair state: {}", localState);
                if (localState.canRunTask)
                {
                    ranTask = true;
                    logger.info("Starting repair!");
                    heartBeatTimer = scheduleHeartBeat(localState);
                    doTask(localState);
                }
                else
                {
                    logger.info("Not repairing node state: {}", localState);
                }

                if (assessCurrentPostRepairHooks(localState, ranTask))
                {
                    ranHooks = true;
                    doPostRepairHooks(localState);
                }
                else
                {
                    //TODO make this debug level instead of info
                    logger.info("Not running hooks, node state: {}", localState);
                }
            }
        }
        catch (Exception e) {
            logger.error("Exception in running repair. Cleaning up and exiting the state machine", e);
            if (taskId > 0 && localState != null && localState.sequence != null)
            {
                // Cancel just to be extra safe, if we encounter exceptions and a node is running
                // repair we want to try to cancel any ongoing repairs (to e.g. prevent duplicate
                // repaired ranges.
                currentTask.cancelTaskOnNode(taskId, "Exception in TaskManager", localState.sequence);
            }
        } finally {
            if (ranTask)
                hasTaskInitiated.set(false);

            if (ranHooks)
                hasHookInitiated.set(false);

            if (heartBeatTimer != null) {
                heartBeatTimer.cancel();
                heartBeatTimer.purge();
            }
        }

        return taskId;
    }


    /**
     * Aborts the repair state machine, and if run on the machine running the repair
     * will also immediately cancel repairs
     */
    public void pauseTasksOnCluster() {
        int currentTaskId = currentTask.getTaskId();
        // Mark paused prior to canceling any outstanding repairs so that other threads see this sentinel and stop
        // scheduling new repairs via iRepairController.canRunRepair
        if (currentTaskId > 0)
        {
            logger.info("Trying to pause the currently running currentTask for taskId: {}", currentTaskId);
            taskLifecycle.pauseTaskOnCluster(currentTaskId);
        } else {
            logger.info("Task is not currently running on the cluster, hence nothing to pause.");
        }

        //FIXME: This needs some kind of locking
        if (currentTask != null)
            currentTask.cancelTaskOnNode(currentTaskId, "Tasks paused", null);
    }


    /** State Machine **/


    // State (A)
    private int assessCurrentTask()
    {
        int taskId = -1;
        Optional<ClusterTaskStatus> crs = taskLifecycle.getClusterTaskStatus();

        if (!crs.isPresent() || shouldGenerateSequence(crs.get()))
        {
            int newTaskId = crs.map(clusterRepairStatus -> (clusterRepairStatus.getTaskId() + 1))
                               .orElse(1);
            taskId = generateTaskClusterSequence(newTaskId);
        }
        else if (crs.get().getTaskStatus().isPaused())
        {
            logger.debug("Repair is paused at cluster level since {}", crs.get().getPauseTime());
            return -1;
        }
        else if (crs.get().getTaskStatus().isStarted())
        {
            taskId = crs.get().getTaskId();
            if (currentTask.isSequenceGenerationStuckOnCluster(taskId))
            {
                logger.error("Detected that no repair sequence was generated within process_timeout_seconds!");
                currentTask.abortTaskOnCluster(taskId);
                return -1;
            }
        }
        else if (crs.get().getTaskStatus().isHookRunning())
        {
            taskId = crs.get().getTaskId();
        }
        logger.info("Found running repairID: {}", taskId);
        return taskId;
    }

    // State (B)
    private LocalTaskStatus assessCurrentTask(int activeRepairId)
    {
        if (currentTask.isSequenceRunningOnCluster(activeRepairId))
        {
            return new LocalTaskStatus(activeRepairId, null, false, NodeStatus.RUNNING_ELSEWHERE);
        }

        Optional<TaskSequence> mySeq = currentTask.amINextInSequenceOrDone(activeRepairId);

        if (!mySeq.isPresent())
        {
            return new LocalTaskStatus(activeRepairId, null, false, NodeStatus.NOT_NEXT);
        }

        TaskStatus status = mySeq.get().getStatus();

        if (status == TaskStatus.FINISHED)
        {
            return new LocalTaskStatus(activeRepairId, mySeq.get(), false, NodeStatus.I_AM_DONE);
        }
        else if (status.isCompleted() || status == TaskStatus.NOT_STARTED || status == TaskStatus.STARTED)
        {
            if (hasTaskInitiated.compareAndSet(false, true))
                return new LocalTaskStatus(activeRepairId, mySeq.get(), true, NodeStatus.NOT_RUNNING);
            else
                return new LocalTaskStatus(activeRepairId, mySeq.get(), false, NodeStatus.RUNNING);
        }
        else
        {
            return new LocalTaskStatus(activeRepairId, mySeq.get(), false, NodeStatus.HOOK_RUNNING);
        }
    }

    /**
     * State C
     *
     * In the case where we can't run repair on this node, we may be able to run post repair hooks if all
     * neighbors have repaired.
     *
     * @param taskState The current state of repair on this node
     * @param ranRepair
     * @return true if this node can run post repair hooks at this time, false otherwise
     */
    private boolean assessCurrentPostRepairHooks(LocalTaskStatus taskState, boolean ranRepair)
    {
        boolean canRunHooks = (taskState.nodeStatus == NodeStatus.I_AM_DONE ||
                               taskState.nodeStatus == NodeStatus.RUNNING_ELSEWHERE ||
                               ranRepair);

        boolean finished = false;
        if (currentTask.isTaskDoneOnCluster(taskState.taskId) && canRunHooks)
        {
            // This is almost always false
            finished = taskLifecycle.finishClusterTask(taskState);
        }

        // Another thread can't be actively running repair hooks at this time.
        canRunHooks = canRunHooks && hasHookInitiated.compareAndSet(false, true);
        if (canRunHooks)
            canRunHooks = canRunHooks && currentTask.amIReadyForPostTaskHook(taskState.taskId);

        return !finished && canRunHooks;
    }

    // State (D)
    private boolean shouldGenerateSequence(ClusterTaskStatus clusterTaskStatus)
    {
        if (clusterTaskStatus.getTaskStatus().readyToStartNew())
        {
            Map<String, List<TableTaskConfig>> schedules = taskLifecycle.getTableTaskConfigBySchedule();
            // We only support a single schedule right now
            if (schedules.size() > 1)
                throw new RuntimeException(
                    String.format("Task scheduler only works with one schedule right now found: %s", schedules.keySet())
                );

            Optional<Map.Entry<String, List<TableTaskConfig>>> schedule = schedules.entrySet().stream().findFirst();
            if (schedule.isPresent())
            {
                List<TableTaskConfig> allTableConfigs = schedule.get().getValue();
                if (allTableConfigs.size() > 0)
                {
                    allTableConfigs.sort(Comparator.comparing(TableTaskConfig::getInterTaskDelayMinutes));
                    DateTime earlierTableRepairDt = new DateTime(clusterTaskStatus.getEndTime()).plusHours(allTableConfigs.get(0).getInterTaskDelayMinutes());

                    if (DateTime.now().isAfter(earlierTableRepairDt)
                        || (clusterTaskStatus.getTaskDurationInMinutes() + allTableConfigs.get(0).getInterTaskDelayMinutes() * 60 >= 7 * 24 * 60)) //If repair duration time and min delay hours time is making up to more than 7 days, then start repair
                    {
                        return true;
                    }
                    else
                    {
                        logger.warn("This node has recently repaired and does not need repair at this moment, recent repair completed time: {}," +
                                    " next potential repair start time: {}", clusterTaskStatus.getEndTime().toString(), earlierTableRepairDt.toString(TaskUtil.DATE_PATTERN));
                        return false;
                    }
                }
                else
                {
                    logger.warn("No tables discovered, hence nothing to repair.");
                    return false;
                }
            }
        }
        return false;
    }

    // State (D) -> (E)
    private int generateTaskClusterSequence(int proposedTaskId)
    {
        if (taskLifecycle.attemptClusterTaskStart(proposedTaskId))
        {
            // Version 1 of repair scheduler only supports one schedule.
            currentTask.populateTaskSequence(proposedTaskId, context.getConfig().getDefaultSchedule());
            logger.info("Successfully generated repair sequence for taskId {} with schedule {}",
                        proposedTaskId, context.getConfig().getDefaultSchedule());
            return proposedTaskId;
        }
        return -1;
    }

    // State (F)
    private void doTask(final LocalTaskStatus taskStatus) throws InterruptedException
    {
        int taskId = taskStatus.taskId;
        TaskSequence seq = taskStatus.sequence;

        currentTask.prepareForTaskOnNode(taskId, seq);
        TaskStatus taskResult = currentTask.runTask(taskStatus);
        currentTask.cleanupAfterTaskOnNode(seq, taskResult);

        if (hasTaskInitiated.compareAndSet(true, false)) {
            logger.debug("Marking task completed on node.");
        } else {
            logger.warn("Some other thread has already set [hasTaskInitiated] to false, this is probably a bug");
        }
    }

    // State (G)
    private void doPostRepairHooks(final LocalTaskStatus repairState)
    {
        int repairId = repairState.taskId;
        TaskSequence seq = repairState.sequence;
        Map<String, Boolean> hookSucceeded = new HashMap<>();

        try
        {
            logger.info("Starting post repair hook(s) for taskId: {}", repairId);
            List<TableTaskConfig> repairHookEligibleTables = taskLifecycle.prepareForRepairHookOnNode(seq);


            for (TableTaskConfig tableConfig : repairHookEligibleTables) {
                // Read post repair hook types for the table, Load valid post repair hook types
                List<ITaskHook> repairHooks = TaskHookManager.getRepairHooks(tableConfig.getPostTaskHooks());

                // Run the post repair hooks in the order that was read above in sync fashion
                // This way users can e.g. cleanup before compacting or vice versa
                for (ITaskHook repairHook : repairHooks) {
                    try {
                        logger.info("Starting post repair hook [{}] on table [{}]",
                                    repairHook.getName(), tableConfig.getKsTbName());
                        boolean success = currentTask.runHook(repairHook, tableConfig);
                        hookSucceeded.compute(repairHook.getName(),
                                              (key, value) -> value == null ? success : value && success);
                        logger.info("Completed post repair hook [{}] on table [{}], success: [{}]",
                                    repairHook.getName(), tableConfig.getKsTbName(), success);

                    } catch (Exception e) {
                        hookSucceeded.put(repairHook.getName(), false);
                        logger.error("Exception in running post repair hook [{}] on table [{}].",
                                     repairHook.getName(), tableConfig.getKsTbName(), e);
                    }
                }
            }
        }
        catch (Exception e)
        {
            hookSucceeded.put("Exception", false);
            logger.error("Exception during hook", e);
        }

        taskLifecycle.cleanupAfterHookOnNode(seq, hookSucceeded);
    }

    /*
     * Helper Methods
     */


    private Timer scheduleHeartBeat(final LocalTaskStatus repairState) {
        int delay = 100;
        int period = context.getConfig().getHeartbeatPeriodInMs();

        Timer timer = new Timer();
        logger.info("Scheduling HeartBeat sent event with initial delay of {} ms and interval of {} ms.", delay,period);
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                taskLifecycle.publishHeartBeat(repairState.taskId, repairState.sequence.getSeq());
            }
        }, delay, period);
        return timer;
    }
}
