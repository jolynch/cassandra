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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalRepairState;
import org.apache.cassandra.repair.scheduler.entity.NodeStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.hooks.IRepairHook;
import org.apache.cassandra.repair.scheduler.hooks.RepairHookManager;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.apache.cassandra.repair.scheduler.tasks.BaseTask;
import org.apache.cassandra.repair.scheduler.tasks.IManagementTask;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.joda.time.DateTime;

public class TaskManager
{
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private static final int REPAIR_THREAD_CNT = Runtime.getRuntime().availableProcessors();

    private final TaskSchedulerContext context;
    Map<String, IManagementTask> tasks;
    private final ListeningScheduledExecutorService executorService;
    private final Set<Future> repairFutures = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean hasRepairInitiated = new AtomicBoolean(false);
    private final AtomicBoolean hasHookInitiated = new AtomicBoolean(false);

    // This changes every time that a task is chosen to execute
    private volatile IManagementTask currentTask;

    /**
     * Gets repairHook from class name
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

        executorService = MoreExecutors.listeningDecorator(
            Executors.newScheduledThreadPool(REPAIR_THREAD_CNT,
                                             new NamedThreadFactory("TaskManager"))
        );

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
     * @return A positive repairId if the state machine was entered or -1 otherwise
     */
    public int runRepairOnCluster()
    {
        int repairId = -1;
        Timer heartBeatTimer = null;
        LocalRepairState localState = null;
        boolean ranRepair, ranHooks;
        ranRepair = ranHooks = false;

        try {
            repairId = assessCurrentTask();
            if (repairId > 0)
            {
                localState = assessCurrentTask(repairId);
                logger.info("Found local repair state: {}", localState);
                if (localState.canRepair)
                {
                    ranRepair = true;
                    logger.info("Starting repair!");
                    heartBeatTimer = scheduleHeartBeat(localState);
                    doTask(localState);
                }
                else
                {
                    logger.info("Not repairing node state: {}", localState);
                }

                if (assessCurrentPostRepairHooks(localState, ranRepair))
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
            if (repairId > 0 && localState != null && localState.sequence != null)
            {
                // Cancel just to be extra safe, if we encounter exceptions and a node is running
                // repair we want to try to cancel any ongoing repairs (to e.g. prevent duplicate
                // repaired ranges.
                currentTask.cancelTaskOnNode(repairId, "Exception in TaskManager", localState.sequence);
            }
        } finally {
            if (ranRepair)
                hasRepairInitiated.set(false);

            if (ranHooks)
                hasHookInitiated.set(false);

            if (heartBeatTimer != null) {
                heartBeatTimer.cancel();
                heartBeatTimer.purge();
            }
        }

        return repairId;
    }


    /**
     * Aborts the repair state machine, and if run on the machine running the repair
     * will also immediately cancel repairs
     */
    public void pauseTasksOnCluster() {
        int currentTaskId = currentTask.getTaskId();
        // Mark paused prior to canceling any outstanding repairs so that other threads see this sentinel and stop
        // scheduling new repairs via iRepairController.canRunTask
        if (currentTaskId > 0)
        {
            logger.info("Trying to pause the currently running currentTask for taskId: {}", currentTaskId);
            currentTask.pauseTaskOnCluster(currentTaskId);
        } else {
            logger.info("Task is not currently running on the cluster, hence nothing to pause.");
        }

        // This has to be synchronized because another thread might be in the middle of scheduling futures. All
        // methods which generate repairFutures also lock on this and check if the repair is paused before
        // scheduling the repair.
        synchronized (repairFutures) {
            repairFutures.stream().filter(Objects::nonNull)
                         .forEach(future -> future.cancel(true));
            currentTask.cancelTaskOnNode(currentTaskId, "Tasks paused", null);
        }

    }


    /** State Machine **/


    // State (A)
    private int assessCurrentTask()
    {
        int taskId = -1;
        Optional<ClusterTaskStatus> crs = currentTask.getClusterTaskStatus();

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
    private LocalRepairState assessCurrentTask(int activeRepairId)
    {
        if (currentTask.isSequenceRunningOnCluster(activeRepairId))
        {
            return new LocalRepairState(activeRepairId, null, false, NodeStatus.RUNNING_ELSEWHERE);
        }

        Optional<RepairSequence> mySeq = currentTask.amINextInSequenceOrDone(activeRepairId);

        if (!mySeq.isPresent())
        {
            return new LocalRepairState(activeRepairId, null, false, NodeStatus.NOT_NEXT);
        }

        TaskStatus status = mySeq.get().getStatus();

        if (status == TaskStatus.FINISHED)
        {
            return new LocalRepairState(activeRepairId, mySeq.get(), false, NodeStatus.I_AM_DONE);
        }
        else if (status.isCompleted() || status == TaskStatus.NOT_STARTED || status == TaskStatus.STARTED)
        {
            if (hasRepairInitiated.compareAndSet(false, true))
                return new LocalRepairState(activeRepairId, mySeq.get(), true, NodeStatus.NOT_RUNNING);
            else
                return new LocalRepairState(activeRepairId, mySeq.get(), false, NodeStatus.RUNNING);
        }
        else
        {
            return new LocalRepairState(activeRepairId, mySeq.get(), false, NodeStatus.HOOK_RUNNING);
        }
    }

    /**
     * State C
     *
     * In the case where we can't run repair on this node, we may be able to run post repair hooks if all
     * neighbors have repaired.
     *
     * @param repairState The current state of repair on this node
     * @param ranRepair
     * @return true if this node can run post repair hooks at this time, false otherwise
     */
    private boolean assessCurrentPostRepairHooks(LocalRepairState repairState, boolean ranRepair)
    {
        boolean canRunHooks = (repairState.nodeStatus == NodeStatus.I_AM_DONE ||
                               repairState.nodeStatus == NodeStatus.RUNNING_ELSEWHERE ||
                               ranRepair);

        boolean finished = false;
        if (currentTask.isTaskDoneOnCluster(repairState.repairId) && canRunHooks)
        {
            // This is almost always false
            finished = currentTask.finishClusterRepair(repairState);
        }

        // Another thread can't be actively running repair hooks at this time.
        canRunHooks = canRunHooks && hasHookInitiated.compareAndSet(false, true);
        if (canRunHooks)
            canRunHooks = canRunHooks && currentTask.amIReadyForPostTaskHook(repairState.repairId);

        return !finished && canRunHooks;
    }

    // State (D)
    private boolean shouldGenerateSequence(ClusterTaskStatus clusterTaskStatus)
    {
        if (clusterTaskStatus.getTaskStatus().readyToStartNew())
        {
            Map<String, List<TableTaskConfig>> schedules = currentTask.getRepairableTablesBySchedule();
            // We only support a single schedule right now
            if (schedules.size() > 1)
                throw new RuntimeException(
                    String.format("Repair scheduler only works with one schedule right now found: %s", schedules.keySet())
                );

            Optional<Map.Entry<String, List<TableTaskConfig>>> schedule = schedules.entrySet().stream().findFirst();
            if (schedule.isPresent())
            {
                List<TableTaskConfig> allTableConfigs = schedule.get().getValue();
                if (allTableConfigs.size() > 0)
                {
                    allTableConfigs.sort(Comparator.comparing(TableTaskConfig::getInterRepairDelayMinutes));
                    DateTime earlierTableRepairDt = new DateTime(clusterTaskStatus.getEndTime()).plusHours(allTableConfigs.get(0).getInterRepairDelayMinutes());

                    if (DateTime.now().isAfter(earlierTableRepairDt)
                        || (clusterTaskStatus.getTaskDurationInMinutes() + allTableConfigs.get(0).getInterRepairDelayMinutes() * 60 >= 7 * 24 * 60)) //If repair duration time and min delay hours time is making up to more than 7 days, then start repair
                    {
                        return true;
                    }
                    else
                    {
                        logger.warn("This node has recently repaired and does not need repair at this moment, recent repair completed time: {}," +
                                    " next potential repair start time: {}", clusterTaskStatus.getEndTime().toString(), earlierTableRepairDt.toString(RepairUtil.DATE_PATTERN));
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
        if (currentTask.attemptClusterTaskStart(proposedTaskId))
        {
            // Version 1 of repair scheduler only supports one schedule.
            currentTask.populateTaskSequence(proposedTaskId, context.getConfig().getDefaultSchedule());
            logger.info("Successfully generated repair sequence for repairId {} with schedule {}",
                        proposedTaskId, context.getConfig().getDefaultSchedule());
            return proposedTaskId;
        }
        return -1;
    }

    // State (F)
    private void doTask(final LocalRepairState repairState) throws InterruptedException
    {
        int repairId = repairState.repairId;
        RepairSequence seq = repairState.sequence;
        String scheduleName = seq.getScheduleName();

        List<TableTaskConfig> tablesToRepair = currentTask.getTablesForRepair(repairId, scheduleName);
        currentTask.prepareForTaskOnNode(repairId, seq);

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

            if (currentTask.canRunTask(repairId))
            {
                List<Range<Token>> tableFailedRanges = executeRepair(repairId, tableToRepair);
                failedRanges.get(tableToRepair.getKsTbName()).addAll(tableFailedRanges);
                needsRetry = !tableFailedRanges.isEmpty();
            }

            if (needsRetry)
            {
                tableRetries.compute(tableToRepair.getKsTbName(), (key, value) -> value == null ? 1 : 1 + value);
                int secondsToSleep = 10;
                Thread.sleep(secondsToSleep * 1000);
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

        currentTask.cleanupAfterRepairOnNode(repairState, finalStatus);

        if (hasRepairInitiated.compareAndSet(true, false)) {
            logger.debug("Done marking repair completed on node");
        } else {
            logger.warn("Some other thread has already set [hasRepairInitiated] to false, this is probably a bug");
        }

        logger.info("All tables repaired, {} ranges failed. Exiting", numFailedRanges);
    }

    // State (G)
    private void doPostRepairHooks(final LocalRepairState repairState)
    {
        int repairId = repairState.repairId;
        RepairSequence seq = repairState.sequence;
        Map<String, Boolean> hookSucceeded = new HashMap<>();

        try
        {
            logger.info("Starting post repair hook(s) for repairId: {}", repairId);
            List<TableTaskConfig> repairHookEligibleTables = currentTask.prepareForRepairHookOnNode(seq);


            for (TableTaskConfig tableConfig : repairHookEligibleTables) {
                // Read post repair hook types for the table, Load valid post repair hook types
                List<IRepairHook> repairHooks = RepairHookManager.getRepairHooks(tableConfig.getPostTaskHooks());

                // Run the post repair hooks in the order that was read above in sync fashion
                // This way users can e.g. cleanup before compacting or vice versa
                for (IRepairHook repairHook : repairHooks) {
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

        currentTask.cleanupAfterHookOnNode(seq, hookSucceeded);
    }

    /*
     * Helper Methods
     */


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
        final List<Range<Token>> repairRanges = currentTask.getRangesForRepair(repairId, tableConfig);
        if (repairRanges.size() == 0)
            return Collections.emptyList();

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
            Range<Token> subRange = runningRepairRanges.take();
            final ListenableFuture<Boolean> future;

            // Repair might get paused _mid_ repair so we have to check if the cluster is paused
            // on every RepairRunner generation
            synchronized (repairFutures) {
                if (currentTask.isSequencePausedOnCluster(repairId))
                {
                    List<Range<Token>> ranges = new ArrayList<>(pendingRepairRanges);
                    ranges.addAll(runningRepairRanges);
                    ranges.addAll(failedRepairRanges);
                    return ranges;
                }

                final RepairRunner repairRunner = new RepairRunner(repairId, tableConfig, subRange,
                                                                   context, RepairSchedulerMetrics.instance,
                                                                   currentTask, globalProgress);

                logger.info("Scheduling Repair for [{}] on range {}", tableConfig.getKsTbName(), subRange);

                future = executorService.submit(repairRunner);
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
                currentTask.cancelRepairOnNode();
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

    private Timer scheduleHeartBeat(final LocalRepairState repairState) {
        int delay = 100;
        int period = context.getConfig().getHeartbeatPeriodInMs();

        Timer timer = new Timer();
        logger.info("Scheduling HeartBeat sent event with initial delay of {} ms and interval of {} ms.", delay,period);
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                currentTask.publishHeartBeat(repairState.repairId, repairState.sequence.getSeq());
            }
        }, delay, period);
        return timer;
    }
}
