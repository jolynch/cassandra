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
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalRepairState;
import org.apache.cassandra.repair.scheduler.entity.NodeStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;
import org.apache.cassandra.repair.scheduler.hooks.IRepairHook;
import org.apache.cassandra.repair.scheduler.hooks.RepairHookManager;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.joda.time.DateTime;

public class RepairManager
{
    private static final Logger logger = LoggerFactory.getLogger(RepairManager.class);
    private static final int REPAIR_THREAD_CNT = Runtime.getRuntime().availableProcessors();

    private final RepairSchedulerContext context;
    private final RepairController repairController;
    private final ListeningScheduledExecutorService executorService;
    private final Set<Future> repairFutures = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean hasRepairInitiated = new AtomicBoolean(false);
    private final AtomicBoolean hasHookInitiated = new AtomicBoolean(false);


    @VisibleForTesting
    public RepairManager(RepairSchedulerContext context)
    {
        this.context = context;

        repairController = new RepairController(context, new RepairDaoManager(context));
        executorService = MoreExecutors.listeningDecorator(
            Executors.newScheduledThreadPool(REPAIR_THREAD_CNT,
                                             new NamedThreadFactory("RepairManager"))
        );

        logger.info("RepairManager Initialized.");
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
            repairId = assessCurrentRepair();
            if (repairId > 0)
            {
                localState = assessCurrentRepair(repairId);
                logger.info("Found local repair state: {}", localState);
                if (localState.canRepair)
                {
                    ranRepair = true;
                    logger.info("Starting repair!");
                    heartBeatTimer = scheduleHeartBeat(localState);
                    doRepair(localState);
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
                repairController.cancelRepairOnNode(repairId, localState.sequence, "Exception in RepairManager");
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
    public void pauseRepairOnCluster() {
        int currentRepairId = repairController.getRepairId();
        // Mark paused prior to canceling any outstanding repairs so that other threads see this sentinel and stop
        // scheduling new repairs via iRepairController.canRunRepair
        if (currentRepairId > 0)
        {
            logger.info("Trying to pause the currently running repair for repairId: {}", currentRepairId);
            repairController.pauseRepairOnCluster(currentRepairId);
        } else {
            logger.info("Repair is not currently running on the cluster, hence nothing to pause.");
        }

        // This has to be synchronized because another thread might be in the middle of scheduling futures. All
        // methods which generate repairFutures also lock on this and check if the repair is paused before
        // scheduling the repair.
        synchronized (repairFutures) {
            repairFutures.stream().filter(Objects::nonNull)
                         .forEach(future -> future.cancel(true));
            repairController.cancelRepairOnNode();
        }

    }


    /** State Machine **/


    // State (A)
    private int assessCurrentRepair()
    {
        int repairId = -1;
        Optional<ClusterRepairStatus> crs = repairController.getClusterRepairStatus();

        if (!crs.isPresent() || shouldGenerateSequence(crs.get()))
        {
            int newRepairId = crs.map(clusterRepairStatus -> (clusterRepairStatus.getRepairId() + 1))
                                 .orElse(1);
            repairId = generateRepairClusterSequence(newRepairId);
        }
        else if (crs.get().getRepairStatus().isPaused())
        {
            logger.debug("Repair is paused at cluster level since {}", crs.get().getPauseTime());
            return -1;
        }
        else if (crs.get().getRepairStatus().isStarted())
        {
            repairId = crs.get().getRepairId();
            if (repairController.isRepairSequenceGenerationStuckOnCluster(repairId))
            {
                logger.error("Detected that no repair sequence was generated within process_timeout_seconds!");
                repairController.abortRepairOnCluster(repairId);
                return -1;
            }
        }
        else if (crs.get().getRepairStatus().isRepairHookRunning())
        {
            repairId = crs.get().getRepairId();
        }
        logger.info("Found running repairID: {}", repairId);
        return repairId;
    }

    // State (B)
    private LocalRepairState assessCurrentRepair(int activeRepairId)
    {
        if (repairController.isRepairRunningOnCluster(activeRepairId))
        {
            return new LocalRepairState(activeRepairId, null, false, NodeStatus.RUNNING_ELSEWHERE);
        }
        else
        {
            Optional<RepairSequence> mySeq = repairController.amINextInRepairOrDone(activeRepairId);
            if (!mySeq.isPresent())
            {
                return new LocalRepairState(activeRepairId, null, false, NodeStatus.NOT_NEXT);
            }
            else if (mySeq.get().getStatus().isCompleted())
            {
                return new LocalRepairState(activeRepairId, mySeq.get(), false, NodeStatus.I_AM_DONE);
            }
            else if (mySeq.get().getStatus() == RepairStatus.NOT_STARTED)
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
        if (repairController.isRepairDoneOnCluster(repairState.repairId) && canRunHooks)
        {
            // This is almost always false
            finished = repairController.finishClusterRepair(repairState);
        }

        // Another thread can't be actively running repair hooks at this time.
        canRunHooks = canRunHooks && hasHookInitiated.compareAndSet(false, true);
        if (canRunHooks)
            canRunHooks = canRunHooks && repairController.amIReadyForPostRepairHook(repairState.repairId);

        return !finished && canRunHooks;
    }

    // State (D)
    private boolean shouldGenerateSequence(ClusterRepairStatus clusterRepairStatus)
    {
        if (clusterRepairStatus.getRepairStatus().readyToStartNew())
        {
            Map<String, List<TableRepairConfig>> schedules = repairController.getRepairableTablesBySchedule();
            // We only support a single schedule right now
            if (schedules.size() > 1)
                throw new RuntimeException(
                    String.format("Repair scheduler only works with one schedule right now found: %s", schedules.keySet())
                );

            Optional<Map.Entry<String, List<TableRepairConfig>>> schedule = schedules.entrySet().stream().findFirst();
            if (schedule.isPresent())
            {
                List<TableRepairConfig> allTableConfigs = schedule.get().getValue();
                if (allTableConfigs.size() > 0)
                {
                    allTableConfigs.sort(Comparator.comparing(TableRepairConfig::getInterRepairDelayMinutes));
                    DateTime earlierTableRepairDt = new DateTime(clusterRepairStatus.getEndTime()).plusHours(allTableConfigs.get(0).getInterRepairDelayMinutes());

                    if (DateTime.now().isAfter(earlierTableRepairDt)
                        || (clusterRepairStatus.getRepairDurationInMin() + allTableConfigs.get(0).getInterRepairDelayMinutes() * 60 >= 7 * 24 * 60)) //If repair duration time and min delay hours time is making up to more than 7 days, then start repair
                    {
                        return true;
                    }
                    else
                    {
                        logger.warn("This node has recently repaired and does not need repair at this moment, recent repair completed time: {}," +
                                    " next potential repair start time: {}", clusterRepairStatus.getEndTime().toString(), earlierTableRepairDt.toString(RepairUtil.DATE_PATTERN));
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
    private int generateRepairClusterSequence(int proposedRepairId)
    {
        if (repairController.attemptClusterRepairStart(proposedRepairId))
        {
            // Version 1 of repair scheduler only supports one schedule.
            repairController.populateEndpointSequenceMap(proposedRepairId, context.getConfig().getDefaultSchedule());
            logger.info("Successfully generated repair sequence for repairId {} with schedule {}",
                        proposedRepairId, context.getConfig().getDefaultSchedule());
            return proposedRepairId;
        }
        return -1;
    }

    // State (F)
    private void doRepair(final LocalRepairState repairState) throws InterruptedException
    {
        int repairId = repairState.repairId;
        RepairSequence seq = repairState.sequence;
        String scheduleName = seq.getScheduleName();

        List<TableRepairConfig> tablesToRepair = repairController.getTablesForRepair(repairId, scheduleName);
        repairController.prepareForRepairOnNode(repairId, seq);

        Map<String, Integer> tableRetries = new HashMap<>();
        Queue<TableRepairConfig> tablesToRepairQueue = new LinkedBlockingQueue<>(tablesToRepair.size());
        tablesToRepairQueue.addAll(tablesToRepair);

        while (!tablesToRepairQueue.isEmpty())
        {
            boolean needsRetry;
            TableRepairConfig tableToRepair = tablesToRepairQueue.poll();
            if (tableToRepair == null || !tableToRepair.isRepairEnabled())
               continue;

            if (repairController.canRunRepair(repairId))
                needsRetry = !executeRepair(repairId, tableToRepair);
            else
                needsRetry = true;

            if (needsRetry)
            {
                tableRetries.compute(tableToRepair.getKsTbName(), (key, value) -> value == null ? 1 : 1 + value);
                int secondsToSleep = 10;
                Thread.sleep(secondsToSleep * 1000);
                int total_retries = tableRetries.values().stream().mapToInt(v->v).sum();
                if ((secondsToSleep * total_retries) > context.getConfig().getRepairProcessTimeoutInS())
                {
                    logger.error("Timed out waiting for running repair or pause to finish, exiting state machine");
                    break;
                }
                if (tableRetries.get(tableToRepair.getKsTbName()) < 3)
                    tablesToRepairQueue.offer(tableToRepair);
            }

        }

        logger.debug("Ensuring that all repairs are finished/killed before marking instance complete");
        repairController.cleanupAfterRepairOnNode(repairState);

        if (hasRepairInitiated.compareAndSet(true, false)) {
            logger.debug("Done marking repair completed on node");
        } else {
            logger.warn("Some other thread has already set [hasRepairInitiated] to false, this is probably a bug");
        }

        logger.info("All tables repaired, no currently running repairs. Really exiting...");
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
            List<TableRepairConfig> repairHookEligibleTables = repairController.prepareForRepairHookOnNode(seq);


            for (TableRepairConfig tableConfig : repairHookEligibleTables) {
                // Read post repair hook types for the table, Load valid post repair hook types
                List<IRepairHook> repairHooks = RepairHookManager.getRepairHooks(tableConfig.getPostRepairHooks());

                // Run the post repair hooks in the order that was read above in sync fashion
                // This way users can e.g. cleanup before compacting or vice versa
                for (IRepairHook repairHook : repairHooks) {
                    try {
                        logger.info("Starting post repair hook [{}] on table [{}]",
                                    repairHook.getName(), tableConfig.getKsTbName());
                        boolean success = repairController.runHook(repairHook, tableConfig);
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

        repairController.cleanupAfterHookOnNode(seq, hookSucceeded);
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
     * @return if this table needs to be retried.
     */
    private boolean executeRepair(int repairId, TableRepairConfig tableConfig) throws InterruptedException
    {
        // Includes both multiple ranges (vnodes) or full repair using subranges.
        // Also automatically handles splits for full vs no splits for incremental
        final List<Range<Token>> repairRanges = repairController.getRangesForRepair(repairId, tableConfig);
        if (repairRanges.size() == 0)
            return false;

        int tableWorkers = tableConfig.getRepairOptions().getNumWorkers();
        final LinkedBlockingQueue<Range<Token>> pendingRepairRanges = new LinkedBlockingQueue<>(repairRanges);
        final LinkedBlockingQueue<Range<Token>> runningRepairRanges = new LinkedBlockingQueue<>(tableWorkers);
        final LinkedBlockingQueue<Range<Token>> failedRepairRanges = new LinkedBlockingQueue<>();
        final AtomicInteger numTodo = new AtomicInteger(repairRanges.size());
        final Condition progress = new SimpleCondition();


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
                if (repairController.isRepairPausedOnCluster(repairId)) {
                    return false;
                }

                final RepairRunner repairRunner = new RepairRunner(repairId, tableConfig, subRange,
                                                                   context, RepairSchedulerMetrics.instance,
                                                                   repairController);

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
                    progress.signalAll();
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
            if (!progress.await(tableConfig.getRepairOptions().getSecondsToWait(), TimeUnit.SECONDS))
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
                repairController.cancelRepairOnNode();
                // Timeout, we should cancel repair so as to not overload the cluster
                // TODO: What do we do here...
            }
        } else {
            logger.info("Done with Repair on table: {}!", tableConfig.getKsTbName());
        }
        return true;
    }

    private Timer scheduleHeartBeat(final LocalRepairState repairState) {
        int delay = 100;
        int period = context.getConfig().getHeartbeatPeriodInMs();

        Timer timer = new Timer();
        logger.info("Scheduling HeartBeat sent event with initial delay of {} ms and interval of {} ms.", delay,period);
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                repairController.publishHeartBeat(repairState.repairId, repairState.sequence.getSeq());
            }
        }, delay, period);
        return timer;
    }
}
