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

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.metrics.RepairSchedulerMetrics;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.joda.time.DateTime;


class RepairRunner implements Callable<Boolean>, NotificationListener
{

    private static final Logger logger = LoggerFactory.getLogger(RepairRunner.class);
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    private final Condition progress = new SimpleCondition();
    private final Condition globalProgress;
    private final TaskSchedulerContext context;
    private final RepairSchedulerMetrics metrics;
    private final RepairTask repairTask;
    private final int secondsToWait;
    private final String keyspace;
    private final String table;
    private final Range<Token> range;
    private final RepairParallelism repairParallelism;
    private final boolean fullRepair;
    private final String schedule;
    private volatile boolean isDone = false;
    private int cmd;
    private boolean success = true;
    private volatile Exception error = null;
    private final TaskMetadata taskMetadata;
    private Timer.Context repairTimingContext;


    RepairRunner(int repairId, TableTaskConfig tableTaskConfig, Range<Token> range,
                 TaskSchedulerContext context, RepairSchedulerMetrics metrics,
                 RepairTask repairTask, Condition globalProgress)
    {
        this.range = range;
        this.context = context;
        this.metrics = metrics;
        this.repairTask = repairTask;
        this.secondsToWait = tableTaskConfig.getRepairOptions().getSecondsToWait();
        this.keyspace = tableTaskConfig.getKeyspace();
        this.table = tableTaskConfig.getName();
        this.repairParallelism = tableTaskConfig.getRepairOptions().getParallelism();
        this.fullRepair = !tableTaskConfig.isIncremental();
        this.schedule = tableTaskConfig.getSchedule();
        this.globalProgress = progress;

        taskMetadata = new TaskMetadata();
        taskMetadata.setKeyspaceName(keyspace)
                    .setTableName(table)
                    .setTaskId(repairId)
                    .setClusterName(context.getCassInteraction().getClusterName())
                    .setNodeId(context.getCassInteraction().getLocalHostId())
                    .setStartToken(range.left.toString())
                    .setRepairConfig(tableTaskConfig)
                    .setEndToken(range.right.toString())
                    .setStatus(TaskStatus.STARTED)
        ;
    }


    @Override
    public Boolean call() throws Exception
    {
        return repairAndWait();
    }

    private void runRepair()
    {
        taskMetadata.setCreatedTime(DateTime.now().toDate());

        if (waitForHealthy())
        {
            repairTimingContext = metrics.getRepairDuration().time();
            metrics.incNumTimesRepairStarted();
            taskMetadata.setStartTime(DateTime.now().toDate());

            cmd = context.getCassInteraction().triggerRepair(Collections.singletonList(range),
                                                             repairParallelism, fullRepair, keyspace, table);

            synchronized (taskMetadata)
            {
                // The notification listener can actually beat us to this, since that
                // uses an upsert, we can just not run this upsert before or after and it should still work
                taskMetadata.setRepairNum(cmd);
                repairTask.getRepairStatusDao().markTaskStatusChange(taskMetadata);
            }
            handleRepairResult(cmd);
        }
        else
        {
            this.metrics.incNumTimesRepairFailed();

            logger.error("Failed to run repair on [{}].[{}] as token range/neighbours are not healthy, waited [{}] millis before giving up.",
                         keyspace, table, context.getConfig()
                                                 .getRepairWaitForHealthyInMs(schedule));
            taskMetadata.setRepairNum(-1)
                        .setStartTime(DateTime.now().toDate())
                        .setStatus(TaskStatus.FAILED)
                        .setEndTime(DateTime.now().toDate())
                        .setLastEvent("Failed on " + keyspace + "." + table + " " + range,
                                        "Timed out waiting for token range health");

            repairTask.getRepairStatusDao().markTaskStatusChange(taskMetadata);
        }
    }

    private boolean repairAndWait() throws Exception
    {
        // setup the callbacks
        this.context.getCassInteraction().addRepairNotificationListener(this);
        try
        {
            runRepair();
            waitForRepair();
        }
        finally
        {
            this.context.getCassInteraction().removeRepairNotificationListener(this);
        }
        return success;
    }

    private void handleRepairResult(int cmd)
    {
        if (cmd > 0)
        {
            logger.info("Repair Command [{}] has been issued, Notification listener will start monitoring it", cmd);
        }
        else if (cmd == 0)
        {
            logger.info("Repair Command [{}] returned with code zero, indicating a RF=1 column family", cmd);
            recordRepairResult(TaskStatus.FINISHED, "Repair Command returned with zero code, indicating RF=1");
            metrics.incNumTimesRepairCompleted();
        }
        else
        {
            success = false;
            metrics.incNumTimesRepairFailed();
            recordRepairResult(TaskStatus.FAILED, "Repair Command returned with code " + cmd);

            logger.warn("Repair Command [{}] was returned, nothing to repair with given parameters. Possible causes, Given range is incorrect, No data in the given range, given KS and CF exists, ReplicationFactor is <2", cmd);
        }
    }

    private void waitForRepair() throws Exception
    {
        try
        {
            if (cmd > 0)
            {
                while (!isDone && error == null && success)
                {
                    if (!progress.await(secondsToWait, TimeUnit.SECONDS))
                    {
                        error = new TimeoutException(
                        String.format("Waited %d seconds for repair to make any progress but it did not, timing out",
                                      secondsToWait)
                        );
                    }
                }
            }

            if (error != null)
            {
                throw error;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in waiting for repair to complete. RepairCmd: {}", cmd, e);
            if (error == null)
            {
                error = e;
            }
            recordRepairResult(TaskStatus.FAILED, "Encountered exception: " + e.toString());
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean waitForHealthy()
    {
        Boolean result;
        FutureTask<Boolean> future = new FutureTask(() -> {
            int iterations = 0;
            while (true)
            {
                if (repairTask.areNeighborsHealthy(this.keyspace, this.range))
                {
                    return true;
                }
                iterations += 1;
                if (iterations % 60 == 0)
                    logger.debug("Slept for 60s waiting for neighbors to become healthy for keyspace: {}", keyspace);
                Thread.sleep(1000);
            }
        });
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(future);
        try
        {
            result = future.get(context.getConfig().getRepairWaitForHealthyInMs(schedule), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex)
        {
            logger.debug("Waited {} millis for this token range/ cluster to become healthy, but it did not",
                         context.getConfig().getRepairWaitForHealthyInMs(schedule));
            future.cancel(true);
            result = false;
        }
        catch (InterruptedException | ExecutionException e)
        {
            logger.error("Could not wait for {} millis for this token range/ cluster to become healthy, Got this exception. ",
                         context.getConfig().getRepairWaitForHealthyInMs(schedule), e);
            future.cancel(true);
            result = false;
        }
        finally
        {
            service.shutdown();
        }
        return result;
    }

    private void recordRepairResult(TaskStatus status, String event)
    {
        repairTimingContext.stop();
        boolean markedStatusChange;

        synchronized (taskMetadata)
        {
            taskMetadata.setRepairNum(cmd)
                        .setEndTime(DateTime.now().toDate())
                        .setStatus(status)
                        .setLastEvent("Complete Reason", event);

            switch (status)
            {
                case FAILED:
                    success = false;
                    metrics.incNumTimesRepairFailed();
                    break;
                case NOTIFS_LOST:
                case FINISHED:
                    metrics.incNumTimesRepairCompleted();
                    break;
            }

            markedStatusChange = repairTask.getRepairStatusDao().markTaskStatusChange(taskMetadata);
        }


        if (markedStatusChange)
        {
            isDone = true;
            progress.signalAll();
            globalProgress.signalAll();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleNotification(Notification notification, Object handback)
    {
        if ("progress".equals(notification.getType()) &&
            notification.getSource().equals("repair:" + cmd))
        {
            Map<String, Integer> progress = (Map<String, Integer>) notification.getUserData();
            ProgressEvent event = new ProgressEvent(ProgressEventType.values()[progress.get("type")],
                                                    progress.get("progressCount"),
                                                    progress.get("total"),
                                                    notification.getMessage());
            String message = String.format("[%s]-[%d] %s", format.format(notification.getTimeStamp()), cmd, event.getMessage());
            if (event.getType() == ProgressEventType.PROGRESS)
            {
                message = message + " (progress: " + (int) event.getProgressPercentage() + "%)";
                this.progress.signalAll();
                globalProgress.signalAll();
            }
            logger.debug(message);
            if (event.getType() == ProgressEventType.ERROR || event.getType() == ProgressEventType.ABORT)
            {
                recordRepairResult(TaskStatus.FAILED, "Received ProgressEventType " + event.getType().toString());
            }
            else if (event.getType() == ProgressEventType.SUCCESS)
            {
                recordRepairResult(TaskStatus.FINISHED, "Received ProgressEventType " + event.getType().toString());
            }
        }
        else if (JMXConnectionNotification.NOTIFS_LOST.equals(notification.getType()))
        {
            String message = String.format("[%s] Lost notification. You should check server log for repair status of keyspace %s",
                                           format.format(notification.getTimeStamp()), keyspace);
            logger.warn(message);
            recordRepairResult(TaskStatus.NOTIFS_LOST, "Lost JMX connection");
        }
        else if (JMXConnectionNotification.FAILED.equals(notification.getType())
                 || JMXConnectionNotification.CLOSED.equals(notification.getType()))
        {
            String message = String.format("[%s] JMX connection closed. You should check server log for repair status of keyspace %s",
                                           format.format(notification.getTimeStamp()), keyspace);
            logger.warn(message);
            recordRepairResult(TaskStatus.NOTIFS_LOST, "JMX connection closed ");
        }
    }
}
