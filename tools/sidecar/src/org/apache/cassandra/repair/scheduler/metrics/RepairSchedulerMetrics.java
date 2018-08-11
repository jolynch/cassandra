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
package org.apache.cassandra.repair.scheduler.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for RepairScheduler
 */
public class RepairSchedulerMetrics
{
    public static final RepairSchedulerMetrics instance = new RepairSchedulerMetrics();

    //=============================================================================
    // Counters concerning repair start, stop, failure, stuck cancel etc...
    //=============================================================================

    /**
     * Number of times repair started on this node
     */
    private final Counter numTimesRepairStarted;

    /**
     * Number of times repair failed on this node
     */
    private final Counter numTimesRepairFailed;

    /**
     * Number of times repair canceled on this node
     */
    private final Counter numTimesRepairCancelled;

    /**
     * Number of times repair completed on this node
     */
    private final Counter numTimesRepairCompleted;

    /**
     * Number of times repair stuck on this node
     */
    private final Counter numTimesRepairStuck;

    //=============================================================================
    // Counters for repair hook start, stop, fail, etc...
    //=============================================================================

    /**
     * Number of times repair hook started this node
     */
    private final Counter numTimesRepairHookStarted;

    /**
     * Number of times repair hook failed this node
     */
    private final Counter numTimesRepairHookFailed;

    /**
     * Number of times repair hook completed this node
     */
    private final Counter numTimesRepairHookCompleted;

    // Time to measure the repair duration
    private final Timer repairDuration;

    private RepairSchedulerMetrics()
    {
        MetricNameFactory factory = new DefaultNameFactory("RepairScheduler");
        String namePrefix = "RepairScheduler-";

        numTimesRepairStarted = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairStarted"));
        numTimesRepairCancelled = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairCancelled"));
        numTimesRepairFailed = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairFailed"));
        numTimesRepairCompleted = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairCompleted"));
        numTimesRepairStuck = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairStuck"));
        numTimesRepairHookStarted = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairHookStarted"));
        numTimesRepairHookFailed = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairHookFailed"));
        numTimesRepairHookCompleted = Metrics.counter(factory.createMetricName(namePrefix + "NumTimesRepairHookCompleted"));

        repairDuration = Metrics.timer(factory.createMetricName(namePrefix + "SingleRepairDurationSeconds"));
    }

    /**
     * Increment number of times repair started on this node
     */
    public void incNumTimesRepairStarted()
    {
        numTimesRepairStarted.inc();
    }

    /**
     * Increment number of times repair canceled on this node
     */
    public void incNumTimesRepairCancelled()
    {
        numTimesRepairCancelled.inc();
    }

    /**
     * Increment number of times repair failed on this node
     */
    public void incNumTimesRepairFailed()
    {
        numTimesRepairFailed.inc();
    }

    /**
     * Increment number of times repair completed on this node
     */
    public void incNumTimesRepairCompleted()
    {
        numTimesRepairCompleted.inc();
    }

    /**
     * Increment number of times repair hook stuck on this node
     */
    public void incNumTimesRepairStuck()
    {
        numTimesRepairStuck.inc();
    }

    /**
     * Increment number of times repair hook started this node
     */
    public void incNumTimesRepairHookStarted()
    {
        numTimesRepairHookStarted.inc();
    }

    /**
     * Increment number of times repair hook failed on this node
     */
    public void incNumTimesRepairHookFailed()
    {
        numTimesRepairHookFailed.inc();
    }

    /**
     * Increment number of time Repair Hook completed
     */
    public void incNumTimesRepairHookCompleted()
    {
        numTimesRepairHookCompleted.inc();
    }

    /**
     * Gets the repair duration timer
     *
     * @return RepairDuration
     */
    public Timer getRepairDuration()
    {
        return repairDuration;
    }
}
