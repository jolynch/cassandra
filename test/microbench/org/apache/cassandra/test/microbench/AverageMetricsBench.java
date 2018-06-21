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

package org.apache.cassandra.test.microbench;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import org.apache.cassandra.metrics.ExponentialMovingAverage;
import org.apache.cassandra.metrics.ExponentialMovingAverageTest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;


@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3)
@State(Scope.Benchmark)
@Fork(2)
public class AverageMetricsBench
{
    private static final Logger logger = LoggerFactory.getLogger(ExponentialMovingAverageTest.class);
    private ExponentialMovingAverage sharedEma;
    private ExponentiallyDecayingReservoir sharedReservoir;

    private final int NUM_UPDATES = 10000;

    @Setup(Level.Trial)
    public void trialSetup()
    {
        sharedEma = new ExponentialMovingAverage(0.25, 0);
        sharedReservoir = new ExponentiallyDecayingReservoir();
    }

    @Benchmark
    public void emaUpdate() throws Exception
    {
        ExponentialMovingAverage avg = new ExponentialMovingAverage(0.25, 0);
        for (int i = 0; i < NUM_UPDATES; i++)
        {
            avg.update((double) i);
        }
        avg.getAvg();
    }

    @Benchmark
    public void resevoirUpdate()
    {
        ExponentiallyDecayingReservoir reservoir = new ExponentiallyDecayingReservoir();

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            reservoir.update(i);
        }
        reservoir.getSnapshot();
    }

    @Benchmark
    @Threads(10)
    public void emaThreadedUpdate() throws Exception
    {
        double value = ThreadLocalRandom.current().nextDouble(50, 100);

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            sharedEma.update(value);
        }
        //sharedEma.getAvg();
    }

    @Benchmark
    @Threads(10)
    public void reservoirThreaded() throws Exception
    {
        int value = ThreadLocalRandom.current().nextInt(50, 100);

        for (int i = 0; i < NUM_UPDATES; i++)
        {
            sharedReservoir.update(value);
        }
        //sharedReservoir.getSnapshot().getMean();
    }
}
