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

package org.apache.cassandra.repair.scheduler.entity;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.tasks.repair.RepairOptions;
import org.apache.cassandra.repair.scheduler.tasks.repair.RepairType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class RepairOptionsTest extends EmbeddedUnitTestBase
{

    TaskSchedulerConfig config;

    private RepairOptions options;

    @Before
    public void beforeMethod() throws Exception
    {
        context = getContext();
        options = new RepairOptions(context.getConfig(), context.getConfig().getDefaultSchedule());
        config = context.getConfig();
    }

    @Test
    public void getRepairType() throws Exception
    {
        Assert.assertEquals(options.getType(), RepairType.FULL);
    }

    @Test
    public void getNumWorkers() throws Exception
    {
        int numProcessors = Runtime.getRuntime().availableProcessors();
        Assert.assertTrue(options.getNumWorkers()== numProcessors/2);
    }

    @Test
    public void getConfiguredNumWorkers() throws Exception
    {
        int numProcessors = Runtime.getRuntime().availableProcessors();
        Assert.assertEquals(numProcessors/2, options.getNumWorkers());
    }

    @Test
    public void getSplitRange() throws Exception
    {
        Assert.assertEquals(options.getSplitStrategy().getStrategy(), RepairSplitStrategy.Strategy.ADAPTIVE);
    }

    @Test
    public void getSecondsToWait() throws Exception
    {
        Assert.assertEquals(options.getSecondsToWait(), config.getRepairProcessTimeoutInS());

    }

}