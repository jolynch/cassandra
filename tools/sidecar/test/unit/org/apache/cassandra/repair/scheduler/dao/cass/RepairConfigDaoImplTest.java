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

package org.apache.cassandra.repair.scheduler.dao.cass;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.entity.RepairOptions;
import org.apache.cassandra.repair.scheduler.entity.RepairSplitStrategy;
import org.apache.cassandra.repair.scheduler.entity.RepairType;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class RepairConfigDaoImplTest extends EmbeddedUnitTestBase
{
    private IRepairConfigDao repairConfigDao;

    @Before
    public void beforeMethod()
    {
        RepairSchedulerContext context = getContext();
        repairConfigDao = new RepairConfigDaoImpl(context, getCassDaoUtil());

        TableRepairConfig testConfig = new TableRepairConfig(context.getConfig(), context.getConfig().getDefaultSchedule());
        RepairOptions options = testConfig.getRepairOptions();

        options.setNumWorkers(17)
               .setSplitStrategy("12345")
               .setType(RepairType.fromString("FULL"))
               .setParallelism(RepairParallelism.fromName("sequential"));

        testConfig.setKeyspace("test_ks")
                  .setName("test_tbl")
                  .setRepairOptions(options)
                  .setInterRepairDelayMinutes(123);

        repairConfigDao.saveRepairConfig("default", testConfig);
    }

    @Test
    public void getRepairConfigs()
    {
        List<TableRepairConfig> repairConfigs = repairConfigDao.getRepairConfigs( "default");
        Assert.assertEquals(1, repairConfigs.size());
        Assert.assertEquals(RepairParallelism.SEQUENTIAL, repairConfigs.get(0).getRepairOptions().getParallelism());
        Assert.assertEquals(RepairSplitStrategy.Strategy.PARTITION, repairConfigs.get(0).getRepairOptions().getSplitStrategy().getStrategy());
        Assert.assertEquals(12345, repairConfigs.get(0).getRepairOptions().getSplitStrategy().getValue());
    }
}