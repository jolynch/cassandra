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

import java.util.List;
import java.util.SortedSet;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.entity.RepairOptions;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairType;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairManagerTest extends EmbeddedUnitTestBase
{
    RepairDaoManager daoManager;

    @Before
    public void setupManager() throws InterruptedException
    {
        loadDataset(100);
        daoManager = new RepairDaoManager(getContext());

        TableRepairConfig testConfig = new TableRepairConfig(getContext().getConfig(), context.getConfig().getDefaultSchedule());
        RepairOptions options = testConfig.getRepairOptions();

        // Full + subrange table
        options.setNumWorkers(2)
               .setSplitStrategy("1000")
               .setType(RepairType.fromString("full"))
               .setParallelism(RepairParallelism.fromName("sequential"));

        testConfig.setKeyspace("test_repair")
                  .setName("subrange_test")
                  .setRepairOptions(options)
                  .setInterRepairDelayMinutes(10);

        daoManager.getRepairConfigDao().saveRepairConfig("default", testConfig);

        // Incremental table
        options.setNumWorkers(2)
               .setSplitStrategy("100")
               .setType(RepairType.fromString("incremental"))
               .setParallelism(RepairParallelism.fromName("sequential"));

        testConfig.setKeyspace("test_repair")
                  .setName("incremental_test")
                  .setRepairOptions(options)
                  .setInterRepairDelayMinutes(10);

        daoManager.getRepairConfigDao().saveRepairConfig("default", testConfig);

        // Disabled table is inserted directly into the repair config to simulate someone supplying
        // _just_ the disabled type.
        getContext().localSession().execute(
        "INSERT INTO system_distributed.repair_config (cluster_name, schedule_name, keyspace_name, table_name, type) " +
        "VALUES ('" + context.getCassInteraction().getClusterName() + "', 'default', 'test_repair', 'no_repair', 'disabled')");
    }

    @Test
    public void testRunRepairOnCluster()
    {
        RepairManager rm = new RepairManager(getContext());
        int repairId = rm.runRepairOnCluster();
        // This is hard to test with a RF=1 cluster, Since everything is RF=1 we just have nothing to
        // repair and everything succeeds instantly. So ... check really basic stuff and leave the rest
        // to the e2e tests.
        assertEquals(1, repairId);
        SortedSet<RepairSequence> sequence = daoManager.getRepairSequenceDao().getRepairSequence(repairId);
        assertEquals(RepairStatus.FINISHED, sequence.first().getStatus());
        assertEquals(RepairStatus.REPAIR_HOOK_RUNNING,
                     daoManager.getRepairProcessDao().getClusterRepairStatus().get().getRepairStatus());

        // Now this should finish the repair
        assertEquals(1, rm.runRepairOnCluster());
        assertEquals(RepairStatus.FINISHED,
                     daoManager.getRepairProcessDao().getClusterRepairStatus().get().getRepairStatus());

        // Repair should be done now for a little while.
        assertEquals(-1, rm.runRepairOnCluster());
    }
}
