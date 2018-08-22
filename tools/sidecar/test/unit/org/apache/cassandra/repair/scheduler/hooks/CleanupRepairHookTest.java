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

package org.apache.cassandra.repair.scheduler.hooks;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CleanupRepairHookTest extends EmbeddedUnitTestBase
{
    private CassandraInteraction interactionSpy;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        //Mock-Spy
        TaskSchedulerContext contextSpy = Mockito.spy(context);
        interactionSpy = Mockito.spy(context.getCassInteraction());
        when(contextSpy.getCassInteraction()).thenReturn(interactionSpy);
    }

    @Test
    public void verifyRepirHook_Cleanup() throws Exception
    {
        CleanupRepairHook repairHook = new CleanupRepairHook();
        TableTaskConfig tableTaskConfig = new TableTaskConfig(context.getConfig(), context.getConfig().getDefaultSchedule()).setKeyspace(REPAIR_SCHEDULER_KS_NAME).setName("repair_config");

        repairHook.run(this.interactionSpy, tableTaskConfig);

        verify(this.interactionSpy, times(1)).triggerCleanup(0, REPAIR_SCHEDULER_KS_NAME, "repair_config");
    }
}