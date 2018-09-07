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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;
import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompactAllRepairHookTest extends EmbeddedUnitTestBase
{
    private CassandraInteraction interactionSpy;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        //Mock-Spy
        RepairSchedulerContext contextSpy = Mockito.spy(context);
        interactionSpy = Mockito.spy(context.getCassInteraction());
        when(contextSpy.getCassInteraction()).thenReturn(interactionSpy);
    }

    @Test
    public void verifySTCSRepirHook_Compaction()
    {
        CompactAllRepairHook repairHook = new CompactAllRepairHook();

        TableMetadata tblMetadata = context.localSession().getCluster().getMetadata().getKeyspace(REPAIR_SCHEDULER_KS_NAME).getTable("repair_config");
        TableRepairConfig tableRepairConfig = new TableRepairConfig(context.getConfig(),
                                                                    context.getConfig().getDefaultSchedule());

        TableMetadata tblMetadataSpy = Mockito.spy(tblMetadata);
        TableOptionsMetadata optionsSpy = Mockito.spy(tblMetadataSpy.getOptions());

        tableRepairConfig.setKeyspace(REPAIR_SCHEDULER_KS_NAME).setName("repair_config")
                         .setTableMetadata(tblMetadataSpy);

        Map<String, String> tblOptions = new HashMap<>();
        tblOptions.put("class", "SizeTiredCompactionStrategy");

        doReturn(optionsSpy).when(tblMetadataSpy).getOptions();
        doReturn(tblOptions).when(optionsSpy).getCompaction();

        repairHook.run(this.interactionSpy, tableRepairConfig);

        verify(this.interactionSpy, times(1)).triggerCompaction(REPAIR_SCHEDULER_KS_NAME, "repair_config");

    }

    @Test
    public void verifyLCSRepirHook_Compaction() throws Exception
    {
        CompactAllRepairHook repairHook = new CompactAllRepairHook();

        TableMetadata tblMetadata = context.localSession().getCluster().getMetadata().getKeyspace(REPAIR_SCHEDULER_KS_NAME).getTable("repair_config");
        TableRepairConfig tableRepairConfig = new TableRepairConfig(context.getConfig(),
                                                                    context.getConfig().getDefaultSchedule())
                                              .setKeyspace(REPAIR_SCHEDULER_KS_NAME).setName("repair_config")
                                              .setTableMetadata(tblMetadata);

        repairHook.run(this.interactionSpy, tableRepairConfig);

        verify(this.interactionSpy, times(1)).triggerCompaction(REPAIR_SCHEDULER_KS_NAME, "repair_config");

    }

    @Test
    public void verifyNullMetadata_RepirHook_Compaction() throws Exception
    {
        CompactAllRepairHook repairHook = new CompactAllRepairHook();
        TableRepairConfig tableRepairConfig = new TableRepairConfig(context.getConfig(),
                                                                    context.getConfig().getDefaultSchedule())
                                              .setPostRepairHooks(Collections.singletonList("COMPACTION"))
                                              .setKeyspace(REPAIR_SCHEDULER_KS_NAME).setName("repair_config");

        repairHook.run(this.interactionSpy, tableRepairConfig);

        verify(this.interactionSpy, times(1)).triggerCompaction(REPAIR_SCHEDULER_KS_NAME, "repair_config");

    }
}