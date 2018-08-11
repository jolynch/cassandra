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

import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

/**
 * Performs Compaction after repair. This class compacts everything
 * regardless of the compaction strategy (LCS, TWCS etc.,). If you are looking for STCS compaction only,
 * {@link CompactSTCSRepairHook} would be the alternative
 */
public class CompactAllRepairHook implements IRepairHook
{
    @Override
    public String getName()
    {
        return "COMPACT_ALL";
    }

    @Override
    public void run(CassandraInteraction interaction, TableRepairConfig tableConfig)
    {
        interaction.triggerCompaction(tableConfig.getKeyspace(), tableConfig.getName());
    }
}