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
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;

/**
 * Performs cleanup after repair
 */
public class CleanupTaskHook implements ITaskHook
{
    @Override
    public String getName()
    {
        return "CLEANUP";
    }

    @Override
    public TaskStatus run(CassandraInteraction interaction, TableTaskConfig tableConfig)
    {
        interaction.triggerCleanup(0, tableConfig.getKeyspace(), tableConfig.getName());
        return TaskStatus.FINISHED;
    }
}