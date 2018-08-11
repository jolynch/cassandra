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
 * Pluggable Repair Hooks. Performs post repair actions.
 * Designed to run any action that can be wrapped in run() method.
 * Custom repair hooks has to implement this interface, preferably in this package so that configs
 * does not have to provide fully qualified class name.
 */
public interface IRepairHook
{
    /**
     * Log friendly name for the repair hook
     *
     * @return Name of the Repair hook
     */
    String getName();

    /**
     * Perform post repair hook action. E.g., compaction, cleanup, GET or POST rest calls
     *
     * @param interaction CassandraInteraction (Cassandra - JMX)
     * @param tableConfig TableConfig object
     * @throws Exception Exception in executing post repair hook
     */
    void run(CassandraInteraction interaction, TableRepairConfig tableConfig) throws Exception;
}

