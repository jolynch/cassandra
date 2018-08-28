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

package org.apache.cassandra.repair.scheduler.config;

import com.datastax.driver.core.Session;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;

/**
 * TaskSchedulerContext holds C* session object to repairing cluster, config object
 * and C* interaction object. RepairScheduler needs these objects in many places,
 * instead of constructing them and passing them around, this Context class makes it easier to access
 */
public interface TaskSchedulerContext
{
    /**
     * Cassandra session object to local Cassandra cluster
     *
     * @return Session
     */
    Session localSession();

    /**
     * Returns the TaskSchedulerConfig object
     *
     * @return TaskSchedulerConfig
     */
    TaskSchedulerConfig getConfig();

    /**
     * Returns CassandraInteraction object
     *
     * @return CassandraInteraction
     */
    CassandraInteraction getCassInteraction();
}
