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

import java.util.function.Supplier;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;

public class CassDaoUtil
{
    private final Supplier<Session> repairSession;
    private final TaskSchedulerConfig config;

    public CassDaoUtil(TaskSchedulerConfig config, Supplier<Session> repairSession)
    {
        this.repairSession = repairSession;
        this.config = config;
    }

    ResultSet execSelectStmtRepairDb(Statement statement)
    {
        return repairSession.get()
                            .execute(statement.setIdempotent(true)
                                              .setConsistencyLevel(config.getReadCl()));
    }

    ResultSet execUpsertStmtRepairDb(Statement statement)
    {
        return repairSession.get()
                            .execute(statement.setIdempotent(true)
                                              .setConsistencyLevel(config.getWriteCl()));
    }

    ResultSet execSerialUpsertStmtRepairDb(Statement statement)
    {
        return repairSession.get()
                            .execute(statement.setIdempotent(true)
                                              .setConsistencyLevel(config.getWriteCl())
                                              .setSerialConsistencyLevel(ConsistencyLevel.SERIAL));
    }
}
