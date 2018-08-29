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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * A class that contains configuration properties for the RepairScheduler on the node
 */
public class TaskSchedulerConfig
{
    public volatile boolean repair_scheduler_enabled = true;

    public int task_api_port = 7198;

    public String cassandra_interaction_class = "org.apache.cassandra.repair.scheduler.conn.Cass4xInteraction";

    public String local_jmx_address = "127.0.0.1";

    public int local_jmx_port = 7199;

    public List<String> task_state_persistence_endpoints = Collections.singletonList("127.0.0.1:9042");

    public String task_native_endpoint = "127.0.0.1:9042";

    public int jmx_connection_monitor_period_in_ms = 60000;

    public int jmx_cache_ttl = 10000;

    public String jmx_username = null;

    public String jmx_password = null;

    public ConsistencyLevel read_cl = ConsistencyLevel.LOCAL_QUORUM;

    public ConsistencyLevel write_cl = ConsistencyLevel.LOCAL_QUORUM;

    public String task_keyspace = "system_distributed";

    public String task_sequence_tablename = "task_sequence";

    public String task_process_tablename = "task_process";

    public String task_table_status_tablename = "task_table_status";

    public String task_hook_sequence_tablename = "task_hook_sequence";

    public String table_config_tablename = "repair_config";

    public int heartbeat_period_in_ms = 2000;

    public int repair_entryttl_in_days = 182;

    public int repair_process_timeout_in_s = 1800;

    public List<String> default_schedules = Collections.singletonList("default");

    public Map<String, ScheduleConfig> schedules = Collections.singletonMap("default", new ScheduleConfig());

    // Getters for RepairScheduler configuration

    public boolean isRepairSchedulerEnabled()
    {
        return repair_scheduler_enabled;
    }

    public void setRepairSchedulerEnabled(boolean repairSchedulerEnabled)
    {
        repair_scheduler_enabled = repairSchedulerEnabled;
    }

    public int getRepairAPIPort()
    {
        return task_api_port;
    }

    public String getCassandraInteractionClass()
    {
        return cassandra_interaction_class;
    }

    public String getLocalJmxAddress()
    {
        return local_jmx_address;
    }

    public int getLocalJmxPort()
    {
        return local_jmx_port;
    }

    public int getJmxConnectionMonitorPeriodInMs()
    {
        return jmx_connection_monitor_period_in_ms;
    }

    public int getJmxCacheTTL()
    {
        return jmx_cache_ttl;
    }

    public ConsistencyLevel getReadCl()
    {
        return read_cl;
    }

    public ConsistencyLevel getWriteCl()
    {
        return write_cl;
    }

    public String getTaskKeyspace()
    {
        return task_keyspace;
    }

    public String getTaskSequenceTableName()
    {
        return task_sequence_tablename;
    }

    public String getTaskProcessTableName()
    {
        return task_process_tablename;
    }

    public String getTaskTableStatusTableName()
    {
        return task_table_status_tablename;
    }

    public String getTableHookSequenceTableName()
    {
        return task_hook_sequence_tablename;
    }

    public String getTableConfigTableName()
    {
        return table_config_tablename;
    }

    public int getHeartbeatPeriodInMs()
    {
        return heartbeat_period_in_ms;
    }

    public int getRepairProcessTimeoutInS()
    {
        return repair_process_timeout_in_s;
    }

    public List<String> getDefaultSchedules()
    {
        return default_schedules;
    }

    public Map<String, ScheduleConfig> getScheduleConfigs()
    {
        return schedules;
    }

    /**
     * Gets default schedule, v1 only supports 1 schedule.
     *
     * @return Default schedule
     */
    public String getDefaultSchedule()
    {
        return default_schedules.get(0);
    }

    public String getTaskName(String schedule)
    {
        return schedules.get(schedule).task_name;
    }

    public String getTaskType(String schedule)
    {
        return schedules.get(schedule).task_type;
    }

    public int getTaskTimeoutInS(String schedule)
    {
        return schedules.get(schedule).task_timeout_seconds;
    }

    public int getDefaultTaskTimeoutInS()
    {
        return schedules.get(default_schedules.get(0)).task_timeout_seconds;
    }

    public List<String> getHooks(String schedule)
    {
        return schedules.get(schedule).hooks;
    }

    public Map<String, String> getTaskConfig(String schedule)
    {
        return schedules.get(schedule).task_config;
    }

    public int getIntertaskDelayMinutes(String schedule)
    {
        return schedules.get(schedule).intertask_delay_minutes;
    }

    public List<String> getRepairStatePersistenceEndpoints()
    {
        return task_state_persistence_endpoints;
    }

    public String getRepairNativeEndpoint()
    {
        return task_native_endpoint;
    }
}
