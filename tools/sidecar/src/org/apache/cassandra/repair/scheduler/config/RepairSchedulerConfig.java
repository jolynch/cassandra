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
public class RepairSchedulerConfig
{
    public volatile boolean repair_scheduler_enabled = true;

    public int repair_api_port = 7198;

    public String cassandra_interaction_class = "org.apache.cassandra.repair.scheduler.conn.Cass4xInteraction";

    public String local_jmx_address = "127.0.0.1";

    public int local_jmx_port = 7199;

    public List<String> repair_state_persistence_endpoints = Collections.singletonList("127.0.0.1:9042");

    public String repair_native_endpoint = "127.0.0.1:9042";

    public int jmx_connection_monitor_period_in_ms = 60000;

    public int jmx_cache_ttl = 10000;

    public String jmx_username = null;

    public String jmx_password = null;

    public ConsistencyLevel read_cl = ConsistencyLevel.LOCAL_QUORUM;

    public ConsistencyLevel write_cl = ConsistencyLevel.LOCAL_QUORUM;

    public String repair_keyspace = "system_distributed";

    public String repair_sequence_tablename = "repair_sequence";

    public String repair_process_tablename = "repair_process";

    public String repair_config_tablename = "repair_config";

    public String repair_status_tablename = "repair_status";

    public String repairhook_status_tablename = "repair_hook_status";

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
        return repair_api_port;
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

    public String getRepairKeyspace()
    {
        return repair_keyspace;
    }

    public String getRepairSequenceTableName()
    {
        return repair_sequence_tablename;
    }

    public String getRepairProcessTableName()
    {
        return repair_process_tablename;
    }

    public String getRepairConfigTableName()
    {
        return repair_config_tablename;
    }

    public String getRepairStatusTableName()
    {
        return repair_status_tablename;
    }

    public String getRepairHookStatusTableName()
    {
        return repairhook_status_tablename;
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

    /**
     * Gets default schedule, v1 only supports 1 schedule.
     *
     * @return Default schedule
     */
    public String getDefaultSchedule()
    {
        return default_schedules.get(0);
    }

    public int getRepairTimeoutInS(String schedule)
    {
        return schedules.get(schedule).repair_timeout_in_s;
    }

    public int getDefaultRepairTimeoutInS()
    {
        return schedules.get(default_schedules.get(0)).repair_timeout_in_s;
    }

    public String getRepairType(String schedule)
    {
        return schedules.get(schedule).repair_type;
    }

    public int getWorkers(String schedule)
    {
        return schedules.get(schedule).workers;
    }

    public List<String> getHooks(String schedule)
    {
        return schedules.get(schedule).hooks;
    }

    public String getParallelism(String schedule)
    {
        return schedules.get(schedule).parallelism;
    }

    public String getSplitStrategy(String schedule)
    {
        return schedules.get(schedule).split_strategy;
    }

    public int getInterrepairDelayMinutes(String schedule)
    {
        return schedules.get(schedule).interrepair_delay_minutes;
    }

    public int getRepairWaitForHealthyInMs(String schedule)
    {
        return schedules.get(schedule).repair_wait_for_healthy_in_ms;
    }

    public List<String> getRepairStatePersistenceEndpoints()
    {
        return repair_state_persistence_endpoints;
    }

    public String getRepairNativeEndpoint()
    {
        return repair_native_endpoint;
    }
}
