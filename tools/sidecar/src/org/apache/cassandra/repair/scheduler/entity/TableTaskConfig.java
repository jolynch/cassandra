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
package org.apache.cassandra.repair.scheduler.entity;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.tasks.repair.RepairOptions;

public class TableTaskConfig
{
    private static final Logger logger = LoggerFactory.getLogger(TableTaskConfig.class);

    private String schedule;
    private String name;
    private String keyspace;
    private String taskType;
    private int interTaskDelayMinutes;
    private int taskTimeoutSeconds;
    private List<String> postTaskHooks;
    private Map<String, String> taskConfig;

    private TableMetadata tableMetadata;

    /**
     * Default constructor needed for Jackson JSON Deserialization
     */
    public TableTaskConfig()
    {

    }

    public TableTaskConfig(TaskSchedulerConfig config, String schedule)
    {
        this.schedule = schedule;
        postTaskHooks = config.getHooks(schedule);
        taskConfig = config.getTaskConfig(schedule);
        taskType = config.getTaskType(schedule);
        if (config.getIntertaskDelayMinutes(schedule) >= 0)
        {
            interTaskDelayMinutes = config.getIntertaskDelayMinutes(schedule);
        }
        else
        {
            logger.warn("Setting interTaskDelayMinutes to < 0 is not allowed, using default of {}", 1440);
            interTaskDelayMinutes = 1440;
        }
        if (config.getTaskTimeoutInS(schedule) >= 0)
        {
            taskTimeoutSeconds = config.getTaskTimeoutInS(schedule);
        }
        else
        {
            logger.warn("Setting task_timeout_seconds to < 0 is not allowed, using default of {}", 1800);
            taskTimeoutSeconds = 1800;
        }

        tableMetadata = null;
    }

    @JsonIgnore
    public String getKsTbName()
    {
        return keyspace + "." + name;
    }

    public String getName()
    {
        return name;
    }

    public TableTaskConfig setName(String name)
    {
        this.name = name;
        return this;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public TableTaskConfig setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
        return this;
    }

    public String getTaskType()
    {
        return taskType;
    }

    public TableTaskConfig setTaskType(String taskType)
    {
        this.taskType = taskType;
        return this;
    }

    public Integer getInterTaskDelayMinutes()
    {
        return interTaskDelayMinutes;
    }

    public TableTaskConfig setInterTaskDelayMinutes(int interTaskDelayMinutes)
    {
        if (interTaskDelayMinutes < 0)
        {
            logger.warn("Setting intertask_delay_minutes to < 0 is not allowed, using default of {}", this.interTaskDelayMinutes);
            return this;
        }
        this.interTaskDelayMinutes = interTaskDelayMinutes;
        return this;
    }

    public Integer getTaskTimeoutSeconds()
    {
        return taskTimeoutSeconds;
    }

    public TableTaskConfig setTaskTimeoutSeconds(int taskTimeoutSeconds)
    {
        if (taskTimeoutSeconds < 0)
        {
            logger.warn("Setting task_timeout_seconds to < 0 is not allowed, using default of {}", this.taskTimeoutSeconds);
            return this;
        }
        this.taskTimeoutSeconds = taskTimeoutSeconds;
        return this;
    }

    public String getSchedule()
    {
        return schedule;
    }

    public List<String> getPostTaskHooks() {
        return postTaskHooks;
    }

    public TableTaskConfig setPostTaskHooks(List<String> postTaskHooks)
    {
        this.postTaskHooks = postTaskHooks;
        return this;
    }

    public Map<String, String> getTaskConfig()
    {
        return taskConfig;
    }

    public TableTaskConfig setTaskConfig(Map<String, String> taskConfig)
    {
        this.taskConfig = taskConfig;
        return this;
    }

    public boolean shouldRunPostTaskHook()
    {
        return postTaskHooks.size() > 0;
    }

    @JsonIgnore
    public Optional<TableMetadata> getTableMetadata()
    {
        if (tableMetadata == null)
            return Optional.empty();
        return Optional.of(tableMetadata);
    }

    public TableTaskConfig setTableMetadata(TableMetadata tableMetadata)
    {
        this.tableMetadata = tableMetadata;
        return this;
    }

    /**
     * Clones all the properties that came from repair_config to provided tableConfig
     *
     * @param tableConfig
     * @return
     */
    public TableTaskConfig clone(TableTaskConfig tableConfig)
    {
        this.interTaskDelayMinutes = tableConfig.getInterTaskDelayMinutes();
        this.postTaskHooks = tableConfig.getPostTaskHooks();
        this.taskConfig = tableConfig.getTaskConfig();
        return this;
    }


    public Map<String, String> toMap()
    {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("keyspace", keyspace);
        configMap.put("name", name);
        configMap.put("schedule", schedule);
        configMap.put("inter_repair_delay_minutes", String.valueOf(interTaskDelayMinutes));
        configMap.put("task_timeout_seconds", String.valueOf(taskTimeoutSeconds));
        configMap.put("task_config", taskConfig.toString());
        configMap.put("hooks", postTaskHooks.toString());
        return configMap;
    }

    @Override
    public String toString()
    {
        return "TableTaskConfig{" +
               "schedule='" + schedule + '\'' +
               ", name='" + name + '\'' +
               ", keyspace='" + keyspace + '\'' +
               ", taskType='" + taskType + '\'' +
               ", interTaskDelayMinutes=" + interTaskDelayMinutes +
               ", taskTimeoutSeconds=" + taskTimeoutSeconds +
               ", postTaskHooks=" + postTaskHooks +
               ", taskConfig=" + taskConfig +
               ", tableMetadata=" + tableMetadata +
               '}';
    }
}
