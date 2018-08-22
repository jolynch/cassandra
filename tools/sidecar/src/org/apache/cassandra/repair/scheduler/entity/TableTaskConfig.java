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

public class TableTaskConfig
{
    private static final Logger logger = LoggerFactory.getLogger(TableTaskConfig.class);

    private String name;
    private String keyspace;
    private String schedule;
    private RepairOptions repairOptions;
    private List<String> postTaskHooks;
    private int interRepairDelayMinutes;

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
        this.repairOptions = new RepairOptions(config, schedule);
        this.postTaskHooks = config.getHooks(schedule);
        if (config.getIntertaskDelayMinutes(schedule) >= 0)
        {
            interRepairDelayMinutes = config.getIntertaskDelayMinutes(schedule);
        }
        else
        {
            logger.warn("Setting interRepairDelayMinutes to < 0 is not allowed, using default of {}", 1440);
            interRepairDelayMinutes = 1440;
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
    @JsonIgnore
    public boolean isRepairEnabled()
    {
        return repairOptions.getType() != RepairType.DISABLED;
    }

    @JsonIgnore
    public boolean isIncremental()
    {
        return repairOptions.getType().equals(RepairType.INCREMENTAL);
    }

    public Integer getInterRepairDelayMinutes()
    {
        return interRepairDelayMinutes;
    }

    public TableTaskConfig setInterRepairDelayMinutes(int interRepairDelayMinutes)
    {
        if (interRepairDelayMinutes < 0)
        {
            logger.warn("Setting intertask_delay_minutes to < 0 is not allowed, using default of {}", this.interRepairDelayMinutes);
            return this;
        }
        this.interRepairDelayMinutes = interRepairDelayMinutes;
        return this;
    }

    public RepairOptions getRepairOptions()
    {
        return repairOptions;
    }

    public TableTaskConfig setRepairOptions(RepairOptions repairOptions)
    {
        this.repairOptions = repairOptions;
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
        this.repairOptions = tableConfig.getRepairOptions();
        this.interRepairDelayMinutes = tableConfig.getInterRepairDelayMinutes();
        this.postTaskHooks = tableConfig.getPostTaskHooks();
        return this;
    }

    @Override
    public String toString()
    {
        return "TableConfig{" + "name='" + name + '\'' +
               ", keyspace='" + keyspace + '\'' +
               ", schedule=" + schedule +
               ", interRepairDelayMinutes=" + interRepairDelayMinutes +
               ", repairOptions=" + repairOptions +
               ", postTaskHooks=" + postTaskHooks +
               '}';
    }

    public Map<String, String> toMap()
    {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("keyspace", keyspace);
        configMap.put("name", name);
        configMap.put("schedule", schedule);
        configMap.put("inter_repair_delay_minutes", String.valueOf(interRepairDelayMinutes));
        configMap.put("repair_type", repairOptions.getType().toString());
        configMap.put("repair_workers", String.valueOf(repairOptions.getNumWorkers()));
        configMap.put("repair_split_strategy", repairOptions.getSplitStrategy().toString());
        configMap.put("repair_parallelism", repairOptions.getParallelism().toString());
        configMap.put("repair_seconds_to_wait", String.valueOf(repairOptions.getSecondsToWait()));
        return configMap;
    }
}
