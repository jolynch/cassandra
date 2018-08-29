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

package org.apache.cassandra.repair.scheduler.tasks.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.entity.RepairSplitStrategy;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;

public class RepairOptions
{
    private static final Logger logger = LoggerFactory.getLogger(RepairOptions.class);

    // What kind of repair (full, incremental, etc ...)
    private RepairType repairType;

    // Number of workers to use for parallelism
    private int numWorkers;

    // Strategy for splitting full range repairs into sub range repairs
    private RepairSplitStrategy splitStrategy;

    // Configuration of merkel tree parallelism
    private RepairParallelism parallelism;

    // Shared table config
    private TableTaskConfig tableTaskConfig;

    /**
     * Default constructor needed for Jackson JSON Deserialization
     */
    public RepairOptions()
    {

    }

    public RepairOptions(TaskSchedulerConfig config, TableTaskConfig tableTaskConfig)
    {
        this.tableTaskConfig = tableTaskConfig;

        // TODO: do we actually need to set this to 1?
        // In testing with concurrent sessions incremental would just get stuck a lot...
        if (repairOptions.getType() == RepairType.INCREMENTAL)
            repairOptions.setNumWorkers(1);

        if (hasColumn(row, "repair_timeout_seconds"))
            repairOptions.setSecondsToWait(row.getInt("repair_timeout_seconds"));

        tableTaskConfig.setRepairOptions(repairOptions);

        this.repairType = RepairType.fromString(tableTaskConfig.getTaskConfig()
                                                               .getOrDefault("repair_type", "disabled"));

        int workers = 0;
        try
        {
            workers = Integer.valueOf(taskConfig.getTaskConfig().get("workers"));
        }
            catch (NumberFormatException ignored) { }
        if (workers <= 0)
            numWorkers = Math.max(1, (int) Math.ceil(Runtime.getRuntime().availableProcessors() / 2));
        else
            numWorkers = workers;

        setSplitStrategy(taskConfig.getTaskConfig().getOrDefault("split_strategy", "disabled"));
        setType(RepairType.valueOf(taskConfig.getOrDefault("repair_type", )));
        setParallelism(RepairParallelism.fromName(config.getParallelism(schedule)));
        setSecondsToWait(config.getTaskTimeoutInS(schedule));
    }

    public RepairType getType()
    {
        return repairType;
    }

    public RepairOptions setType(RepairType repairType)
    {
        this.repairType = repairType;
        return this;
    }

    public RepairParallelism getParallelism()
    {
        return parallelism;
    }

    public RepairOptions setParallelism(RepairParallelism parallelism)
    {
        this.parallelism = parallelism;
        return this;
    }

    public int getNumWorkers()
    {
        return numWorkers;
    }

    public RepairOptions setNumWorkers(int numWorkers)
    {
        if (numWorkers <= 0)
        {
            logger.warn("Setting numWorkers to <= 0 is not allowed, using default of {}", numWorkers);
            return this;
        }
        this.numWorkers = numWorkers;
        return this;
    }

    public RepairSplitStrategy getSplitStrategy()
    {
        return splitStrategy;
    }

    public RepairOptions setSplitStrategy(String splitStrategy)
    {
        this.splitStrategy = new RepairSplitStrategy(splitStrategy);
        return this;
    }

    public TableTaskConfig getTableTaskConfig()
    {
        return tableTaskConfig;
    }

    public RepairOptions setTableTaskConfig(TableTaskConfig tableTaskConfig)
    {
        this.tableTaskConfig = tableTaskConfig;
        return this;
    }
}