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

package org.apache.cassandra.repair.scheduler.dao.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

/**
 * Interface to get Table repair configurations/ overrides from configuration file defaults.
 */
public interface IRepairConfigDao
{
    /**
     * Get table repair configurations/ overrides for all schedules to current cluster
     * This is part of the interface when repair metadata persistent store is different from repairing cluster
     *
     * @return  Map of {@link TableRepairConfig} list keyed by schedule name
     */
    Map<String, List<TableRepairConfig>> getRepairConfigs();

    /**
     * Get table repair configurations/ overrides from defaults for a given schedule name to current cluster
     * @param scheduleName Schedule name to get the configs for
     * @return List of {@link TableRepairConfig} objects
     */
    List<TableRepairConfig> getRepairConfigs(String scheduleName);

    /**
     * Gets all available repair schedules. These schedule names are from both config file and repair_config table
     * @return Set of schedule names
     */
    Set<String> getAllRepairSchedules();

    /**
     * Save table level repair config overrides per a given schedule
     * @param schedule Schedule name to save the configs in
     * @param repairConfig Table level repair configs/ overrides
     * @return boolean indicating the operation result.
     */
    boolean saveRepairConfig(String schedule, TableRepairConfig repairConfig);

    /**
     * Gets all repair enabled tables for a give schedule. Get all tables by connecting local C* node and overlay
     * that information with config information from repair_config,
     * using defaults for any tables not found in repair_config.
     *
     * @param scheduleName Schedule name
     * @return List of TableRepairConfigs
     */
    List<TableRepairConfig> getAllRepairEnabledTables(String scheduleName);
}
