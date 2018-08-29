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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.ITableTaskConfigDao;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;

import static org.apache.cassandra.repair.scheduler.TaskUtil.getKsTbName;

public class TableTaskConfigDaoImpl implements ITableTaskConfigDao
{
    private static final Logger logger = LoggerFactory.getLogger(TaskHookDaoImpl.class);

    private final TaskSchedulerContext context;
    private static String clusterName;
    private final CassDaoUtil daoUtil;

    public TableTaskConfigDaoImpl(TaskSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.context = context;
    }

    /**
     * Get table task configurations/ overrides for all schedules
     *
     * @return Map of TableRepairConfigs list keyed by schedule name
     */
    @Override
    public Map<String, List<TableTaskConfig>> getTableTaskConfigs()
    {
        Map<String, List<TableTaskConfig>> tableConfigMap = new HashMap<>();
        Statement selectQuery = QueryBuilder.select()
                                            .from(context.getConfig().getTaskKeyspace(),
                                                  context.getConfig().getTableConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", getClusterName()));

        daoUtil.execSelectStmtRepairDb(selectQuery)
               .forEach(row ->
                        {
                            TableTaskConfig tableTaskConfig = getTableRepairConfig(row);
                            String schedule = row.getString("schedule_name");
                            tableConfigMap.computeIfAbsent(schedule, k -> new ArrayList<>());
                            tableConfigMap.get(schedule).add(tableTaskConfig);
                        });

        return tableConfigMap;
    }

    @Override
    public List<TableTaskConfig> getTableTaskConfigs(String scheduleName)
    {
        List<TableTaskConfig> lstTableTaskConfig = new ArrayList<>();
        Statement selectQuery = QueryBuilder.select()
                                            .from(context.getConfig().getTaskKeyspace(),
                                                  context.getConfig().getTableConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", getClusterName()))
                                            .and(QueryBuilder.eq("schedule_name", scheduleName));

        daoUtil.execSelectStmtRepairDb(selectQuery).forEach(row -> {
            TableTaskConfig TableTaskConfig = getTableRepairConfig(row);
            lstTableTaskConfig.add(TableTaskConfig);
        });

        return lstTableTaskConfig;
    }

    @Override
    public Set<String> getAllTaskSchedules()
    {
        Set<String> schedules = new HashSet<>();
        Statement selectQuery = QueryBuilder.select("schedule_name")
                                            .from(context.getConfig().getTaskKeyspace(),
                                                  context.getConfig().getTableConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", getClusterName()));

        daoUtil.execSelectStmtRepairDb(selectQuery)
               .forEach(row -> schedules.add(row.getString("schedule_name")));

        //Get default schedules from config
        schedules.addAll(context.getConfig().getDefaultSchedules());
        return schedules;
    }

    @Override
    public boolean saveTaskConfig(String schedule, TableTaskConfig repairConfig)
    {
        logger.info("Saving Repair Configuration for {}.{}", getClusterName(), schedule);
        try
        {
            Statement insertQuery = QueryBuilder.insertInto(context.getConfig().getTaskKeyspace(),
                                                            context.getConfig().getTableConfigTableName())
                                                .value("cluster_name", getClusterName())
                                                .value("schedule_name", schedule)
                                                .value("keyspace_name", repairConfig.getKeyspace())
                                                .value("table_name", repairConfig.getName())
                                                .value("parallelism", repairConfig.getRepairOptions().getParallelism().toString())
                                                .value("type", repairConfig.getRepairOptions().getType().toString())
                                                .value("intertask_delay_minutes", repairConfig.getInterTaskDelayMinutes())
                                                .value("workers", repairConfig.getRepairOptions().getNumWorkers())
                                                .value("split_strategy", repairConfig.getRepairOptions().getSplitStrategy().toString())
                                                .value("hooks", repairConfig.getPostTaskHooks());

            daoUtil.execUpsertStmtRepairDb(insertQuery);
        }
        catch (Exception e)
        {
            logger.error("Exception in marking cluster post repair hook status started", e);
            return false;
        }
        return true;
    }

    /**
     * Gets all repair enabled tables keyed by keyspace.table
     */
    @Override
    public List<TableTaskConfig> getAllTaskEnabledTables(String scheduleName)
    {
        // Get all tables by connecting local C* node and overlay that information with config information from
        // repair_config, using defaults for any tables not found in repair_config.
        Map<String, TableTaskConfig> returnMap = new HashMap<>();

        context.localSession().getCluster().getMetadata()
               .getKeyspaces()
               .forEach(keyspaceMetadata -> keyspaceMetadata.getTables().forEach(tableMetadata -> {
                   if (!isRepairableKeyspace(tableMetadata.getKeyspace().getName()))
                   {
                       String ksTbName = tableMetadata.getKeyspace().getName() + "." + tableMetadata.getName();
                       TableTaskConfig tableConfig = new TableTaskConfig(context.getConfig(), scheduleName);

                       // Repairing or compacting a TWCS or a DTCS is a bad idea, let's not do that.
                       if (!tableMetadata.getOptions().getCompaction()
                                         .get("class").matches(".*TimeWindow.*|.*DateTiered.*"))
                       {
                           tableConfig.setKeyspace(tableMetadata.getKeyspace().getName())
                                      .setName(tableMetadata.getName())
                                      .setTableMetadata(tableMetadata);

                           returnMap.put(ksTbName, tableConfig);
                       }
                   }
               }));

        // Apply any table specific overrides from the repair config
        List<TableTaskConfig> allConfigs = getTableTaskConfigs(scheduleName);
        for (TableTaskConfig tcDb : allConfigs)
        {
            TableTaskConfig tableConfig = returnMap.get(getKsTbName(tcDb.getKeyspace(), tcDb.getName()));

            if (null != tableConfig)
            {
                tableConfig.clone(tcDb);
            }
        }
        return returnMap.values().stream().filter(TableTaskConfig::isRepairEnabled).collect(Collectors.toList());
    }

    /**
     * Checks whether a given keyspace is system related keyspace or not, scope of this function is to be used
     * explicitly for repair. This method considers `system_auth` and `system_distributed` as repairable
     * keyspaces in the explicit context of repair as these 2 keyspaces demand consistency with Network Topology
     * replications in production setups.
     *
     * @param name Name of the keyspace
     * @return boolean which indicates the keyspace is system/ repair-able
     */
    private boolean isRepairableKeyspace(String name)
    {
        return (
        name.equalsIgnoreCase("system") ||
        name.equalsIgnoreCase("system_traces") ||
        name.equalsIgnoreCase("dse_system") ||
        name.equalsIgnoreCase("system_schema")
        );
    }

    private boolean hasColumn(Row row, String columnName)
    {
        return row.getColumnDefinitions().contains(columnName) && !row.isNull(columnName);
    }

    private TableTaskConfig getTableTaskConfig(Row row)
    {
        TableTaskConfig tableTaskConfig = new TableTaskConfig(context.getConfig(), row.getString("schedule_name"))
                                              .setKeyspace(row.getString("keyspace_name"))
                                              .setName(row.getString("table_name"));


        if (hasColumn(row, "intertask_delay_minutes"))
            tableTaskConfig.setInterTaskDelayMinutes(row.getInt("intertask_delay_minutes"));

        if (hasColumn(row, "task_timeout_seconds"))
            tableTaskConfig.setTaskTimeoutSeconds(row.getInt("task_timeout_seconds"));

        if (hasColumn(row, "hooks"))
            tableTaskConfig.setPostTaskHooks(row.getList("hooks", String.class));

        if (hasColumn(row, "task_config"))
            tableTaskConfig.setTaskConfig(row.getMap("task_config", String.class, String.class));

        return tableTaskConfig;
    }

    private String getClusterName()
    {
        if (clusterName == null)
        {
            clusterName = context.getCassInteraction().getClusterName();
        }
        return clusterName;
    }
}
