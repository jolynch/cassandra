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
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.entity.RepairOptions;
import org.apache.cassandra.repair.scheduler.entity.RepairType;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;

import static org.apache.cassandra.repair.scheduler.RepairUtil.getKsTbName;

public class RepairConfigDaoImpl implements IRepairConfigDao
{
    private static final Logger logger = LoggerFactory.getLogger(RepairHookDaoImpl.class);

    private final TaskSchedulerContext context;
    private static String clusterName;
    private final CassDaoUtil daoUtil;

    public RepairConfigDaoImpl(TaskSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.context = context;
    }

    /**
     * Get table repair configurations/ overrides for all schedules
     *
     * @return Map of TableRepairConfigs list keyed by schedule name
     */
    @Override
    public Map<String, List<TableTaskConfig>> getRepairConfigs()
    {
        Map<String, List<TableTaskConfig>> repairConfigListMap = new HashMap<>();
        Statement selectQuery = QueryBuilder.select()
                                            .from(context.getConfig().getRepairKeyspace(),
                                                  context.getConfig().getRepairConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", getClusterName()));

        daoUtil.execSelectStmtRepairDb(selectQuery)
               .forEach(row ->
                        {
                            TableTaskConfig tableTaskConfig = getTableRepairConfig(row);
                            String schedule = row.getString("schedule_name");
                            repairConfigListMap.computeIfAbsent(schedule, k -> new ArrayList<>());
                            repairConfigListMap.get(schedule).add(tableTaskConfig);
                        });

        return repairConfigListMap;
    }

    @Override
    public List<TableTaskConfig> getRepairConfigs(String scheduleName)
    {
        List<TableTaskConfig> lstTableTaskConfig = new ArrayList<>();
        Statement selectQuery = QueryBuilder.select()
                                            .from(context.getConfig().getRepairKeyspace(),
                                                  context.getConfig().getRepairConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", getClusterName()))
                                            .and(QueryBuilder.eq("schedule_name", scheduleName));

        daoUtil.execSelectStmtRepairDb(selectQuery).forEach(row -> {
            TableTaskConfig TableTaskConfig = getTableRepairConfig(row);
            lstTableTaskConfig.add(TableTaskConfig);
        });

        return lstTableTaskConfig;
    }

    @Override
    public Set<String> getAllRepairSchedules()
    {
        Set<String> schedules = new HashSet<>();
        Statement selectQuery = QueryBuilder.select("schedule_name")
                                            .from(context.getConfig().getRepairKeyspace(),
                                                  context.getConfig().getRepairConfigTableName())
                                            .where(QueryBuilder.eq("cluster_name", getClusterName()));

        daoUtil.execSelectStmtRepairDb(selectQuery)
               .forEach(row -> schedules.add(row.getString("schedule_name")));

        //Get default schedules from config
        schedules.addAll(context.getConfig().getDefaultSchedules());
        return schedules;
    }

    @Override
    public boolean saveRepairConfig(String schedule, TableTaskConfig repairConfig)
    {
        logger.info("Saving Repair Configuration for {}.{}", getClusterName(), schedule);
        try
        {
            Statement insertQuery = QueryBuilder.insertInto(context.getConfig().getRepairKeyspace(),
                                                            context.getConfig().getRepairConfigTableName())
                                                .value("cluster_name", getClusterName())
                                                .value("schedule_name", schedule)
                                                .value("keyspace_name", repairConfig.getKeyspace())
                                                .value("table_name", repairConfig.getName())
                                                .value("parallelism", repairConfig.getRepairOptions().getParallelism().toString())
                                                .value("type", repairConfig.getRepairOptions().getType().toString())
                                                .value("intertask_delay_minutes", repairConfig.getInterRepairDelayMinutes())
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
    public List<TableTaskConfig> getAllRepairEnabledTables(String scheduleName)
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
        List<TableTaskConfig> allConfigs = getRepairConfigs(scheduleName);
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

    private TableTaskConfig getTableRepairConfig(Row row)
    {
        TableTaskConfig tableTaskConfig = new TableTaskConfig(context.getConfig(), row.getString("schedule_name"))
                                              .setKeyspace(row.getString("keyspace_name"))
                                              .setName(row.getString("table_name"));

        RepairOptions repairOptions = tableTaskConfig.getRepairOptions();

        repairOptions.setType(RepairType.fromString(row.getString("type")));

        if (hasColumn(row, "intertask_delay_minutes"))
            tableTaskConfig.setInterRepairDelayMinutes(row.getInt("intertask_delay_minutes"));

        if (hasColumn(row, "hooks"))
            tableTaskConfig.setPostTaskHooks(row.getList("hooks", String.class));

        if (hasColumn(row, "parallelism"))
            repairOptions.setParallelism(RepairParallelism.fromName(row.getString("parallelism")));

        if (hasColumn(row, "workers"))
            repairOptions.setNumWorkers(row.getInt("workers"));

        if (hasColumn(row, "split_strategy"))
            repairOptions.setSplitStrategy(row.getString("split_strategy"));

        // TODO: do we actually need to set this to 1?
        // In testing with concurrent sessions incremental would just get stuck a lot...
        if (repairOptions.getType() == RepairType.INCREMENTAL)
            repairOptions.setNumWorkers(1);

        if (hasColumn(row, "repair_timeout_seconds"))
            repairOptions.setSecondsToWait(row.getInt("repair_timeout_seconds"));

        tableTaskConfig.setRepairOptions(repairOptions);
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
