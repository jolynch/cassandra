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
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.scheduler.RepairUtil;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairStatusDao;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.joda.time.DateTime;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class RepairStatusDaoImpl implements IRepairStatusDao
{
    private static final Logger logger = LoggerFactory.getLogger(RepairStatusDaoImpl.class);
    private final CassDaoUtil daoUtil;
    private final RepairSchedulerConfig config;
    private final CassandraInteraction cassInteraction;

    public RepairStatusDaoImpl(RepairSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.config = context.getConfig();
        cassInteraction = context.getCassInteraction();
    }

    private List<RepairMetadata> getCurrentlyRunningRepairs(int repairId, String nodeId)
    {
        List<RepairMetadata> currRunRepair = new LinkedList<>();
        List<RepairMetadata> repairHistory = getRepairHistory(repairId, nodeId);

        for (RepairMetadata runningRepair : repairHistory)
        {
            if (runningRepair.getStatus().isStarted())
            {
                currRunRepair.add(runningRepair);
            }
        }
        return currRunRepair;
    }

    @Override
    public boolean markRepairStatusChange(RepairMetadata repairMetadata)
    {
        assert repairMetadata.repairId > 0;
        logger.info("Marking Repair {} on :: {}", repairMetadata.getStatus().toString(), repairMetadata.toString());

        try
        {
            Statement statement = QueryBuilder.update(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                              .with(QueryBuilder.set("status", repairMetadata.getStatus().toString()))
                                              .and(QueryBuilder.set("end_time", repairMetadata.getEndTime()))
                                              .and(QueryBuilder.set("last_event", repairMetadata.getLastEvent()))
                                              .and(QueryBuilder.set("create_time", repairMetadata.getCreatedTime()))
                                              .and(QueryBuilder.set("start_time", repairMetadata.getStartTime()))
                                              .and(QueryBuilder.set("config", repairMetadata.getRepairConfig().toMap()))
                                              .where(eq("cluster_name", repairMetadata.getClusterName()))
                                              .and(eq("repair_id", repairMetadata.getRepairId()))
                                              .and(eq("node_id", repairMetadata.getNodeId()))
                                              .and(eq("keyspace_name", repairMetadata.getKeyspaceName()))
                                              .and(eq("table_name", repairMetadata.getTableName()))
                                              .and(eq("repair_cmd", repairMetadata.getRepairNum()))
                                              .and(eq("start_token", repairMetadata.getStartToken()))
                                              .and(eq("end_token", repairMetadata.getEndToken()));

            daoUtil.execUpsertStmtRepairDb(statement);
        }
        catch (Exception e)
        {
            logger.error("Exception in updating repair status", e);
            return false;
        }
        return true;
    }

    @Override
    public List<RepairMetadata> getRepairHistory(int repairId, String keyspace, String table, String nodeId)
    {
        List<RepairMetadata> res = new LinkedList<>();
        try
        {

            Statement selectQuery;
            if (repairId < 0)
            {
                selectQuery = QueryBuilder.select().from(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                          .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                          .and(QueryBuilder.eq("node_id", nodeId))
                                          .and(QueryBuilder.eq("keyspace_name", keyspace))
                                          .and(QueryBuilder.eq("table_name", table));
            }
            else
            {
                selectQuery = QueryBuilder.select().from(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                          .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                          .and(QueryBuilder.eq("node_id", nodeId))
                                          .and(QueryBuilder.eq("keyspace_name", keyspace))
                                          .and(QueryBuilder.eq("table_name", table))
                                          .and(QueryBuilder.eq("repair_id", repairId));
            }

            ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

            for (Row row : results.all())
            {

                RepairMetadata repairMetadata = new RepairMetadata();
                repairMetadata.setClusterName(row.getString("cluster_name"))
                              .setNodeId(row.getString("node_id"))
                              .setKeyspaceName(row.getString("keyspace_name"))
                              .setTableName(row.getString("table_name"))
                              .setStartToken(row.getString("start_token"))
                              .setEndToken(row.getString("end_token"))
                              .setStartTime(row.getTimestamp("start_time"))
                              .setEndTime(row.getTimestamp("end_time"))
                              .setPauseTime(row.getTimestamp("pause_time"))
                              .setStatus(row.getString("status"))
                              .setRepairId(row.getInt("repair_id"))
                              .setRepairNum(row.getInt("repair_cmd"))
                              .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

                res.add(repairMetadata);
            }
        }
        catch (Exception e)
        {
            logger.error(String.format("Exception in getting repair status from repair_status table for node_id: %s, tableName: %s",
                                       cassInteraction.getLocalHostId(), RepairUtil.getKsTbName(keyspace, table)), e);
        }
        return res;
    }

    @Override
    public List<RepairMetadata> getRepairHistory(int repairId, String nodeId)
    {
        List<RepairMetadata> res = new LinkedList<>();
        Statement selectQuery = QueryBuilder.select().from(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                            .and(QueryBuilder.eq("repair_id", repairId))
                                            .and(QueryBuilder.eq("node_id", nodeId));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

        List<Row> allRows = results.all();

        for (Row row : allRows)
        {
            RepairMetadata repairMetadata = new RepairMetadata();
            repairMetadata.setClusterName(row.getString("cluster_name"))
                          .setNodeId(row.getString("node_id"))
                          .setKeyspaceName(row.getString("keyspace_name"))
                          .setTableName(row.getString("table_name"))
                          .setStartToken(row.getString("start_token"))
                          .setEndToken(row.getString("end_token"))
                          .setCreatedTime(row.getTimestamp("create_time"))
                          .setStartTime(row.getTimestamp("start_time"))
                          .setEndTime(row.getTimestamp("end_time"))
                          .setPauseTime(row.getTimestamp("pause_time"))
                          .setStatus(row.getString("status"))
                          .setRepairId(row.getInt("repair_id"))
                          .setRepairNum(row.getInt("repair_cmd"))
                          .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

            res.add(repairMetadata);
        }
        return res;
    }

    @Override
    public List<RepairMetadata> getRepairHistory(int repairId)
    {
        List<RepairMetadata> res = new LinkedList<>();

        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                            .and(QueryBuilder.eq("repair_id", repairId));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);
        List<Row> allRows = results.all();
        for (Row row : allRows)
        {
            RepairMetadata repairMetadata = new RepairMetadata();
            repairMetadata.setClusterName(row.getString("cluster_name"))
                          .setNodeId(row.getString("node_id"))
                          .setKeyspaceName(row.getString("keyspace_name"))
                          .setTableName(row.getString("table_name"))
                          .setStartToken(row.getString("start_token"))
                          .setEndToken(row.getString("end_token"))
                          .setCreatedTime(row.getTimestamp("create_time"))
                          .setStartTime(row.getTimestamp("start_time"))
                          .setEndTime(row.getTimestamp("end_time"))
                          .setPauseTime(row.getTimestamp("pause_time"))
                          .setStatus(row.getString("status"))
                          .setRepairId(row.getInt("repair_id"))
                          .setRepairNum(row.getInt("repair_cmd"))
                          .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

            res.add(repairMetadata);
        }
        return res;
    }

    @Override
    public List<RepairMetadata> getRepairHistory()
    {
        List<RepairMetadata> res = new LinkedList<>();

        //Get RepairIds
        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);
        List<Integer> allRepairIds = new ArrayList<>();
        for (Row row : results.all())
        {
            allRepairIds.add(row.getInt("repair_id"));
        }

        //GetRepairHistory
        selectQuery = QueryBuilder.select()
                                  .from(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                  .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                  .and(QueryBuilder.in("repair_id", allRepairIds));

        results = daoUtil.execSelectStmtRepairDb(selectQuery);
        for (Row row : results.all())
        {
            RepairMetadata repairMetadata = new RepairMetadata();
            repairMetadata.setClusterName(row.getString("cluster_name"))
                          .setNodeId(row.getString("node_id"))
                          .setKeyspaceName(row.getString("keyspace_name"))
                          .setTableName(row.getString("table_name"))
                          .setStartToken(row.getString("start_token"))
                          .setEndToken(row.getString("end_token"))
                          .setCreatedTime(row.getTimestamp("create_time"))
                          .setStartTime(row.getTimestamp("start_time"))
                          .setEndTime(row.getTimestamp("end_time"))
                          .setPauseTime(row.getTimestamp("pause_time"))
                          .setStatus(row.getString("status"))
                          .setRepairId(row.getInt("repair_id"))
                          .setRepairNum(row.getInt("repair_cmd"))
                          .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

            res.add(repairMetadata);
        }
        return res;
    }

    @Override
    public boolean markRepairCancelled(int repairId, String nodeId)
    {
        logger.info("Marking Repair Cancelled for repairId: {} on all tables", repairId);
        try
        {
            BatchStatement batch = new BatchStatement();
            List<RepairMetadata> currentlyRunningRepairs = getCurrentlyRunningRepairs(repairId, nodeId);
            for (RepairMetadata runningRepair : currentlyRunningRepairs)
            {
                runningRepair.setLastEvent("Cancellation Reason", "Long pause");
                Statement statement = QueryBuilder.update(config.getRepairKeyspace(), config.getRepairStatusTableName())
                                                  .with(QueryBuilder.set("pause_time", DateTime.now().toDate()))
                                                  .and(QueryBuilder.set("status", RepairStatus.CANCELLED.toString()))
                                                  .and(QueryBuilder.set("last_event", runningRepair.getLastEvent()))
                                                  .where(eq("cluster_name", cassInteraction.getClusterName()))
                                                  .and(eq("node_id", nodeId)).and(eq("repair_id", repairId))
                                                  .and(eq("keyspace_name", runningRepair.getKeyspaceName()))
                                                  .and(eq("table_name", runningRepair.getTableName()))
                                                  .and(eq("repair_cmd", runningRepair.getRepairNum()))
                                                  .and(eq("start_token", runningRepair.getStartToken()))
                                                  .and(eq("end_token", runningRepair.getEndToken()))
                                                  .onlyIf(QueryBuilder.eq("status", RepairStatus.STARTED.toString()));
                batch.add(statement);
            }

            ResultSet res = daoUtil.execUpsertStmtRepairDb(batch);
            return res.wasApplied();
        }
        catch (Exception e)
        {
            logger.error("Exception in updating repair status", e);
        }
        return false;
    }
}
