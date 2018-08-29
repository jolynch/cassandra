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
import org.apache.cassandra.repair.scheduler.TaskUtil;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskTableStatusDao;
import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.joda.time.DateTime;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class TaskTableStatusDao implements ITaskTableStatusDao
{
    private static final Logger logger = LoggerFactory.getLogger(TaskTableStatusDao.class);
    private final CassDaoUtil daoUtil;
    private final TaskSchedulerConfig config;
    private final CassandraInteraction cassInteraction;

    public TaskTableStatusDao(TaskSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.config = context.getConfig();
        cassInteraction = context.getCassInteraction();
    }

    private List<TaskMetadata> getCurrentlyRunningTasks(int taskId, String nodeId)
    {
        List<TaskMetadata> currRunRepair = new LinkedList<>();
        List<TaskMetadata> repairHistory = getTaskHistory(taskId, nodeId);

        for (TaskMetadata runningRepair : repairHistory)
        {
            if (runningRepair.getStatus().isStarted())
            {
                currRunRepair.add(runningRepair);
            }
        }
        return currRunRepair;
    }

    @Override
    public boolean markTaskStatusChange(TaskMetadata taskMetadata)
    {
        assert taskMetadata.taskId > 0;
        logger.info("Marking Repair {} on :: {}", taskMetadata.getStatus().toString(), taskMetadata.toString());

        try
        {
            Statement statement = QueryBuilder.update(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                              .with(QueryBuilder.set("status", taskMetadata.getStatus().toString()))
                                              .and(QueryBuilder.set("end_time", taskMetadata.getEndTime()))
                                              .and(QueryBuilder.set("last_event", taskMetadata.getLastEvent()))
                                              .and(QueryBuilder.set("create_time", taskMetadata.getCreatedTime()))
                                              .and(QueryBuilder.set("start_time", taskMetadata.getStartTime()))
                                              .and(QueryBuilder.set("config", taskMetadata.getRepairConfig().toMap()))
                                              .where(eq("cluster_name", taskMetadata.getClusterName()))
                                              .and(eq("repair_id", taskMetadata.getTaskId()))
                                              .and(eq("node_id", taskMetadata.getNodeId()))
                                              .and(eq("keyspace_name", taskMetadata.getKeyspaceName()))
                                              .and(eq("table_name", taskMetadata.getTableName()))
                                              .and(eq("repair_cmd", taskMetadata.getRepairNum()))
                                              .and(eq("start_token", taskMetadata.getStartToken()))
                                              .and(eq("end_token", taskMetadata.getEndToken()));

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
    public List<TaskMetadata> getTaskHistory(int taskId, String keyspace, String table, String nodeId)
    {
        List<TaskMetadata> res = new LinkedList<>();
        try
        {

            Statement selectQuery;
            if (taskId < 0)
            {
                selectQuery = QueryBuilder.select().from(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                          .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                          .and(QueryBuilder.eq("node_id", nodeId))
                                          .and(QueryBuilder.eq("keyspace_name", keyspace))
                                          .and(QueryBuilder.eq("table_name", table));
            }
            else
            {
                selectQuery = QueryBuilder.select().from(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                          .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                          .and(QueryBuilder.eq("node_id", nodeId))
                                          .and(QueryBuilder.eq("keyspace_name", keyspace))
                                          .and(QueryBuilder.eq("table_name", table))
                                          .and(QueryBuilder.eq("repair_id", taskId));
            }

            ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

            for (Row row : results.all())
            {

                TaskMetadata taskMetadata = new TaskMetadata();
                taskMetadata.setClusterName(row.getString("cluster_name"))
                            .setNodeId(row.getString("node_id"))
                            .setKeyspaceName(row.getString("keyspace_name"))
                            .setTableName(row.getString("table_name"))
                            .setStartToken(row.getString("start_token"))
                            .setEndToken(row.getString("end_token"))
                            .setStartTime(row.getTimestamp("start_time"))
                            .setEndTime(row.getTimestamp("end_time"))
                            .setPauseTime(row.getTimestamp("pause_time"))
                            .setStatus(row.getString("status"))
                            .setTaskId(row.getInt("repair_id"))
                            .setRepairNum(row.getInt("repair_cmd"))
                            .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

                res.add(taskMetadata);
            }
        }
        catch (Exception e)
        {
            logger.error(String.format("Exception in getting repair status from repair_status table for node_id: %s, tableName: %s",
                                       cassInteraction.getLocalHostId(), TaskUtil.getKsTbName(keyspace, table)), e);
        }
        return res;
    }

    @Override
    public List<TaskMetadata> getTaskHistory(int taskId, String nodeId)
    {
        List<TaskMetadata> res = new LinkedList<>();
        Statement selectQuery = QueryBuilder.select().from(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                            .and(QueryBuilder.eq("repair_id", taskId))
                                            .and(QueryBuilder.eq("node_id", nodeId));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

        List<Row> allRows = results.all();

        for (Row row : allRows)
        {
            TaskMetadata taskMetadata = new TaskMetadata();
            taskMetadata.setClusterName(row.getString("cluster_name"))
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
                        .setTaskId(row.getInt("repair_id"))
                        .setRepairNum(row.getInt("repair_cmd"))
                        .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

            res.add(taskMetadata);
        }
        return res;
    }

    @Override
    public List<TaskMetadata> getTaskHistory(int taskId)
    {
        List<TaskMetadata> res = new LinkedList<>();

        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                            .and(QueryBuilder.eq("repair_id", taskId));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);
        List<Row> allRows = results.all();
        for (Row row : allRows)
        {
            TaskMetadata taskMetadata = new TaskMetadata();
            taskMetadata.setClusterName(row.getString("cluster_name"))
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
                        .setTaskId(row.getInt("task_id"))
                        .setRepairNum(row.getInt("repair_cmd"))
                        .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

            res.add(taskMetadata);
        }
        return res;
    }

    @Override
    public List<TaskMetadata> getTaskHistory()
    {
        List<TaskMetadata> res = new LinkedList<>();

        //Get RepairIds
        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getTaskKeyspace(), config.getTaskProcessTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);
        List<Integer> allRepairIds = new ArrayList<>();
        for (Row row : results.all())
        {
            allRepairIds.add(row.getInt("task_id"));
        }

        //GetRepairHistory
        selectQuery = QueryBuilder.select()
                                  .from(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                  .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                  .and(QueryBuilder.in("task_id", allRepairIds));

        results = daoUtil.execSelectStmtRepairDb(selectQuery);
        for (Row row : results.all())
        {
            TaskMetadata taskMetadata = new TaskMetadata();
            taskMetadata.setClusterName(row.getString("cluster_name"))
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
                        .setTaskId(row.getInt("task_id"))
                        .setRepairNum(row.getInt("repair_cmd"))
                        .setEntireLastEvent(row.getMap("last_event", String.class, String.class));

            res.add(taskMetadata);
        }
        return res;
    }

    @Override
    public boolean markTaskCancelled(int taskId, String nodeId)
    {
        logger.info("Marking Repair Cancelled for taskId: {} on all tables", taskId);
        try
        {
            BatchStatement batch = new BatchStatement();
            List<TaskMetadata> currentlyRunningRepairs = getCurrentlyRunningTasks(taskId, nodeId);
            for (TaskMetadata runningRepair : currentlyRunningRepairs)
            {
                runningRepair.setLastEvent("Cancellation Reason", "Long pause");
                Statement statement = QueryBuilder.update(config.getTaskKeyspace(), config.getTaskTableStatusTableName())
                                                  .with(QueryBuilder.set("pause_time", DateTime.now().toDate()))
                                                  .and(QueryBuilder.set("status", TaskStatus.CANCELLED.toString()))
                                                  .and(QueryBuilder.set("last_event", runningRepair.getLastEvent()))
                                                  .where(eq("cluster_name", cassInteraction.getClusterName()))
                                                  .and(eq("node_id", nodeId)).and(eq("task_id", taskId))
                                                  .and(eq("keyspace_name", runningRepair.getKeyspaceName()))
                                                  .and(eq("table_name", runningRepair.getTableName()))
                                                  .and(eq("start_token", runningRepair.getStartToken()))
                                                  .and(eq("end_token", runningRepair.getEndToken()))
                                                  .and(eq("task_cmd", runningRepair.getRepairNum()))
                                                  .onlyIf(QueryBuilder.eq("status", TaskStatus.STARTED.toString()));
                batch.add(statement);
            }

            ResultSet res = daoUtil.execUpsertStmtRepairDb(batch);
            return res.wasApplied();
        }
        catch (Exception e)
        {
            logger.error("Exception in updating task status", e);
        }
        return false;
    }
}
