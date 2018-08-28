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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskHookDao;
import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.joda.time.DateTime;

public class TaskHookDaoImpl implements ITaskHookDao
{
    private static final Logger logger = LoggerFactory.getLogger(TaskHookDaoImpl.class);

    private final TaskSchedulerConfig config;
    private final CassandraInteraction cassInteraction;
    private final CassDaoUtil daoUtil;

    public TaskHookDaoImpl(TaskSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.config = context.getConfig();
        this.cassInteraction = context.getCassInteraction();
    }

    @Override
    public boolean markLocalPostRepairHookStarted(int taskId)
    {
        logger.info("Marking Cluster Post Repair Hook Started on repair Id: {} ", taskId);
        try
        {
            Statement insertQuery = QueryBuilder.insertInto(config.getTaskKeyspace(), config.getTableHookSequenceTableName())
                                                .value("cluster_name", this.cassInteraction.getClusterName())
                                                .value("task_id", taskId)
                                                .value("node_id", this.cassInteraction.getLocalHostId())
                                                .value("status", TaskStatus.STARTED.toString())
                                                .value("start_time", DateTime.now().toDate())
                                                .value("created_node_id", this.cassInteraction.getLocalHostId());

            ResultSet results = daoUtil.execUpsertStmtRepairDb(insertQuery);
            return results.wasApplied();
        }
        catch (Exception e)
        {
            logger.error("Exception in marking cluster post repair hook status started", e);
            return false;
        }
    }

    @Override
    public boolean markLocalPostRepairHookEnd(int taskId, TaskStatus status, Map<String, Boolean> hookSuccess)
    {
        logger.info("Marking Cluster Post Repair Hook {} for repair Id: {} ", status, taskId);

        Map<String, String> hookMetadata = new HashMap<>();
        hookSuccess.forEach((k, v) -> hookMetadata.put(k, v ? "success" : "fail"));
        hookMetadata.put("Completed By", this.cassInteraction.getLocalHostId());

        try
        {
            Statement updateQuery = QueryBuilder.update(config.getTaskKeyspace(), config.getTableHookSequenceTableName())
                                                .with(QueryBuilder.set("end_time", DateTime.now().toDate()))
                                                .and(QueryBuilder.set("status", status.toString()))
                                                .and(QueryBuilder.set("last_event", hookMetadata))
                                                .where(QueryBuilder.eq("cluster_name", this.cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("task_id", taskId))
                                                .and(QueryBuilder.eq("node_id", this.cassInteraction.getLocalHostId()));


            daoUtil.execUpsertStmtRepairDb(updateQuery);
        }
        catch (Exception e)
        {
            logger.error("Exception in marking cluster post task hook status completed", e);
            return false;
        }

        return true;
    }

    @Override
    public List<TaskMetadata> getLocalTaskHookStatus(int taskId)
    {
        List<TaskMetadata> returnLst = new LinkedList<>();
        try
        {
            Statement selectQuery = QueryBuilder.select()
                                                .from(config.getTaskKeyspace(), config.getTableHookSequenceTableName())

                                                .where(QueryBuilder.eq("cluster_name", this.cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("task_id", taskId));

            ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

            for (Row r : results.all())
            {
                TaskMetadata taskMetadata = new TaskMetadata();
                taskMetadata.setStatus(r.getString("status"))
                            .setNodeId(r.getString("node_id"))
                            .setTaskId(r.getInt("task_id"))
                            .setStartTime(r.getTimestamp("start_time"))
                            .setEndTime(r.getTimestamp("end_time"))
                            .setPauseTime(r.getTimestamp("pause_time"));

                returnLst.add(taskMetadata);
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in getting task status from task_hook_sequence table", e);
        }
        return returnLst;
    }

    @Override
    public TaskMetadata getLocalTaskHookStatus(int taskId, String nodeId)
    {
        TaskMetadata taskMetadata = new TaskMetadata();
        try
        {
            Statement selectQuery = QueryBuilder.select()
                                                .from(config.getTaskKeyspace(), config.getTableHookSequenceTableName())
                                                .where(QueryBuilder.eq("cluster_name", this.cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("task_id", taskId))
                                                .and(QueryBuilder.eq("node_id", nodeId));

            ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

            for (Row r : results.all())
            {
                taskMetadata.setStatus(r.getString("status"))
                            .setTaskId(r.getInt("task_id"))
                            .setStartTime(r.getTimestamp("start_time"))
                            .setEndTime(r.getTimestamp("end_time"))
                            .setPauseTime(r.getTimestamp("pause_time"));
                return taskMetadata;
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in getting task status from task_hook_sequence table", e);
        }
        return taskMetadata;
    }
}
