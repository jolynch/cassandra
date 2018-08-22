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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskProcessDao;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.joda.time.DateTime;

public class TaskProcessDaoImpl implements ITaskProcessDao
{
    private static final Logger logger = LoggerFactory.getLogger(TaskProcessDaoImpl.class);

    private final TaskSchedulerConfig config;
    private final CassandraInteraction cassInteraction;
    private final CassDaoUtil daoUtil;

    public TaskProcessDaoImpl(TaskSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.config = context.getConfig();
        cassInteraction = context.getCassInteraction();
    }

    @Override
    public Optional<ClusterTaskStatus> getClusterRepairStatus()
    {
        Optional<ClusterTaskStatus> clusterRepairStatus = Optional.empty();
        try
        {
            Statement selectQuery = QueryBuilder.select()
                                                .from(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                .limit(1); // We need only first and latest row from clustering order.

            Row row = daoUtil.execSelectStmtRepairDb(selectQuery).one();
            if (row != null)
            {
                ClusterTaskStatus status = new ClusterTaskStatus().setTaskStatus(row.getString("status"))
                                                                  .setTaskId(row.getInt("repair_id"))
                                                                  .setStartTime(row.getTimestamp("start_time"))
                                                                  .setEndTime(row.getTimestamp("end_time"))
                                                                  .setPauseTime(row.getTimestamp("pause_time"));
                clusterRepairStatus = Optional.of(status);
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in getting repair status from repair_process table", e);
            throw e;
        }
        return clusterRepairStatus;
    }

    @Override
    public Optional<ClusterTaskStatus> getClusterRepairStatus(int taskId)
    {
        Optional<ClusterTaskStatus> clusterRepairStatus = Optional.empty();
        try
        {
            Statement selectQuery = QueryBuilder.select()
                                                .from(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("repair_id", taskId));

            Row row = daoUtil.execSelectStmtRepairDb(selectQuery).one();
            if (row != null)
            {
                ClusterTaskStatus status = new ClusterTaskStatus().setRepairStatus(row.getString("status"))
                                                                  .setRepairId(row.getInt("repair_id"))
                                                                  .setStartTime(row.getTimestamp("start_time"))
                                                                  .setEndTime(row.getTimestamp("end_time"))
                                                                  .setPauseTime(row.getTimestamp("pause_time"));
                clusterRepairStatus = Optional.of(status);
            }
        }
        catch (Exception e)
        {
            logger.error("Exception in getting repair status from repair_process table", e);
            throw e;
        }
        return clusterRepairStatus;
    }

    @Override
    public boolean acquireRepairInitLock(int repairId)
    {
        Statement insertQuery = QueryBuilder.insertInto(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                            .value("cluster_name", cassInteraction.getClusterName())
                                            .value("repair_id", repairId)
                                            .value("status", TaskStatus.STARTED.toString())
                                            .value("start_time", DateTime.now().toDate())
                                            .value("created_node_id", cassInteraction.getLocalHostId())
                                            .ifNotExists();

        ResultSet results = daoUtil.execSerialUpsertStmtRepairDb(insertQuery);
        return results.wasApplied();
    }

    @Override
    public boolean markClusterRepairFinished(int repairId)
    {
        logger.info("Marking Cluster Repair Completed on repair Id: {} ", repairId);

        try
        {
            Statement updateQuery = QueryBuilder.update(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                                .with(QueryBuilder.set("end_time", DateTime.now().toDate()))
                                                .and(QueryBuilder.set("status", TaskStatus.FINISHED.toString()))
                                                .and(QueryBuilder.put("last_event", "Completed By", cassInteraction.getLocalHostId()))
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("repair_id", repairId));

            daoUtil.execUpsertStmtRepairDb(updateQuery);
        }
        catch (Exception e)
        {
            logger.error("Exception in marking cluster repair status completed", e);
            return false;
        }

        return true;
    }

    @Override
    public boolean deleteClusterRepairStatus(int repairId)
    {
        logger.info("Deleting Cluster Repair status for repair Id: {} ", repairId);

        try
        {
            Statement deleteQuery = QueryBuilder.delete().from(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("repair_id", repairId));

            daoUtil.execUpsertStmtRepairDb(deleteQuery);
        }
        catch (Exception e)
        {
            logger.error("Exception in deleting cluster repair status", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean updateClusterRepairStatus(ClusterTaskStatus clusterTaskStatus)
    {
        logger.info("Updating Cluster Repair status on repair Id: {} ", clusterTaskStatus.getTaskId());

        try
        {
            Statement updateQuery = QueryBuilder.update(config.getRepairKeyspace(), config.getRepairProcessTableName())
                                                .with(QueryBuilder.set("end_time", clusterTaskStatus.getEndTime()))
                                                .and(QueryBuilder.set("status", clusterTaskStatus.getTaskStatus().toString()))
                                                .and(QueryBuilder.set("pause_time", clusterTaskStatus.getPauseTime()))
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("repair_id", clusterTaskStatus.getTaskId()));
            daoUtil.execUpsertStmtRepairDb(updateQuery);
        }
        catch (Exception e)
        {
            logger.error("Exception in updating cluster repair status", e);
            return false;
        }

        return true;
    }
}
