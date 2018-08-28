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

import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskSequenceDao;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.TaskSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.joda.time.DateTime;

public class TaskSequenceDaoImpl implements ITaskSequenceDao
{
    private static final Logger logger = LoggerFactory.getLogger(TaskSequenceDaoImpl.class);
    private final CassDaoUtil daoUtil;
    private final TaskSchedulerConfig config;
    private final CassandraInteraction cassInteraction;

    public TaskSequenceDaoImpl(TaskSchedulerContext context, CassDaoUtil daoUtil)
    {
        this.daoUtil = daoUtil;
        this.config = context.getConfig();
        cassInteraction = context.getCassInteraction();
    }

    @Override
    public boolean markTaskStartedOnInstance(int taskId, int seq)
    {
        logger.info("Marking Node Repair Started on repair Id: {}, seq: {} ", taskId, seq);
        try
        {
            Statement exampleQuery = QueryBuilder.update(config.getTaskKeyspace(), config.getTaskSequenceTableName())
                                                 .with(QueryBuilder.set("start_time", DateTime.now().toDate()))
                                                 .and(QueryBuilder.set("status", TaskStatus.STARTED.toString()))
                                                 .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                 .and(QueryBuilder.eq("repair_id", taskId))
                                                 .and(QueryBuilder.eq("seq", seq));
            daoUtil.execUpsertStmtRepairDb(exampleQuery);
            return true;
        }
        catch (Exception e)
        {
            logger.error("Exception in marking node repair status started", e);
        }
        return false;
    }

    public boolean markNodeDone(int taskId, int seq, TaskStatus status)
    {
        logger.info("Marking Node Repair Completed on taskId: {} ", taskId);
        try
        {
            Statement updateQuery = QueryBuilder.update(config.getTaskKeyspace(), config.getTaskSequenceTableName())
                                                .with(QueryBuilder.set("end_time", DateTime.now().toDate()))
                                                .and(QueryBuilder.set("status", status.toString()))
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                                .and(QueryBuilder.eq("repair_id", taskId))
                                                .and(QueryBuilder.eq("seq", seq));
            daoUtil.execUpsertStmtRepairDb(updateQuery);
            return true;
        }
        catch (Exception e)
        {
            logger.error("Exception in marking node repair status completed", e);
            return false;
        }
    }

    @Override
    public Optional<TaskSequence> getMyNodeStatus(int repairId)
    {
        Optional<TaskSequence> repairSequence = Optional.empty();

        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getTaskKeyspace(), config.getTaskSequenceTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                            .and(QueryBuilder.eq("repair_id", repairId));
        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);

        String localHostId = cassInteraction.getLocalHostId();
        for (Row r : results.all())
        {
            if (r.getString("node_id").equalsIgnoreCase(localHostId))
            {
                return Optional.of(repairSequenceFromRow(r));
            }
        }
        return repairSequence;
    }

    @Override
    public void persistEndpointSeqMap(int repairId, String scheduleName, List<RepairHost> allHosts)
    {
        logger.info("Persisting Endpoints Sequence Map for Repair Id: {}", repairId);
        try
        {
            Batch batch = QueryBuilder.batch();
            for (int i = 0; i < allHosts.size(); i++)
            {
                Insert statement = QueryBuilder.insertInto(config.getTaskKeyspace(), config.getTaskSequenceTableName())
                                               .value("cluster_name", cassInteraction.getClusterName())
                                               .value("repair_id", repairId)
                                               .value("seq", i + 1)
                                               .value("node_id", allHosts.get(i).getNodeId())
                                               .value("create_time", DateTime.now().toDate())
                                               .value("created_nodeid", cassInteraction.getLocalHostId())
                                               .value("schedule_name", scheduleName)
                                               .value("status", TaskStatus.NOT_STARTED.toString());

                batch.add(statement);
            }
            daoUtil.execUpsertStmtRepairDb(batch);
        }
        catch (Exception e)
        {
            logger.error("Exception in updating persistEndpointSeqMap", e);
        }
    }

    @Override
    public SortedSet<TaskSequence> getRepairSequence(int repairId)
    {
        TreeSet<TaskSequence> repairSeqLst = new TreeSet<>();
        Statement selectQuery = QueryBuilder.select()
                                            .from(config.getTaskKeyspace(), config.getTaskSequenceTableName())
                                            .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName()))
                                            .and(QueryBuilder.eq("repair_id", repairId));

        ResultSet results = daoUtil.execSelectStmtRepairDb(selectQuery);
        for (Row r : results.all())
        {
            repairSeqLst.add(repairSequenceFromRow(r));
        }
        return repairSeqLst;
    }

    @Override
    public boolean updateHeartBeat(int repairId, int seq)
    {
        try
        {
            Statement updateQuery = QueryBuilder.update(config.getTaskKeyspace(), config.getTaskSequenceTableName())

                                                .with(QueryBuilder.set("last_heartbeat", DateTime.now().toDate())).and(QueryBuilder.set("last_event", ImmutableMap.of("Status", "Ok")))

                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName())).and(QueryBuilder.eq("repair_id", repairId)).and(QueryBuilder.eq("seq", seq));

            daoUtil.execUpsertStmtRepairDb(updateQuery);
            return true;
        }
        catch (Exception e)
        {
            logger.error("Exception in updating heart beat for repair Id: {}, Seq: {}", repairId, seq, e);
            return false;
        }
    }

    @Override
    public boolean cancelRepairOnNode(int repairId, Integer seq)
    {
        logger.info("Cancelling the repair on node - taskId: {}, Seq: {}", repairId, seq);
        try
        {
            Statement updateQuery = QueryBuilder.update(config.getTaskKeyspace(), config.getTaskSequenceTableName())
                                                .with(QueryBuilder.set("pause_time", DateTime.now().toDate()))
                                                .and(QueryBuilder.set("status", TaskStatus.CANCELLED.toString()))
                                                .and(QueryBuilder.set("last_event", ImmutableMap.of("Reason", "Cancelled due to long pauses")))
                                                .where(QueryBuilder.eq("cluster_name", cassInteraction.getClusterName())).and(QueryBuilder.eq("repair_id", repairId)).and(QueryBuilder.eq("seq", seq));

            daoUtil.execUpsertStmtRepairDb(updateQuery);
            logger.info("Cancelled repair on node - taskId: {}, Seq: {}", repairId, seq);
            return true;
        }
        catch (Exception e)
        {
            logger.error("Exception in cancelling the repair on node- repair Id: {}, Seq: {}", repairId, seq, e);
            return false;
        }
    }


    private TaskSequence repairSequenceFromRow(Row r)
    {
        return new TaskSequence()
               .setClusterName(r.getString("cluster_name"))
               .setTaskId(r.getInt("repair_id"))
               .setSeq(r.getInt("seq"))
               .setNodeId(r.getString("node_id"))
               .setCreationTime(r.getTimestamp("create_time"))
               .setStartTime(r.getTimestamp("start_time"))
               .setEndTime(r.getTimestamp("end_time"))
               .setPauseTime(r.getTimestamp("pause_time"))
               .setStatus(r.getString("status"))
               .setLastHeartbeat(r.getTimestamp("last_heartbeat"))
               .setScheduleName(r.getString("schedule_name"))
               .setLastEvent(r.getMap("last_event", String.class, String.class));
    }
}
