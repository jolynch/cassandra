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
package org.apache.cassandra.repair.scheduler;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.dao.cass.CassDaoUtil;
import org.apache.cassandra.repair.scheduler.dao.cass.RepairConfigDaoImpl;
import org.apache.cassandra.repair.scheduler.dao.cass.TaskHookDaoImpl;
import org.apache.cassandra.repair.scheduler.dao.cass.TaskProcessDaoImpl;
import org.apache.cassandra.repair.scheduler.dao.cass.TaskSequenceDaoImpl;
import org.apache.cassandra.repair.scheduler.dao.cass.TaskTableStatusDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskHookDao;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskProcessDao;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskSequenceDao;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskTableStatusDao;

import static org.apache.cassandra.repair.scheduler.RepairUtil.initSession;

/**
 * TaskDaoManager, entry point for repair daos, this is the only entry point for talking to
 * repair metadata persistent store. This hides the repair persistent metadata store implementation
 * (be it C* or any other persistent store) details, rest of the repair scheduler is abstracted
 * away from the backend implementation. This makes repair metadata store to be pluggable/ swap-able.
 */
public class TaskDaoManager
{
    private static final Logger logger = LoggerFactory.getLogger(TaskDaoManager.class);

    private final ITaskProcessDao taskProcessDao;
    private final ITaskTableStatusDao repairStatusDao;
    private final ITaskSequenceDao repairSequenceDao;
    private final ITaskHookDao repairHookDao;
    private final IRepairConfigDao repairConfigDao;
    /**
     * Using repair session supplier, so that repair scheduler does not try to initiate session with
     * repair metadata persistent store on startup, rather it tries to establish the connection on first use
     */
    private final Supplier<Session> stateSupplier;
    private final TaskSchedulerContext context;
    /**
     * Repair Session object, initialized once on first usage and used it in subsequent calls
     */
    private Session repairSession;

    public TaskDaoManager(TaskSchedulerContext context)
    {
        this.context = context;
        stateSupplier = this::getOrInitRepairSession;
        final CassDaoUtil daoUtil = new CassDaoUtil(context.getConfig(), stateSupplier);

        this.taskProcessDao = new TaskProcessDaoImpl(context, daoUtil);
        this.repairStatusDao = new TaskTableStatusDao(context, daoUtil);
        this.repairSequenceDao = new TaskSequenceDaoImpl(context, daoUtil);
        this.repairHookDao = new TaskHookDaoImpl(context, daoUtil);
        this.repairConfigDao = new RepairConfigDaoImpl(context, daoUtil);
    }

    public ITaskProcessDao getTaskProcessDao()
    {
        return taskProcessDao;
    }

    public ITaskTableStatusDao getRepairStatusDao()
    {
        return repairStatusDao;
    }

    public ITaskSequenceDao getRepairSequenceDao()
    {
        return repairSequenceDao;
    }

    public ITaskHookDao getRepairHookDao()
    {
        return repairHookDao;
    }

    public IRepairConfigDao getRepairConfigDao()
    {
        return repairConfigDao;
    }

    /**
     * Gets or initiates C* session to repair persistent metadata store.
     *
     * @return C* Session
     */
    private Session getOrInitRepairSession()
    {
        if (repairSession == null)
        {
            repairSession = initSession(context.getConfig().getRepairStatePersistenceEndpoints(), false);
            logger.info("Initiated Repair state persistence session with C*");
        }
        return repairSession;
    }
}
