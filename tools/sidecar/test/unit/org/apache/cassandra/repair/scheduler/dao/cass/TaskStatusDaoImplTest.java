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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskSequenceDao;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskTableStatusDao;
import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;

public class TaskStatusDaoImplTest extends EmbeddedUnitTestBase
{
    private ITaskTableStatusDao repairStatusDao;
    private String hostId;
    private int repairId;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        repairStatusDao = new TaskTableStatusDao(context, getCassDaoUtil());
        ITaskSequenceDao repairSequenceDao = new TaskSequenceDaoImpl(context, getCassDaoUtil());
        hostId = context.getCassInteraction().getLocalHostId();
        repairId = getRandomRepairId();
    }

    @After
    public void cleanupMethod()
    {
        context.localSession().execute("TRUNCATE TABLE " + context.getConfig().getTaskKeyspace() + "." + context.getConfig().task_table_status_tablename + ";");
    }

    @Test
    public void markRepairStarted()
    {
        TaskMetadata taskMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markTaskStatusChange(taskMetadata);
        List<TaskMetadata> result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TaskStatus.STARTED, result.get(0).getStatus());
    }

    @Test
    public void markRepairPaused()
    {
        TaskMetadata taskMetadata = generateRepairMetadata(hostId).setStatus("PAUSED");
        repairStatusDao.markTaskStatusChange(taskMetadata);
        List<TaskMetadata> result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TaskStatus.PAUSED, result.get(0).getStatus());
    }

    @Test
    public void markRepairFailed()
    {
        TaskMetadata taskMetadata = generateRepairMetadata(hostId).setStatus("FAILED");

        repairStatusDao.markTaskStatusChange(taskMetadata);
        List<TaskMetadata> result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TaskStatus.FAILED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCompleted()
    {
        TaskMetadata taskMetadata = generateRepairMetadata(hostId).setStatus("FINISHED");

        repairStatusDao.markTaskStatusChange(taskMetadata);
        List<TaskMetadata> result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TaskStatus.FINISHED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCancelled_NOTSTARTED()
    {
        TaskMetadata taskMetadata = generateRepairMetadata(hostId);

        repairStatusDao.markTaskCancelled(taskMetadata.getTaskId(), hostId);
        List<TaskMetadata> result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void markRepairCancelled_STARTED()
    {
        TaskMetadata taskMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markTaskStatusChange(taskMetadata);

        List<TaskMetadata> result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TaskStatus.STARTED, result.get(0).getStatus());
        Assert.assertEquals(hostId, result.get(0).getNodeId());

        Assert.assertTrue("Failed to change the repair status to CANCELLED", repairStatusDao.markTaskCancelled(taskMetadata.getTaskId(), hostId));
        result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(TaskStatus.CANCELLED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCancelled_STARTED_Multi()
    {
        TaskMetadata taskMetadata;
        List<TaskMetadata> result;

        for (int i = 0; i < 5; i++)
        {
            taskMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
            repairStatusDao.markTaskStatusChange(taskMetadata);

            result = repairStatusDao.getTaskHistory(taskMetadata.getTaskId());
            Assert.assertEquals(i+1, result.size());
            Assert.assertEquals(TaskStatus.STARTED, result.get(i).getStatus());
            Assert.assertEquals(hostId, result.get(i).getNodeId());
        }

        Assert.assertTrue("Failed to change the repair status to CANCELLED", repairStatusDao.markTaskCancelled(repairId, hostId));
        result = repairStatusDao.getTaskHistory(repairId);
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(TaskStatus.CANCELLED, result.get(3).getStatus());
    }

    private TaskMetadata generateRepairMetadata(String hostId)
    {
        TaskMetadata taskMetadata = new TaskMetadata();
        String testKS = "TestKS_" + nextRandomPositiveInt();
        String testTable = "TestTable_" + nextRandomPositiveInt();

        TableTaskConfig repairConfig = new TableTaskConfig(getContext().getConfig(), "default");
        repairConfig.setKeyspace(testKS).setName(testTable);

        taskMetadata.setClusterName(TEST_CLUSTER_NAME)
                    .setTaskId(repairId)
                    .setNodeId(hostId)
                    .setKeyspaceName(testKS)
                    .setTableName(testTable)
                    .setRepairNum(nextRandomPositiveInt())
                    .setStartToken("STARTToken_" + nextRandomPositiveInt())
                    .setEndToken("ENDToken_" + nextRandomPositiveInt())
                    .setRepairConfig(repairConfig);

        return taskMetadata;
    }
}