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
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairSequenceDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairStatusDao;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;

public class RepairStatusDaoImplTest extends EmbeddedUnitTestBase
{
    private IRepairStatusDao repairStatusDao;
    private String hostId;
    private int repairId;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        repairStatusDao = new RepairStatusDaoImpl(context, getCassDaoUtil());
        IRepairSequenceDao repairSequenceDao = new RepairSequenceDaoImpl(context, getCassDaoUtil());
        hostId = context.getCassInteraction().getLocalHostId();
        repairId = getRandomRepairId();
    }

    @After
    public void cleanupMethod()
    {
        context.localSession().execute("TRUNCATE TABLE "+context.getConfig().getRepairKeyspace()+"."+context.getConfig().repair_status_tablename+";");
    }

    @Test
    public void markRepairStarted()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.STARTED, result.get(0).getStatus());
    }

    @Test
    public void markRepairPaused()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("PAUSED");
        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.PAUSED, result.get(0).getStatus());
    }

    @Test
    public void markRepairFailed()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("FAILED");

        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.FAILED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCompleted()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("FINISHED");

        repairStatusDao.markRepairStatusChange(repairMetadata);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.FINISHED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCancelled_NOTSTARTED()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId);

        repairStatusDao.markRepairCancelled(repairMetadata.getRepairId(), hostId);
        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void markRepairCancelled_STARTED()
    {
        RepairMetadata repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
        repairStatusDao.markRepairStatusChange(repairMetadata);

        List<RepairMetadata> result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.STARTED, result.get(0).getStatus());
        Assert.assertEquals(hostId, result.get(0).getNodeId());

        Assert.assertTrue("Failed to change the repair status to CANCELLED", repairStatusDao.markRepairCancelled(repairMetadata.getRepairId(), hostId));
        result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(RepairStatus.CANCELLED, result.get(0).getStatus());
    }

    @Test
    public void markRepairCancelled_STARTED_Multi()
    {
        RepairMetadata repairMetadata;
        List<RepairMetadata> result;

        for (int i = 0; i < 5; i++)
        {
            repairMetadata = generateRepairMetadata(hostId).setStatus("STARTED");
            repairStatusDao.markRepairStatusChange(repairMetadata);

            result = repairStatusDao.getRepairHistory(repairMetadata.getRepairId());
            Assert.assertEquals(i+1, result.size());
            Assert.assertEquals(RepairStatus.STARTED, result.get(i).getStatus());
            Assert.assertEquals(hostId, result.get(i).getNodeId());
        }

        Assert.assertTrue("Failed to change the repair status to CANCELLED", repairStatusDao.markRepairCancelled(repairId, hostId));
        result = repairStatusDao.getRepairHistory(repairId);
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(RepairStatus.CANCELLED, result.get(3).getStatus());
    }

    private RepairMetadata generateRepairMetadata(String hostId)
    {
        RepairMetadata repairMetadata = new RepairMetadata();
        String testKS = "TestKS_" + nextRandomPositiveInt();
        String testTable = "TestTable_" + nextRandomPositiveInt();

        TableRepairConfig repairConfig = new TableRepairConfig(getContext().getConfig(), "default");
        repairConfig.setKeyspace(testKS).setName(testTable);

        repairMetadata.setClusterName(TEST_CLUSTER_NAME)
                      .setRepairId(repairId)
                      .setNodeId(hostId)
                      .setKeyspaceName(testKS)
                      .setTableName(testTable)
                      .setRepairNum(nextRandomPositiveInt())
                      .setStartToken("STARTToken_" + nextRandomPositiveInt())
                      .setEndToken("ENDToken_" + nextRandomPositiveInt())
                      .setRepairConfig(repairConfig);

        return repairMetadata;
    }
}