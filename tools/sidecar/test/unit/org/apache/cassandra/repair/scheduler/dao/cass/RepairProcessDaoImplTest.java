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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairProcessDao;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;

public class RepairProcessDaoImplTest extends EmbeddedUnitTestBase
{
    private IRepairProcessDao repairProcessDao;
    private int repairId;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        repairProcessDao = new RepairProcessDaoImpl(context, getCassDaoUtil());
        repairId = getRandomRepairId();
    }

    @After
    public void cleanupMethod()
    {

        repairProcessDao.deleteClusterRepairStatus(repairId);
    }

    @Test
    public void getClusterRepairStatus()
    {
        Assert.assertFalse(repairProcessDao.getClusterRepairStatus().isPresent());
        Assert.assertTrue(repairProcessDao.acquireRepairInitLock(repairId));
        Assert.assertTrue(repairProcessDao.acquireRepairInitLock(repairId + 1));
        Assert.assertTrue(repairProcessDao.acquireRepairInitLock(repairId + 2));
        Assert.assertEquals(RepairStatus.STARTED, repairProcessDao.getClusterRepairStatus().get().getRepairStatus());
        Assert.assertEquals(repairId + 2, repairProcessDao.getClusterRepairStatus().get().getRepairId());
    }

    @Test
    public void acquireRepairInitLock()
    {
        Assert.assertTrue(repairProcessDao.acquireRepairInitLock(repairId));
        Assert.assertEquals(RepairStatus.STARTED, repairProcessDao.getClusterRepairStatus().get().getRepairStatus());
    }

    @Test
    public void markClusterRepairCompleted()
    {
        int repairId = getRandomRepairId();
        Assert.assertTrue(repairProcessDao.acquireRepairInitLock(repairId));
        Assert.assertEquals(repairProcessDao.getClusterRepairStatus().get().getRepairStatus(), RepairStatus.STARTED);
        Assert.assertTrue(repairProcessDao.markClusterRepairFinished(repairId));
        Assert.assertEquals(repairProcessDao.getClusterRepairStatus().get().getRepairStatus(), RepairStatus.FINISHED);
    }
}