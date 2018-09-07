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

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairHookDao;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class RepairHookDaoImplTest extends EmbeddedUnitTestBase
{
    private IRepairHookDao repairHookDao;
    private String hostId;
    private int repairId;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        hostId = context.getCassInteraction().getLocalHostId();
        repairHookDao = new RepairHookDaoImpl(context, getCassDaoUtil());
    }

    @Before
    public void setUpMethod()
    {
        repairId = getRandomRepairId();
    }

    @Test
    public void markClusterPostRepairHookStarted()
    {
        Assert.assertTrue(repairHookDao.markLocalPostRepairHookStarted(repairId));
        Assert.assertEquals(RepairStatus.STARTED, repairHookDao.getLocalRepairHookStatus(repairId, hostId).getStatus());
    }

    @Test
    public void markClusterPostRepairHookCompleted()
    {
        Assert.assertTrue(repairHookDao.markLocalPostRepairHookEnd(repairId, RepairStatus.FINISHED, Collections.emptyMap()));
        Assert.assertEquals(RepairStatus.FINISHED, repairHookDao.getLocalRepairHookStatus(repairId, hostId).getStatus());
    }

    @Test
    public void markClusterPostRepairHookFailed()
    {
        Assert.assertTrue(repairHookDao.markLocalPostRepairHookEnd(repairId, RepairStatus.FAILED, Collections.emptyMap()));
        Assert.assertEquals(RepairStatus.FAILED, repairHookDao.getLocalRepairHookStatus(repairId, hostId).getStatus());
    }

    @Test
    public void getClusterRepairHookStatus()
    {
        Assert.assertTrue(repairHookDao.markLocalPostRepairHookEnd(repairId, RepairStatus.FAILED, Collections.emptyMap()));
        Assert.assertEquals(RepairStatus.FAILED, repairHookDao.getLocalRepairHookStatus(repairId, hostId).getStatus());
    }

    @Test
    public void getClusterRepairHookStatus1()
    {
        Assert.assertTrue(repairHookDao.markLocalPostRepairHookStarted(repairId));
        List<RepairMetadata> statuses = repairHookDao.getLocalRepairHookStatus(repairId);
        Assert.assertEquals(RepairStatus.STARTED, statuses.get(0).getStatus());
    }
}