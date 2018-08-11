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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairSequenceDao;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.RepairStatus;
import org.apache.cassandra.utils.GuidGenerator;

public class RepairSequenceDaoImplTest extends EmbeddedUnitTestBase
{
    private IRepairSequenceDao repairSequenceDao;
    private int repairId;

    @Before
    public void beforeMethod()
    {
        context = getContext();
        repairSequenceDao = new RepairSequenceDaoImpl(context, getCassDaoUtil());
    }

    @Before
    public void setUpMethod()
    {
        repairId = getRandomRepairId();
    }

    @Test
    public void markRepairStartedOnInstance()
    {
        Assert.assertTrue(repairSequenceDao.markRepairStartedOnInstance(repairId, 222));
        SortedSet<RepairSequence> rSeq = repairSequenceDao.getRepairSequence(repairId);
        Assert.assertEquals(repairId, rSeq.first().getRepairId());
        Assert.assertEquals(Optional.of(222), Optional.of(rSeq.first().getSeq()));
        Assert.assertEquals(RepairStatus.STARTED, rSeq.first().getStatus());
    }

    @Test
    public void persistEndpointSeqMap()
    {
        repairSequenceDao.persistEndpointSeqMap(repairId, "default", generateHosts(3));
        Assert.assertEquals(3, repairSequenceDao.getRepairSequence(repairId).size());
    }

    @Test
    public void getRepairSequence()
    {
        repairSequenceDao.persistEndpointSeqMap(repairId, "default", generateHosts(3));
        SortedSet<RepairSequence> rSeq = repairSequenceDao.getRepairSequence(repairId);
        Assert.assertEquals(3, rSeq.size());
    }

    @Test
    public void cancelRepairOnNode()
    {
        int repairId = getRandomRepairId();
        boolean rSeq = repairSequenceDao.cancelRepairOnNode(repairId, 2);

        SortedSet<RepairSequence> r = repairSequenceDao.getRepairSequence(repairId);
        Assert.assertEquals(1, r.size());
        Assert.assertEquals(RepairStatus.CANCELLED, r.first().getStatus());
    }

    private List<RepairHost> generateHosts(int n)
    {
        List<RepairHost> allHosts = new LinkedList<>();
        for (int i = 0; i < n; i++)
        {
            try
            {
                RepairHost host = new RepairHost(InetAddress.getByName("127.0.0." + i), "dc1", "rack1", GuidGenerator.guid());
                allHosts.add(host);
            }
            catch (UnknownHostException e)
            {
            }
        }
        return allHosts;
    }
}