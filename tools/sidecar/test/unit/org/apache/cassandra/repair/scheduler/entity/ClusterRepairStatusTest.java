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

package org.apache.cassandra.repair.scheduler.entity;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterRepairStatusTest
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterRepairStatusTest.class);
    public volatile int count;

    @Test
    public void getRepairDurationInMin() throws Exception
    {
        int minutes = 5;
        ClusterRepairStatus clusterRepairStatus = new ClusterRepairStatus();

        DateTime now = DateTime.now();
        clusterRepairStatus.setStartTime(now.toDate()).setEndTime(now.minusMinutes(minutes).toDate());

        Assert.assertEquals("EndDate in Past", minutes, clusterRepairStatus.getRepairDurationInMin());

        now = DateTime.now();
        clusterRepairStatus.setStartTime(now.toDate()).setEndTime(now.toDate());

        Assert.assertEquals("EndDate and StartDate are same", 0, clusterRepairStatus.getRepairDurationInMin());

        now = DateTime.now();
        clusterRepairStatus.setStartTime(now.minusMinutes(minutes).toDate()).setEndTime(now.toDate());

        Assert.assertEquals("StartDate in Past", minutes, clusterRepairStatus.getRepairDurationInMin());
    }

    @Test
    public void sampleTest() throws Exception
    {

    }

}