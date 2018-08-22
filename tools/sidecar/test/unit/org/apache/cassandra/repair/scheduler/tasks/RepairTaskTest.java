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

package org.apache.cassandra.repair.scheduler.tasks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.TaskDaoManager;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairHookDao;
import org.apache.cassandra.repair.scheduler.dao.model.ITaskProcessDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairSequenceDao;
import org.apache.cassandra.repair.scheduler.dao.model.IRepairStatusDao;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.tasks.repair.RepairTask;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.Mockito;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class RepairTaskTest extends EmbeddedUnitTestBase
{
    private static final String TEST_CLUSTER_NAME = "Test Cluster";
    private static final int TEST_REPAIR_ID = 4;
    private static final String TEST_OTHER_NODE_ID = "0";
    private static final AtomicInteger seqNumber = new AtomicInteger();
    private static final Range<Token> TEST_RING_RANGE = new Range<>(new Murmur3Partitioner.LongToken(-9223372036854775808L),
                                                                    new Murmur3Partitioner.LongToken(-9223372036854775808L));

    private CassandraInteraction interactionSpy;
    private TaskSchedulerConfig configSpy;
    private RepairTask repairTask;
    private ITaskProcessDao repairProcessDaoSpy;
    private IRepairSequenceDao repairSequenceDaoSpy;
    private IRepairStatusDao repairStatusDaoSpy;
    private IRepairHookDao repairHookDaoSpy;


    @Before
    public void beforeMethod()
    {
        context = getContext();

        // Mock-Spy
        TaskSchedulerContext contextSpy = Mockito.spy(context);

        configSpy = Mockito.spy(context.getConfig());
        interactionSpy = Mockito.spy(context.getCassInteraction());

        doReturn(interactionSpy).when(contextSpy).getCassInteraction();
        when(contextSpy.getConfig()).thenReturn(configSpy);

        TaskDaoManager taskDaoManagerSpy = Mockito.spy(new TaskDaoManager(contextSpy));
        repairTask = new RepairTask(contextSpy, taskDaoManagerSpy);

        repairProcessDaoSpy = Mockito.spy(taskDaoManagerSpy.getRepairProcessDao());
        repairSequenceDaoSpy = Mockito.spy(taskDaoManagerSpy.getRepairSequenceDao());
        repairStatusDaoSpy = Mockito.spy(taskDaoManagerSpy.getRepairStatusDao());
        repairHookDaoSpy = Mockito.spy(taskDaoManagerSpy.getRepairHookDao());

        when(taskDaoManagerSpy.getRepairProcessDao()).thenReturn(repairProcessDaoSpy);
        when(taskDaoManagerSpy.getRepairSequenceDao()).thenReturn(repairSequenceDaoSpy);
        when(taskDaoManagerSpy.getRepairStatusDao()).thenReturn(repairStatusDaoSpy);
        when(taskDaoManagerSpy.getRepairHookDao()).thenReturn(repairHookDaoSpy);
    }

    @Test
    public void canRunRepair()
    {
        doReturn(false).when(interactionSpy).isRepairRunning(false);
        Assert.assertTrue(repairTask.canRunTask(TEST_REPAIR_ID));

        when(interactionSpy.isRepairRunning(false)).thenReturn(true);
        Assert.assertFalse(repairTask.canRunTask(TEST_REPAIR_ID));
    }

    @Test
    public void canRunRepairChecksPaused()
    {
        doReturn(false).when(interactionSpy).isRepairRunning(false);

        ClusterTaskStatus mockRepairStatus = Mockito.mock(ClusterTaskStatus.class);
        RepairSequence mockRepairSequence = Mockito.mock(RepairSequence.class);

        when(mockRepairStatus.getTaskStatus()).thenReturn(TaskStatus.PAUSED);
        when(mockRepairSequence.getStatus()).thenReturn(TaskStatus.PAUSED);

        doReturn(Optional.of(mockRepairStatus)).when(repairProcessDaoSpy).getClusterRepairStatus(anyInt());
        doReturn(Optional.of(mockRepairSequence)).when(repairSequenceDaoSpy).getMyNodeStatus(anyInt());

        Assert.assertFalse("Should not be running repair when RepairProcess and RepairSequence statuses are PAUSED",
                           repairTask.canRunTask(TEST_REPAIR_ID));

        when(mockRepairStatus.getTaskStatus()).thenReturn(TaskStatus.STARTED);
        when(mockRepairSequence.getStatus()).thenReturn(TaskStatus.STARTED);

        Assert.assertTrue("Should be running repair when RepairProcess and RepairSequence statuses are STARTED",
                          repairTask.canRunTask(TEST_REPAIR_ID));
    }

    @Test
    public void isRepairRunningOnCluster()
    {
        List<RepairMetadata> repairHistory = new LinkedList<>();
        doReturn(repairHistory).when(repairStatusDaoSpy).getRepairHistory(anyInt());

        generateTestRepairHistory(repairHistory, TaskStatus.STARTED, TaskStatus.STARTED);
        Assert.assertTrue(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.FINISHED);
        Assert.assertFalse(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.STARTED);
        Assert.assertTrue(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.CANCELLED);
        Assert.assertFalse(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.FAILED);
        Assert.assertFalse(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.NOTIFS_LOST);
        Assert.assertFalse(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.STARTED, TaskStatus.FAILED);
        Assert.assertTrue(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.FINISHED, TaskStatus.STARTED);
        Assert.assertTrue(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        generateTestRepairHistory(repairHistory, TaskStatus.FINISHED, TaskStatus.FAILED, TaskStatus.CANCELLED);
        Assert.assertFalse(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));

        // If the in progress node is this node, it should get false.
        repairHistory.clear();
        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_OTHER_NODE_ID,
                                             getKeyspace(), getTable())
                          .setStatus(TaskStatus.FINISHED));
        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, this.interactionSpy.getLocalHostId(),
                                             getKeyspace(), getTable())
                          .setStatus(TaskStatus.STARTED));
        Assert.assertFalse(repairTask.isSequenceRunningOnCluster(getRandomRepairId()));
    }

    /**
     * This test is to ensure that repair controller is not filtering the null values
     */
    @Test
    public void isRepairStuckOnCluster()
    {
        Assert.assertFalse(repairTask.isSequenceGenerationStuckOnCluster(TEST_REPAIR_ID));

        ClusterTaskStatus mockRepairStatus = Mockito.mock(ClusterTaskStatus.class);
        doReturn(DateTime.now().minusMinutes(1900).toDate()).when(mockRepairStatus).getStartTime();
        doReturn(Optional.of(mockRepairStatus)).when(repairProcessDaoSpy).getClusterRepairStatus();

        doReturn(new TreeSet<RepairSequence>()).when(repairSequenceDaoSpy).getRepairSequence(anyInt());

        Assert.assertTrue(repairTask.isSequenceGenerationStuckOnCluster(TEST_REPAIR_ID));
    }

    @Test
    public void amINextInRepairOrDone()
    {
        String TEST_NODE_ID = this.interactionSpy.getLocalHostId();
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(35).toDate()));
        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertEquals("I am next in repair", 3,
                            repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.PAUSED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(35).toDate()));
        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am not next in repair because I was paused before 30 min back", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.PAUSED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(25).toDate()));
        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am not next in repair because I was paused less than 30 min back", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

//        repSeq.clear();
//        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
//        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
//        repSeq.add(getRepairSeq(3, TaskStatus.CANCELLED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(35).toDate()));
//        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED));
//        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
//        Assert.assertEquals("I am next in repair because I was cancelled before 30 min back", 3, repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.CANCELLED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(25).toDate()));
        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am not next in repair because I was cancelled less than 30 min back", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

//        repSeq.clear();
//        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
//        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
//        repSeq.add(getRepairSeq(3, TaskStatus.FAILED).setNodeId(TEST_NODE_ID));
//        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED));
//        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
//        Assert.assertEquals("I am next in repair because I was failed", 3, repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.NOT_STARTED).setNodeId("blah"));
        repSeq.add(getRepairSeq(2, TaskStatus.NOT_STARTED).setNodeId("blah").setEndTime(DateTime.now().minusMinutes(2).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.FAILED).setNodeId("blah"));
        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED).setNodeId(TEST_NODE_ID));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am not next in repair because no-one started repair process", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

//        repSeq.clear();
//        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
//        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(2).toDate()));
//        repSeq.add(getRepairSeq(3, TaskStatus.FAILED).setNodeId("blah"));
//        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED).setNodeId(TEST_NODE_ID));
//        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
//        Assert.assertNotEquals("I am not next in repair because last finished repair was not 30 min back",4,  repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(23).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId("blah"));
        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED).setNodeId(TEST_NODE_ID));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am not next in repair because last finished repair was not before 30 min back", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

//        repSeq.clear();
//        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
//        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(31).toDate()));
//        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId("blah"));
//        repSeq.add(getRepairSeq(4, TaskStatus.NOT_STARTED).setNodeId(TEST_NODE_ID));
//        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
//        Assert.assertEquals("I am next in repair because last finished repair is before 30 min back", 4, repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(31).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId("blah"));
        repSeq.add(getRepairSeq(4, TaskStatus.PAUSED).setNodeId(TEST_NODE_ID));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am not next in repair because last finished repair is before 30 min back and I was paused", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(31).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId("blah"));
        repSeq.add(getRepairSeq(4, TaskStatus.CANCELLED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(29).toDate()));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
        Assert.assertFalse("I am next not in repair because last finished repair is before 30 min back and I was cancelled less than 30 min back", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());

//        repSeq.clear();
//        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
//        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(33).toDate()));
//        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId("blah"));
//        repSeq.add(getRepairSeq(4, TaskStatus.CANCELLED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(31).toDate()));
//        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED));
//        Assert.assertEquals("I am next in repair because last finished repair is before 30 min back and I was cancelled before 30 min back", 4, repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setEndTime(DateTime.now().minusMinutes(33).toDate()));
        repSeq.add(getRepairSeq(3, TaskStatus.NOT_STARTED).setNodeId("blah"));
        repSeq.add(getRepairSeq(4, TaskStatus.FINISHED).setNodeId(TEST_NODE_ID).setPauseTime(DateTime.now().minusMinutes(31).toDate()));
        repSeq.add(getRepairSeq(5, TaskStatus.NOT_STARTED).setNodeId("blah1"));
        Assert.assertEquals("I am supposed to get 4 since I already completed repair", 4, repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).get().getSeq().intValue());

        repSeq.clear();
        Assert.assertFalse("I should tolerate empty sequences", repairTask.amINextInSequenceOrDone(TEST_REPAIR_ID).isPresent());
    }

    @Test
    public void getTablesForRepairMultipleRanges()
    {
            loadDataset(100);

        String start = TEST_RING_RANGE.left.toString();
        String end = TEST_RING_RANGE.right.toString();
        String TEST_NODE_ID = interactionSpy.getLocalHostId();
        Range<Token> one = interactionSpy.tokenRangeFromStrings(start, "0");
        Range<Token> two = interactionSpy.tokenRangeFromStrings("0", end);
        List<Range<Token>> testRanges = new ArrayList<>();
        testRanges.add(one);
        testRanges.add(two);

        List<RepairMetadata> repairHistory = new LinkedList<>();

        doReturn(testRanges).when(interactionSpy).getTokenRanges(anyString(), eq(true));
        doReturn(repairHistory).when(repairStatusDaoSpy).getRepairHistory(anyInt(), anyString());

        Assert.assertEquals("Did not get correct #Tables for Repair", repairableTables,
                            getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, DEFAULT_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(one.left.toString()).setEndToken(one.right.toString()));
        Assert.assertEquals(repairableTables, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, DEFAULT_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(one.left.toString()).setEndToken(one.right.toString()));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, DEFAULT_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(two.left.toString()).setEndToken(two.right.toString()));
        Assert.assertEquals(repairableTables - 1, getTablesForRepairExcludeSystem().size());
    }

    @Test
    public void getTablesForRepair()
    {
        loadDataset(100);

        List<Range<Token>> tokenRanges = this.interactionSpy.getTokenRanges(TEST_REPAIR_KS, true);
        Murmur3Partitioner.LongToken startToken = (Murmur3Partitioner.LongToken) tokenRanges.get(0).left;
        Murmur3Partitioner.LongToken endToken = (Murmur3Partitioner.LongToken) tokenRanges.get(0).right;
        String start = startToken.toString();
        String end = endToken.toString();

        String TEST_NODE_ID = interactionSpy.getLocalHostId();
        List<RepairMetadata> repairHistory = new LinkedList<>();

        doReturn(repairHistory).when(repairStatusDaoSpy).getRepairHistory(anyInt(), anyString());

        repairHistory.clear();
        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME).setStatus(TaskStatus.FINISHED).setStartToken(start).setEndToken(end));
        Assert.assertEquals(repairableTables - 1, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        Assert.assertEquals(repairableTables, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.STARTED)
        .setStartToken(start).setEndToken(end));
        Assert.assertEquals(repairableTables, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.FAILED)
        .setStartToken(start).setEndToken(end));
        Assert.assertEquals(repairableTables - 1, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.PAUSED)
        .setStartToken(start).setEndToken(end));
        Assert.assertEquals(repairableTables, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(start).setEndToken("-4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken("-4611686018427387904").setEndToken("0"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.STARTED)
        .setStartToken("0").setEndToken("4611686018427387904"));

        Assert.assertEquals(repairableTables, getTablesForRepairExcludeSystem().size());

        repairHistory.clear();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(start).setEndToken("-4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.STARTED)
        .setStartToken("-4611686018427387904").setEndToken("0"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.STARTED)
        .setStartToken("0").setEndToken("4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
        .setStatus(TaskStatus.FINISHED)
        .setStartToken("4611686018427387904").setEndToken(end));

        Assert.assertEquals(repairableTables, getTablesForRepairExcludeSystem().size());

    }

    @Test
    public void getTablesForRepair_Subrange_Completed()
    {
        loadDataset(100);
        List<Range<Token>> tokenRanges = this.interactionSpy.getTokenRanges(TEST_REPAIR_KS, true);
        Murmur3Partitioner.LongToken startToken = (Murmur3Partitioner.LongToken) tokenRanges.get(0).left;
        Murmur3Partitioner.LongToken endToken = (Murmur3Partitioner.LongToken) tokenRanges.get(0).right;
        Token p1 = startToken.increaseSlightly();
        Token p2 = p1.increaseSlightly().increaseSlightly().increaseSlightly();
        Token p3 = p2.increaseSlightly().increaseSlightly().increaseSlightly();
        String start = startToken.toString();
        String end = endToken.toString();


        String TEST_NODE_ID = interactionSpy.getLocalHostId();
        List<RepairMetadata> repairHistory = new LinkedList<>();

        doReturn(repairHistory).when(repairStatusDaoSpy).getRepairHistory(anyInt(), anyString());
        List<TableTaskConfig> tablesForRepair = repairTask.getTablesForRepair(TEST_REPAIR_ID, "default");

        Assert.assertTrue(tablesForRepair.stream()
                                         .anyMatch(tr -> tr.getName().equalsIgnoreCase(SUBRANGE_TEST_TBL_NAME)));

        repairHistory.clear();
        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
                          .setStatus(TaskStatus.FINISHED)
                          .setStartToken(start).setEndToken(p1.toString()));

        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
                          .setStatus(TaskStatus.FINISHED)
                          .setStartToken(p1.toString())
                          .setEndToken(p2.toString()));

        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
                          .setStatus(TaskStatus.FINISHED)
                          .setStartToken(p2.toString())
                          .setEndToken(p3.toString()));

        repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, TEST_REPAIR_KS, SUBRANGE_TEST_TBL_NAME)
                          .setStatus(TaskStatus.FINISHED)
                          .setStartToken(p3.toString())
                          .setEndToken(end));

        tablesForRepair = repairTask.getTablesForRepair(TEST_REPAIR_ID, "default");
        Assert.assertFalse(tablesForRepair.stream()
                                          .anyMatch(tr -> tr.getName().equalsIgnoreCase(SUBRANGE_TEST_TBL_NAME)));
    }

    @Test
    //TODO Re-write this test
    public void getRangesForRepair()
    {
        /*
        String TEST_NODE_ID = interactionSpy.getLocalHostId();

        String start = TEST_RING_RANGE.left.toString();
        String end = TEST_RING_RANGE.right.toString();

        TableTaskConfig mockSubRangeTable = new TableTaskConfig(configSpy)
                                              .setKeyspace(REPAIR_SCHEDULER_KS_NAME).setName("repair_config")
                                              .setRepairOptions(new RepairOptions(configSpy)
                                                                .setType(RepairType.FULL)
                                                                .setSplitStrategy("100"));

        List<Range<Token>> tokenSplits = new LinkedList<>();
        tokenSplits.add(new Range<Token>(start, "-6917529027641081856"));
        tokenSplits.add(new Range<Token>("-6917529027641081856", "-4611686018427387904"));
        tokenSplits.add(new Range<Token>("-4611686018427387904", "-2305843009213693952"));
        tokenSplits.add(new Range<Token>("-2305843009213693952", "0"));
        tokenSplits.add(new Range<Token>("0", "2305843009213693952"));
        tokenSplits.add(new Range<Token>("2305843009213693952", "4611686018427387904"));
        tokenSplits.add(new Range<Token>("4611686018427387904", "6917529027641081856"));
        tokenSplits.add(new Range<Token>("6917529027641081856", end));

        Mockito.when(interactionSpy.getTokenRangeSplits(mockSubRangeTable.getKeyspace(),
                                                        mockSubRangeTable.getName(), TEST_RING_RANGE,
                                                        mockSubRangeTable.getRepairOptions().getSplitStrategy()))
               .thenReturn(tokenSplits);

        List<Range<Token>> expectedRanges = new ArrayList<>();
        expectedRanges.add(new Range<Token>("-2305843009213693952", "0"));
        expectedRanges.add(new Range<Token>("0", "2305843009213693952"));
        expectedRanges.add(new Range<Token>("2305843009213693952", "4611686018427387904"));
        expectedRanges.add(new Range<Token>("4611686018427387904", "6917529027641081856"));
        expectedRanges.add(new Range<Token>("6917529027641081856", end));

        List<RepairMetadata> repairHistory = new LinkedList<>();
        Mockito.when(repairStatusDaoSpy.getRepairHistory(
        TEST_REPAIR_ID, mockSubRangeTable.getKeyspace(), mockSubRangeTable.getName(), TEST_NODE_ID))
               .thenReturn(repairHistory);

        Assert.assertEquals(8, repairTask.getRangesForRepair(TEST_REPAIR_ID, mockSubRangeTable).size());

        repairHistory = new LinkedList<>();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(start).setEndToken("-4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.FINISHED)
        .setStartToken("-4611686018427387904").setEndToken("0"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.NOTIFS_LOST)
        .setStartToken("0").setEndToken("4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.STARTED)
        .setStartToken("4611686018427387904").setEndToken(end));

        Mockito.when(repairStatusDaoSpy.getRepairHistory(
        TEST_REPAIR_ID, mockSubRangeTable.getKeyspace(), mockSubRangeTable.getName(), TEST_NODE_ID))
               .thenReturn(repairHistory);

        Assert.assertEquals(expectedRanges, repairTask.getRangesForRepair(TEST_REPAIR_ID, mockSubRangeTable));
         */
    }

    @Test
    //TODO Re-write this test
    public void getRangesForRepairMultipleRanges()
    {
        /*
        String TEST_NODE_ID = interactionSpy.getLocalHostId();
        String start = TEST_RING_RANGE.left.toString();
        String end = TEST_RING_RANGE.right.toString();

        Range<Token> one = new Range<Token>(start, "0");
        Range<Token> two = new Range<Token>("0", end);
        List<Range<Token>> testRanges = new ArrayList<>();
        testRanges.add(one);
        testRanges.add(two);
        Mockito.when(interactionSpy.getTokenRanges(anyString(), true)).thenReturn(testRanges);

        TableTaskConfig mockSubRangeTable = new TableTaskConfig(configSpy)
                                              .setKeyspace(REPAIR_SCHEDULER_KS_NAME).setName("repair_config")
                                              .setRepairOptions(new RepairOptions(configSpy)
                                                                .setType(RepairType.FULL)
                                                                .setSplitStrategy("100")
                                                                .setNumWorkers(3));

        List<Range<Token>> tokenSplitsOne = new LinkedList<>();
        tokenSplitsOne.add(new Range<Token>(start, "-6917529027641081856"));
        tokenSplitsOne.add(new Range<Token>("-6917529027641081856", "-4611686018427387904"));
        tokenSplitsOne.add(new Range<Token>("-4611686018427387904", "-2305843009213693952"));
        tokenSplitsOne.add(new Range<Token>("-2305843009213693952", "0"));

        List<Range<Token>> tokenSplitsTwo = new LinkedList<>();
        tokenSplitsTwo.add(new Range<Token>("0", "2305843009213693952"));
        tokenSplitsTwo.add(new Range<Token>("2305843009213693952", "4611686018427387904"));
        tokenSplitsTwo.add(new Range<Token>("4611686018427387904", "6917529027641081856"));
        tokenSplitsTwo.add(new Range<Token>("6917529027641081856", end));

        Mockito.when(interactionSpy.getTokenRangeSplits(
        mockSubRangeTable.getKeyspace(), mockSubRangeTable.getName(), one,
        mockSubRangeTable.getRepairOptions().getSplitStrategy()))
               .thenReturn(tokenSplitsOne);

        Mockito.when(interactionSpy.getTokenRangeSplits(
        mockSubRangeTable.getKeyspace(), mockSubRangeTable.getName(), two,
        mockSubRangeTable.getRepairOptions().getSplitStrategy()))
               .thenReturn(tokenSplitsTwo);

        List<Range<Token>> expectedRanges = new ArrayList<>();
        expectedRanges.add(new Range<Token>("-2305843009213693952", "0"));
        expectedRanges.add(new Range<Token>("0", "2305843009213693952"));
        expectedRanges.add(new Range<Token>("2305843009213693952", "4611686018427387904"));
        expectedRanges.add(new Range<Token>("4611686018427387904", "6917529027641081856"));
        expectedRanges.add(new Range<Token>("6917529027641081856", end));

        List<RepairMetadata> repairHistory = new LinkedList<>();
        Mockito.when(repairStatusDaoSpy.getRepairHistory(
        TEST_REPAIR_ID, mockSubRangeTable.getKeyspace(), mockSubRangeTable.getName(), TEST_NODE_ID))
               .thenReturn(repairHistory);

        Assert.assertEquals(8, repairTask.getRangesForRepair(TEST_REPAIR_ID, mockSubRangeTable).size());

        repairHistory = new LinkedList<>();
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.FINISHED)
        .setStartToken(start).setEndToken("-4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.FINISHED)
        .setStartToken("-4611686018427387904").setEndToken("0"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.NOTIFS_LOST)
        .setStartToken("0").setEndToken("4611686018427387904"));
        repairHistory.add(
        new RepairMetadata(TEST_CLUSTER_NAME, TEST_NODE_ID, REPAIR_SCHEDULER_KS_NAME, "repair_config")
        .setStatus(TaskStatus.STARTED)
        .setStartToken("4611686018427387904").setEndToken(end));

        Mockito.when(repairStatusDaoSpy.getRepairHistory(
        TEST_REPAIR_ID, mockSubRangeTable.getKeyspace(), mockSubRangeTable.getName(), TEST_NODE_ID))
               .thenReturn(repairHistory);

        Assert.assertEquals(expectedRanges, repairTask.getRangesForRepair(TEST_REPAIR_ID, mockSubRangeTable));
    */
    }

    @Test
    public void amIReadyForPostRepairHookSingleToken()
    {
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        repSeq.add(getRepairSeq(1, TaskStatus.STARTED).setNodeId("717964c6-68d2-4d13-904b-434fed5b75a8"));
        repSeq.add(getRepairSeq(2, TaskStatus.STARTED).setNodeId("285278bc-9e89-4c6b-b367-24ab23aa4463"));
        repSeq.add(getRepairSeq(3, TaskStatus.FINISHED).setNodeId(interactionSpy.getLocalHostId()));
        repSeq.add(getRepairSeq(4, TaskStatus.FINISHED).setNodeId("6083ab0c-5867-4e4e-ac62-99d9d4847728"));
        repSeq.add(getRepairSeq(5, TaskStatus.FINISHED).setNodeId("8d572ca2-1367-48ae-92a4-593ed91b7929"));
        repSeq.add(getRepairSeq(6, TaskStatus.STARTED).setNodeId("1c944082-de9b-4e00-a224-8b0a0f42eda6"));

        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());
        /*
        ( 6148914691236517202, -9223372036854775808 ) = ( 127.0.3.1, 127.0.3.2, 127.0.3.3 );
        ( -9223372036854775808, -6148914691236517206 ) = ( 127.0.3.2, 127.0.3.3, 127.0.3.4 );
        ( -3074457345618258604, -2 ) = ( 127.0.3.4, 127.0.3.5, 127.0.3.6 );
        ( 3074457345618258600, 6148914691236517202 ) = ( 127.0.3.6, 127.0.3.1, 127.0.3.2 );
        ( -2, 3074457345618258600 ) = ( 127.0.3.5, 127.0.3.6, 127.0.3.1 );
        ( -6148914691236517206, -3074457345618258604 ) = ( 127.0.3.3, 127.0.3.4, 127.0.3.5 );
         */
        HashMap<List<String>, List<String>> mockRangeToEndpointMap = new HashMap<>();
        mockRangeToEndpointMap.put(Arrays.asList("6148914691236517202", "-9223372036854775808"),
                                   Arrays.asList("127.0.1.1", "127.0.1.2", "127.0.1.3"));
        mockRangeToEndpointMap.put(Arrays.asList("-9223372036854775808", "-6148914691236517206"),
                                   Arrays.asList("127.0.1.2", "127.0.1.3", "127.0.1.4"));
        mockRangeToEndpointMap.put(Arrays.asList("-6148914691236517206", "-3074457345618258604"),
                                   Arrays.asList("127.0.1.3", "127.0.1.4", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("-3074457345618258604", "-2"),
                                   Arrays.asList("127.0.1.4", "127.0.1.5", "127.0.1.6"));
        mockRangeToEndpointMap.put(Arrays.asList("-2", "3074457345618258600"),
                                   Arrays.asList("127.0.1.5", "127.0.1.6", "127.0.1.1"));
        mockRangeToEndpointMap.put(Arrays.asList("3074457345618258600", "6148914691236517202"),
                                   Arrays.asList("127.0.1.6", "127.0.1.1", "127.0.1.2"));


        Map<String, String> endpointToHostIdMap = new HashMap<>();

        endpointToHostIdMap.put("127.0.1.6", "717964c6-68d2-4d13-904b-434fed5b75a8");
        endpointToHostIdMap.put("127.0.1.5", "285278bc-9e89-4c6b-b367-24ab23aa4463");
        endpointToHostIdMap.put("127.0.1.4", "1c944082-de9b-4e00-a224-8b0a0f42eda6");
        endpointToHostIdMap.put("127.0.1.3", interactionSpy.getLocalHostId());
        endpointToHostIdMap.put("127.0.1.2", "6083ab0c-5867-4e4e-ac62-99d9d4847728");
        endpointToHostIdMap.put("127.0.1.1", "8d572ca2-1367-48ae-92a4-593ed91b7929");

        doReturn(endpointToHostIdMap).when(interactionSpy).getEndpointToHostIdMap();
        doReturn(mockRangeToEndpointMap).when(interactionSpy).getRangeToEndpointMap(anyString());
        doReturn("127.0.1.3").when(interactionSpy).getLocalEndpoint();

        Assert.assertTrue(repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID));

        int numReady = 0;
        for (int i = 1; i <= 6; i++)
        {
            doReturn("127.0.1." + i).when(interactionSpy).getLocalEndpoint();
            if (repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID))
                numReady += 1;
        }
        Assert.assertEquals(1, numReady);
    }

    @Test
    public void amIReadyForPostRepairHookNeighborsCompleted()
    {
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        repSeq.add(getRepairSeq(1, TaskStatus.NOT_STARTED).setNodeId("717964c6-68d2-4d13-904b-434fed5b75a8"));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setNodeId("285278bc-9e89-4c6b-b367-24ab23aa4463"));
        repSeq.add(getRepairSeq(3, TaskStatus.FINISHED).setNodeId(interactionSpy.getLocalHostId()));
        repSeq.add(getRepairSeq(4, TaskStatus.STARTED).setNodeId("6083ab0c-5867-4e4e-ac62-99d9d4847728"));
        repSeq.add(getRepairSeq(5, TaskStatus.STARTED).setNodeId("8d572ca2-1367-48ae-92a4-593ed91b7929"));
        repSeq.add(getRepairSeq(6, TaskStatus.STARTED).setNodeId("1c944082-de9b-4e00-a224-8b0a0f42eda6"));
        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());



        /*
        ( 6148914691236517202, -9223372036854775808 ) = ( 127.0.3.1, 127.0.3.2, 127.0.3.3 );
        ( -9223372036854775808, -6148914691236517206 ) = ( 127.0.3.2, 127.0.3.3, 127.0.3.4 );
        ( -3074457345618258604, -2 ) = ( 127.0.3.4, 127.0.3.5, 127.0.3.6 );
        ( 3074457345618258600, 6148914691236517202 ) = ( 127.0.3.6, 127.0.3.1, 127.0.3.2 );
        ( -2, 3074457345618258600 ) = ( 127.0.3.5, 127.0.3.6, 127.0.3.1 );
        ( -6148914691236517206, -3074457345618258604 ) = ( 127.0.3.3, 127.0.3.4, 127.0.3.5 );
         */
        HashMap<List<String>, List<String>> mockRangeToEndpointMap = new HashMap<>();
        mockRangeToEndpointMap.put(Arrays.asList("6148914691236517202", "-9223372036854775808"),
                                   Arrays.asList("127.0.1.1", "127.0.1.2", "127.0.1.3"));
        mockRangeToEndpointMap.put(Arrays.asList("-9223372036854775808", "-6148914691236517206"),
                                   Arrays.asList("127.0.1.2", "127.0.1.3", "127.0.1.4"));
        mockRangeToEndpointMap.put(Arrays.asList("-6148914691236517206", "-3074457345618258604"),
                                   Arrays.asList("127.0.1.3", "127.0.1.4", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("-3074457345618258604", "-2"),
                                   Arrays.asList("127.0.1.4", "127.0.1.5", "127.0.1.6"));
        mockRangeToEndpointMap.put(Arrays.asList("-2", "3074457345618258600"),
                                   Arrays.asList("127.0.1.5", "127.0.1.6", "127.0.1.1"));
        mockRangeToEndpointMap.put(Arrays.asList("3074457345618258600", "6148914691236517202"),
                                   Arrays.asList("127.0.1.6", "127.0.1.1", "127.0.1.2"));

        doReturn(mockRangeToEndpointMap).when(interactionSpy).getRangeToEndpointMap(anyString());
        doReturn("127.0.1.3").when(interactionSpy).getLocalEndpoint();

        Map<String, String> endpointToHostIdMap = new HashMap<>();

        endpointToHostIdMap.put("127.0.1.6", "717964c6-68d2-4d13-904b-434fed5b75a8");
        endpointToHostIdMap.put("127.0.1.5", "285278bc-9e89-4c6b-b367-24ab23aa4463");
        endpointToHostIdMap.put("127.0.1.4", "1c944082-de9b-4e00-a224-8b0a0f42eda6");
        endpointToHostIdMap.put("127.0.1.3", interactionSpy.getLocalHostId());
        endpointToHostIdMap.put("127.0.1.2", "6083ab0c-5867-4e4e-ac62-99d9d4847728");
        endpointToHostIdMap.put("127.0.1.1", "8d572ca2-1367-48ae-92a4-593ed91b7929");

        doReturn(endpointToHostIdMap).when(interactionSpy).getEndpointToHostIdMap();

        Assert.assertFalse("PostRepair is ready to start when neighbors completed their repair, but current node repair is not", repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID));
    }

    @Test
    public void amIReadyForPostRepairHookPartialSequence()
    {
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED).setNodeId("-9223372036854775808"));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setNodeId("-6148914691236517206"));
        repSeq.add(getRepairSeq(3, TaskStatus.FINISHED).setNodeId("3074457345618258600"));
        repSeq.add(getRepairSeq(4, TaskStatus.FINISHED).setNodeId("6148914691236517202"));
        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());

        /*
        ( 6148914691236517202, -9223372036854775808 ) = ( 127.0.3.1, 127.0.3.2, 127.0.3.3 );
        ( -9223372036854775808, -6148914691236517206 ) = ( 127.0.3.2, 127.0.3.3, 127.0.3.4 );
        ( -3074457345618258604, -2 ) = ( 127.0.3.4, 127.0.3.5, 127.0.3.6 );
        ( 3074457345618258600, 6148914691236517202 ) = ( 127.0.3.6, 127.0.3.1, 127.0.3.2 );
        ( -2, 3074457345618258600 ) = ( 127.0.3.5, 127.0.3.6, 127.0.3.1 );
        ( -6148914691236517206, -3074457345618258604 ) = ( 127.0.3.3, 127.0.3.4, 127.0.3.5 );
         */
        HashMap<List<String>, List<String>> mockRangeToEndpointMap = new HashMap<>();
        mockRangeToEndpointMap.put(Arrays.asList("6148914691236517202", "-9223372036854775808"),
                                   Arrays.asList("127.0.1.1", "127.0.1.2", "127.0.1.3"));
        mockRangeToEndpointMap.put(Arrays.asList("-9223372036854775808", "-6148914691236517206"),
                                   Arrays.asList("127.0.1.2", "127.0.1.3", "127.0.1.4"));
        mockRangeToEndpointMap.put(Arrays.asList("-6148914691236517206", "-3074457345618258604"),
                                   Arrays.asList("127.0.1.3", "127.0.1.4", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("-3074457345618258604", "-2"),
                                   Arrays.asList("127.0.1.4", "127.0.1.5", "127.0.1.6"));
        mockRangeToEndpointMap.put(Arrays.asList("-2", "3074457345618258600"),
                                   Arrays.asList("127.0.1.5", "127.0.1.6", "127.0.1.1"));
        mockRangeToEndpointMap.put(Arrays.asList("3074457345618258600", "6148914691236517202"),
                                   Arrays.asList("127.0.1.6", "127.0.1.1", "127.0.1.2"));

        doReturn(mockRangeToEndpointMap).when(interactionSpy).getRangeToEndpointMap(anyString());

        int numReady = 0;
        for (int i = 1; i <= 6; i++)
        {
            Mockito.when(interactionSpy.getLocalEndpoint()).thenReturn("127.0.1." + i);
            if (repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID))
                numReady += 1;
        }
        Assert.assertEquals(6, numReady);
    }

    @Test
    public void amIReadyForPostRepairHookNodeId()
    {
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());

        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED).setNodeId("aa67b5d5-6bd0-49c3-8637-1a1e970f49dc"));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setNodeId("85109ca4-aa95-463a-b797-c38ad77e1f15"));
        repSeq.add(getRepairSeq(3, TaskStatus.FINISHED).setNodeId("7cea4e9c-b8c5-43b2-bbaa-8b988f78aef7"));
        repSeq.add(getRepairSeq(4, TaskStatus.STARTED).setNodeId("f148711f-2cfa-4c02-94c0-54d7ebcfebfb"));
        repSeq.add(getRepairSeq(5, TaskStatus.STARTED).setNodeId("0b9359c0-9b87-4c20-bc5e-dab404a3c328"));
        repSeq.add(getRepairSeq(6, TaskStatus.STARTED).setNodeId("0871c188-b2c6-49ca-84da-e870bd023622"));

        /*
        ( 6148914691236517202, -9223372036854775808 ) = ( 127.0.1.1, 127.0.1.2, 127.0.1.3 );
        ( -9223372036854775808, -6148914691236517206 ) = ( 127.0.1.2, 127.0.1.3, 127.0.1.4 );
        ( -3074457345618258604, -2 ) = ( 127.0.1.4, 127.0.1.5, 127.0.1.6 );
        ( 3074457345618258600, 6148914691236517202 ) = ( 127.0.1.6, 127.0.1.1, 127.0.1.2 );
        ( -2, 3074457345618258600 ) = ( 127.0.1.5, 127.0.1.6, 127.0.1.1 );
        ( -6148914691236517206, -3074457345618258604 ) = ( 127.0.1.3, 127.0.1.4, 127.0.1.5 );
         */
        HashMap<List<String>, List<String>> mockRangeToEndpointMap = new HashMap<>();
        doReturn(mockRangeToEndpointMap).when(interactionSpy).getRangeToEndpointMap(anyString());

        mockRangeToEndpointMap.put(Arrays.asList("6148914691236517202", "-9223372036854775808"),
                                   Arrays.asList("127.0.1.1", "127.0.1.2", "127.0.1.3"));
        mockRangeToEndpointMap.put(Arrays.asList("-9223372036854775808", "-6148914691236517206"),
                                   Arrays.asList("127.0.1.2", "127.0.1.3", "127.0.1.4"));
        mockRangeToEndpointMap.put(Arrays.asList("-6148914691236517206", "-3074457345618258604"),
                                   Arrays.asList("127.0.1.3", "127.0.1.4", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("-3074457345618258604", "-2"),
                                   Arrays.asList("127.0.1.4", "127.0.1.5", "127.0.1.6"));
        mockRangeToEndpointMap.put(Arrays.asList("-2", "3074457345618258600"),
                                   Arrays.asList("127.0.1.5", "127.0.1.6", "127.0.1.1"));
        mockRangeToEndpointMap.put(Arrays.asList("3074457345618258600", "6148914691236517202"),
                                   Arrays.asList("127.0.1.6", "127.0.1.1", "127.0.1.2"));

        HashMap<String, String> mockEndpointToHostId = new HashMap<>();
        mockEndpointToHostId.put("127.0.1.1", "aa67b5d5-6bd0-49c3-8637-1a1e970f49dc");
        mockEndpointToHostId.put("127.0.1.2", "85109ca4-aa95-463a-b797-c38ad77e1f15");
        mockEndpointToHostId.put("127.0.1.3", "7cea4e9c-b8c5-43b2-bbaa-8b988f78aef7");
        mockEndpointToHostId.put("127.0.1.4", "f148711f-2cfa-4c02-94c0-54d7ebcfebfb");
        mockEndpointToHostId.put("127.0.1.5", "0b9359c0-9b87-4c20-bc5e-dab404a3c328");
        mockEndpointToHostId.put("127.0.1.6", "0871c188-b2c6-49ca-84da-e870bd023622");

        doReturn(mockEndpointToHostId).when(interactionSpy).getEndpointToHostIdMap();
        doReturn("127.0.1.3").when(interactionSpy).getLocalEndpoint();

        Assert.assertTrue(repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID));

        int numReady = 0;
        for (int i = 1; i <= 6; i++)
        {
            Mockito.when(interactionSpy.getLocalEndpoint()).thenReturn("127.0.1." + i);
            if (repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID))
                numReady += 1;
        }
        Assert.assertEquals(1, numReady);
    }

    @Test
    public void amIReadyForPostRepairHookVnodes()
    {
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());

        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED).setNodeId("aa67b5d5-6bd0-49c3-8637-1a1e970f49dc"));
        repSeq.add(getRepairSeq(2, TaskStatus.STARTED).setNodeId("85109ca4-aa95-463a-b797-c38ad77e1f15"));
        repSeq.add(getRepairSeq(3, TaskStatus.FINISHED).setNodeId("7cea4e9c-b8c5-43b2-bbaa-8b988f78aef7"));
        repSeq.add(getRepairSeq(4, TaskStatus.STARTED).setNodeId("f148711f-2cfa-4c02-94c0-54d7ebcfebfb"));
        repSeq.add(getRepairSeq(5, TaskStatus.FINISHED).setNodeId("0b9359c0-9b87-4c20-bc5e-dab404a3c328"));
        repSeq.add(getRepairSeq(6, TaskStatus.STARTED).setNodeId("0871c188-b2c6-49ca-84da-e870bd023622"));

        /*
        ( 6836127902814411582, 7037334694100538342 ) = ( 127.0.1.3, 127.0.1.4, 127.0.1.2 );
        ( -6226050461072679258, -5313593441555820686 ) = ( 127.0.1.2, 127.0.1.5, 127.0.1.6 );
        ( 139771853904925224, 1404432089502259838 ) = ( 127.0.1.1, 127.0.1.5, 127.0.1.3 );
        ( 1404432089502259838, 2268281908109175122 ) = ( 127.0.1.5, 127.0.1.3, 127.0.1.1 );
        ( 7037334694100538342, 7788525348058239787 ) = ( 127.0.1.4, 127.0.1.2, 127.0.1.5 );
        ( 2893766668419047554, 6836127902814411582 ) = ( 127.0.1.1, 127.0.1.3, 127.0.1.4 );
        ( -1524263151450829197, -1179764051153390809 ) = ( 127.0.1.6, 127.0.1.1, 127.0.1.5 );
        ( -3961581983597570892, -1524263151450829197 ) = ( 127.0.1.5, 127.0.1.6, 127.0.1.1 );
        ( -1179764051153390809, 139771853904925224 ) = ( 127.0.1.6, 127.0.1.1, 127.0.1.5 );
        ( -5313593441555820686, -3961581983597570892 ) = ( 127.0.1.2, 127.0.1.5, 127.0.1.6 );
        ( 2268281908109175122, 2893766668419047554 ) = ( 127.0.1.3, 127.0.1.1, 127.0.1.4 );
        ( 7788525348058239787, -6226050461072679258 ) = ( 127.0.1.4, 127.0.1.2, 127.0.1.5 );
        */
        HashMap<List<String>, List<String>> mockRangeToEndpointMap = new HashMap<>();
        doReturn(mockRangeToEndpointMap).when(interactionSpy).getRangeToEndpointMap(anyString());

        mockRangeToEndpointMap.put(Arrays.asList("6836127902814411582", "7037334694100538342"),
                                   Arrays.asList("127.0.1.3", "127.0.1.4", "127.0.1.2"));
        mockRangeToEndpointMap.put(Arrays.asList("-6226050461072679258", "-5313593441555820686"),
                                   Arrays.asList("127.0.1.2", "127.0.1.5", "127.0.1.6"));
        mockRangeToEndpointMap.put(Arrays.asList("139771853904925224", "1404432089502259838"),
                                   Arrays.asList("127.0.1.1", "127.0.1.5", "127.0.1.3"));
        mockRangeToEndpointMap.put(Arrays.asList("1404432089502259838", "2268281908109175122"),
                                   Arrays.asList("127.0.1.5", "127.0.1.3", "127.0.1.1"));
        mockRangeToEndpointMap.put(Arrays.asList("7037334694100538342", "7788525348058239787"),
                                   Arrays.asList("127.0.1.4", "127.0.1.2", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("2893766668419047554", "6836127902814411582"),
                                   Arrays.asList("127.0.1.1", "127.0.1.3", "127.0.1.4"));
        mockRangeToEndpointMap.put(Arrays.asList("-1524263151450829197", "-1179764051153390809"),
                                   Arrays.asList("127.0.1.6", "127.0.1.1", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("-3961581983597570892", "-1524263151450829197"),
                                   Arrays.asList("127.0.1.5", "127.0.1.6", "127.0.1.1"));
        mockRangeToEndpointMap.put(Arrays.asList("-1179764051153390809", "139771853904925224"),
                                   Arrays.asList("127.0.1.6", "127.0.1.1", "127.0.1.5"));
        mockRangeToEndpointMap.put(Arrays.asList("-5313593441555820686", "-3961581983597570892"),
                                   Arrays.asList("127.0.1.2", "127.0.1.5", "127.0.1.6"));
        mockRangeToEndpointMap.put(Arrays.asList("2268281908109175122", "2893766668419047554"),
                                   Arrays.asList("127.0.1.3", "127.0.1.1", "127.0.1.4"));
        mockRangeToEndpointMap.put(Arrays.asList("7788525348058239787", "-6226050461072679258"),
                                   Arrays.asList("127.0.1.4", "127.0.1.2", "127.0.1.5"));


        HashMap<String, String> mockEndpointToHostId = new HashMap<>();
        mockEndpointToHostId.put("127.0.1.1", "aa67b5d5-6bd0-49c3-8637-1a1e970f49dc");
        mockEndpointToHostId.put("127.0.1.2", "85109ca4-aa95-463a-b797-c38ad77e1f15");
        mockEndpointToHostId.put("127.0.1.3", "7cea4e9c-b8c5-43b2-bbaa-8b988f78aef7");
        mockEndpointToHostId.put("127.0.1.4", "f148711f-2cfa-4c02-94c0-54d7ebcfebfb");
        mockEndpointToHostId.put("127.0.1.5", "0b9359c0-9b87-4c20-bc5e-dab404a3c328");
        mockEndpointToHostId.put("127.0.1.6", "0871c188-b2c6-49ca-84da-e870bd023622");

        doReturn(mockEndpointToHostId).when(interactionSpy).getEndpointToHostIdMap();
        doReturn("127.0.1.3").when(interactionSpy).getLocalEndpoint();

        Assert.assertTrue(repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID));

        int numReady = 0;
        for (int i = 1; i <= 6; i++)
        {
            Mockito.when(interactionSpy.getLocalEndpoint()).thenReturn("127.0.1." + i);
            if (repairTask.amIReadyForPostTaskHook(TEST_REPAIR_ID))
                numReady += 1;
        }
        Assert.assertEquals(1, numReady);
    }

    @Test
    public void isPostRepairHookComplete()
    {
        // Nothing is finished
        TreeSet<RepairSequence> repSeq = new TreeSet<>();
        List<RepairMetadata> repairHistory = new LinkedList<>();

        doReturn(repairHistory).when(repairHookDaoSpy).getLocalRepairHookStatus(anyInt());
        doReturn(repSeq).when(repairSequenceDaoSpy).getRepairSequence(anyInt());


        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED).setNodeId("-9223372036854775808"));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setNodeId("0"));
        repSeq.add(getRepairSeq(3, TaskStatus.STARTED).setNodeId("6148914691236517202"));
        repairHistory.add(new RepairMetadata().setNodeId("-9223372036854775808").setStatus(TaskStatus.FINISHED));
        Assert.assertFalse(repairTask.isPostRepairHookCompleteOnCluster(TEST_REPAIR_ID));

        // Repair finished but hook not
        repSeq.clear();
        repSeq.add(getRepairSeq(1, TaskStatus.FINISHED).setNodeId("-9223372036854775808"));
        repSeq.add(getRepairSeq(2, TaskStatus.FINISHED).setNodeId("0"));
        repSeq.add(getRepairSeq(3, TaskStatus.FINISHED).setNodeId("6148914691236517202"));
        Assert.assertFalse(repairTask.isPostRepairHookCompleteOnCluster(TEST_REPAIR_ID));

        // Everything finished
        repairHistory.clear();
        repairHistory.add(new RepairMetadata().setNodeId("-9223372036854775808").setStatus(TaskStatus.FINISHED));
        repairHistory.add(new RepairMetadata().setNodeId("0").setStatus(TaskStatus.FINISHED));
        repairHistory.add(new RepairMetadata().setNodeId("6148914691236517202").setStatus(TaskStatus.FINISHED));
        Assert.assertTrue(repairTask.isPostRepairHookCompleteOnCluster(TEST_REPAIR_ID));
    }

    @Test
    public void getStuckRepairNodes()
    {
        int repairId = getRandomRepairId();
        repairTask.prepareForTaskOnNode(repairId, getRepairSeq(1, TaskStatus.STARTED));
        Optional<RepairSequence> rSeq = repairTask.getStuckSequence(repairId);
        Assert.assertFalse("There should not be any stuck repairs at this stage.", rSeq.isPresent());
        repairTask.publishHeartBeat(repairId, 1);
        DateTimeUtils.setCurrentMillisOffset(31 * 60 * 1000);
        Optional<RepairSequence> rSeq2 = repairTask.getStuckSequence(repairId);
        Assert.assertTrue(rSeq2.isPresent());
        Assert.assertEquals("Ok", rSeq2.get().getLastEvent().get("Status"));
    }

    // Internal utility methods
    private RepairSequence getRepairSeq(int i, TaskStatus status)
    {
        return new RepairSequence(TEST_CLUSTER_NAME, TEST_REPAIR_ID).setSeq(i).setStatus(status);
    }

    private String getKeyspace()
    {
        return "keyspace_" + seqNumber.getAndIncrement();
    }

    private String getTable()
    {
        return "table_" + seqNumber.getAndIncrement();
    }

    private void generateTestRepairHistory(List<RepairMetadata> repairHistory, TaskStatus... statuses)
    {
        String keyspace = getKeyspace();
        repairHistory.clear();
        for (TaskStatus status : statuses)
        {
            repairHistory.add(new RepairMetadata(TEST_CLUSTER_NAME, TEST_OTHER_NODE_ID, keyspace, getTable()).setStatus(status));
        }
    }

    private List<TableTaskConfig> getTablesForRepairExcludeSystem()
    {
        return repairTask.getTablesForRepair(TEST_REPAIR_ID, "default")
                         .stream()
                         .filter(tc -> !sysKs.contains(tc.getKeyspace())).collect(Collectors.toList());
    }
}