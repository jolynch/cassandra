package org.apache.cassandra.repair.scheduler.conn;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.scheduler.EmbeddedUnitTestBase;
import org.apache.cassandra.repair.scheduler.entity.RepairSplitStrategy;
import org.apache.cassandra.repair.scheduler.entity.TableRepairRangeContext;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import javax.management.Notification;
import javax.management.NotificationListener;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class CassandraInteractionTest extends EmbeddedUnitTestBase
{
    CassandraInteraction interaction = null;

    class FakeRepairNotificationListener implements NotificationListener {
        final Condition condition = new SimpleCondition();

        @Override
        public void handleNotification(Notification notification, Object handback)
        {
            Map<String, Integer> progress = (Map<String, Integer>) notification.getUserData();
            ProgressEventType event = ProgressEventType.values()[progress.get("type")];
            if (event == ProgressEventType.COMPLETE)
                condition.signalAll();
        }

        boolean await(int timeout, TimeUnit unit) throws InterruptedException
        {
            return condition.await(timeout, unit);
        }
    }

    @Before
    public void setupCassInteration() throws IOException
    {
        if (interaction != null)
            return;

        interaction = getContext().getCassInteraction();
        //Setting Partitioner to one of the supported partitioners since {@link EmbeddedCassandraService} uses ByteOrderedParttioner
        interaction.setPartitioner(FBUtilities.newPartitioner(Murmur3Partitioner.class.getName()));
        interaction.connectAsync();
        assertTrue(interaction.connectSync());
        assertTrue(interaction.isConnectionAlive(false));
        loadDataset(10);
    }

    @Test
    public void testAddRemoveNotificationListener()
    {
        FakeRepairNotificationListener listener = new FakeRepairNotificationListener();
        interaction.addRepairNotificationListener(listener);
        assertEquals(1, interaction.getOutstandingRepairNotificationListeners().size());
        assertTrue(interaction.getOutstandingRepairNotificationListeners().contains(listener));
        interaction.removeRepairNotificationListener(listener);
        assertEquals(0, interaction.getOutstandingRepairNotificationListeners().size());
    }

    @Test
    public void testGetLocalHostId()
    {
        assertNotNull(interaction.getLocalHostId());
    }

    @Test
    public void testGetLocalEndpoint()
    {
        assertNotNull(interaction.getLocalEndpoint());
    }

    @Test
    public void testGetLocalLoadString()
    {
        assertNotNull(interaction.getLocalLoadString());
    }

    @Test
    public void testGetClusterName()
    {
        assertTrue(interaction.getClusterName().length() > 0);
    }

    @Test
    public void testGetEndpointToHostIdMap()
    {
        assertEquals(interaction.getEndpointToHostIdMap().size(), 1);
    }

    @Test
    public void testGetRangeToEndpointMap()
    {
        assertEquals(interaction.getRangeToEndpointMap("system_distributed").size(), 1);
    }

    @Test
    public void testGetSimpleStates()
    {
        assertEquals(interaction.getSimpleStates().size(), 1);
    }

    @Test
    public void testIsCassandraHealthy()
    {
        assertTrue(interaction.isCassandraHealthy());
    }

    @Test
    public void testIsRunning()
    {
        // With RF=1 we can't really test this because we need an actual running repair to properly test this.
        // Instead we just check for basic things like the mbean name being wrong. Tested with e2e tests instead.
        assertFalse(interaction.isRepairRunning(true));
    }

    @Test
    public void cancelAllRepairs()
    {
        // With RF=1 we can't really test this because we need an actual running repair to properly test this.
        // Instead we just check for basic compilation level issues. Tested with e2e tests instead
        interaction.cancelAllRepairs();
    }

    @Test
    public void triggerCompaction()
    {
        interaction.triggerCompaction("system_distributed", "repair_history");
    }

    @Test
    public void triggerCleanup()
    {
        interaction.triggerCleanup(1, "system_distributed", "repair_history");
    }

    @Test
    public void triggerFlush()
    {
        interaction.triggerFlush("system_distributed", "repair_history");
    }

    @Test
    public void testGetPartitions()
    {
        TableRepairRangeContext rangeContext = interaction.getTableRepairRangeContext("test_repair", "subrange_test");

        for (Range<Token> range: rangeContext.primaryRanges)
        {
            List<Long> partitionsAndSize = ((Cass4xInteraction) interaction).getPartitionCountAndSize(rangeContext, range);
            assertTrue(partitionsAndSize.get(0) >= 1);
            assertTrue(partitionsAndSize.get(1) >= 0);
        }
    }

    @Test
    public void testGetTokenRanges()
    {
        TableRepairRangeContext rangeContext = interaction.getTableRepairRangeContext("test_repair", "subrange_test");
        Map<Range<Token>, List<String>> ranges = interaction.getRangeToEndpointMap("test_repair");

        for (Range<Token> primaryRange : rangeContext.primaryRanges)
        {
            System.out.println(primaryRange + ": " + ranges.get(primaryRange));
        }
        System.out.println(rangeContext.primaryRanges.size());
    }

    @Test
    public void testTokenRangeSplitsPartitions()
    {
        loadDataset(50000);

        TableRepairRangeContext rangeContext = interaction.getTableRepairRangeContext("test_repair", "subrange_test");

        for (Range<Token> range : rangeContext.primaryRanges)
        {
            List<Long> partitionsAndSize = ((Cass4xInteraction) interaction).getPartitionCountAndSize(rangeContext, range);
            Long splitCount = Math.max(1, partitionsAndSize.get(0) / 10);

            RepairSplitStrategy partitionStrategy = new RepairSplitStrategy(splitCount.toString());
            List<Range<Token>> splits = interaction.getTokenRangeSplits(rangeContext, range, partitionStrategy);

            assertEquals((double) splits.size(), 10.0, 1.0);
            assertEquals(range.left, splits.get(0).left);
            assertEquals(range.right, splits.get(splits.size() - 1).right);

            Range<Token> prev = splits.get(0);
            for (int i = 1; i < splits.size(); i++)
            {
                assertEquals(prev.right, splits.get(i).left);
                prev = splits.get(i);
            }
        }
    }

    @Test
    public void testTokenRangeSplitsSize()
    {
        loadDataset(50000);

        TableRepairRangeContext rangeContext = interaction.getTableRepairRangeContext("test_repair", "subrange_test");

        for (Range<Token> range : rangeContext.primaryRanges)
        {
            List<Long> partitionsAndSize = ((Cass4xInteraction) interaction).getPartitionCountAndSize(rangeContext, range);
            Long splitSize = Math.max(1, (partitionsAndSize.get(1) / 1024) / 4);


            RepairSplitStrategy partitionStrategy = new RepairSplitStrategy(splitSize.toString() + "_kb");
            List<Range<Token>> splits = interaction.getTokenRangeSplits(rangeContext, range, partitionStrategy);

            // vnode tests ... not very useful
            if (splitSize * 4 < (partitionsAndSize.get(1) / 1024))
            {
                assertEquals(4.0, (double) splits.size(), 1.0);
            }
            assertEquals(range.left, splits.get(0).left);
            assertEquals(range.right, splits.get(splits.size() - 1).right);

            Range<Token> prev = splits.get(0);
            for (int i = 1; i < splits.size(); i++)
            {
                assertEquals(prev.right, splits.get(i).left);
                prev = splits.get(i);
            }
        }
    }
}