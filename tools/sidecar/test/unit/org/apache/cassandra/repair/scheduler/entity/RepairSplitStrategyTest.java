package org.apache.cassandra.repair.scheduler.entity;

import org.junit.Test;

import static org.junit.Assert.*;

public class RepairSplitStrategyTest
{
    @Test
    public void testAllValues()
    {
        RepairSplitStrategy strategy = new RepairSplitStrategy("1");
        assertEquals(RepairSplitStrategy.Strategy.PARTITION, strategy.getStrategy());
        assertEquals(1, strategy.getValue());
        assertFalse(strategy.isDryRun());

        strategy = new RepairSplitStrategy("1_dry_run");
        assertEquals(RepairSplitStrategy.Strategy.PARTITION_DRY_RUN, strategy.getStrategy());
        assertEquals(1, strategy.getValue());
        assertTrue(strategy.isDryRun());

        strategy = new RepairSplitStrategy("3_kb");
        assertEquals(RepairSplitStrategy.Strategy.SIZE, strategy.getStrategy());
        assertEquals(3, strategy.getValue());
        assertFalse(strategy.isDryRun());

        strategy = new RepairSplitStrategy("4_kb_dry_run");
        assertEquals(RepairSplitStrategy.Strategy.SIZE_DRY_RUN, strategy.getStrategy());
        assertEquals(4, strategy.getValue());
        assertTrue(strategy.isDryRun());

        strategy = new RepairSplitStrategy("adaptive");
        assertEquals(RepairSplitStrategy.Strategy.ADAPTIVE, strategy.getStrategy());
        assertFalse(strategy.isDryRun());

        strategy = new RepairSplitStrategy("adaptive_dry_run");
        assertEquals(RepairSplitStrategy.Strategy.ADAPTIVE_DRY_RUN, strategy.getStrategy());
        assertTrue(strategy.isDryRun());

        strategy = new RepairSplitStrategy("nonsense");
        assertEquals(RepairSplitStrategy.Strategy.DISABLED, strategy.getStrategy());
        assertFalse(strategy.isDryRun());

        strategy = new RepairSplitStrategy("disabled");
        assertEquals(RepairSplitStrategy.Strategy.DISABLED, strategy.getStrategy());
        assertFalse(strategy.isDryRun());
    }
}