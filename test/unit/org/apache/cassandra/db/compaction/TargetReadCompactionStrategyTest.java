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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import junit.framework.TestCase;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class TargetReadCompactionStrategyTest extends TestCase
{
    public void testFindWorkRanges()
    {
        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        List<PartitionPosition> positions = new ArrayList<>();
        for (int i = 0; i < 21537; i ++)
        {
            DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("key_" + i));
            positions.add(dk);
        }
        Collections.sort(positions);

        for (int i = 0; i < positions.size() - 1; i+=2)
        {
            ranges.add(AbstractBounds.bounds(positions.get(i), true, positions.get(i+1), true));
        }

        double intervalSize = TargetReadCompactionStrategy.calculateIntervalSize(ranges);
        assertEquals(intervalSize, 0.5, 0.01);
        double treeIntervalSize = ranges.get(0).left.getToken().size(ranges.get(ranges.size() - 1).right.getToken());

        List<AbstractBounds<PartitionPosition>> splitRanges = TargetReadCompactionStrategy.findWorkRanges(ranges, treeIntervalSize / 16);

        assertEquals(16, splitRanges.size());

        List<Bounds<PartitionPosition>> results = ranges.stream()
                                                        .map(r -> new Bounds<>(r.left, r.right))
                                                        .collect(Collectors.toList());
        double sumRange = 0.0;
        for (AbstractBounds<PartitionPosition> range : splitRanges)
        {
            sumRange += range.left.getToken().size(range.right.getToken());
        }
        assertEquals(results.get(0).left, ranges.get(0).left);
        assertEquals(results.get(results.size() - 1).right, ranges.get(ranges.size() - 1).right);
        assertEquals(sumRange, treeIntervalSize, 0.01);

        List<Interval<PartitionPosition, AbstractBounds<PartitionPosition>>> intervals;
        intervals = ranges.stream().map(ab -> Interval.create(ab.left, ab.right, ab)).collect(Collectors.toList());

        IntervalTree<PartitionPosition, AbstractBounds<PartitionPosition>, ?> tree = IntervalTree.build(intervals);
        Set<AbstractBounds<PartitionPosition>> lookups = new HashSet<>();
        Collections.shuffle(ranges);
        // We should be able to query the splits and get all sstables back
        for (AbstractBounds<PartitionPosition> split: splitRanges)
        {
            lookups.addAll(tree.search(Interval.create(split.left, split.right)));
        }

        assertTrue(lookups.containsAll(ranges));
    }

}