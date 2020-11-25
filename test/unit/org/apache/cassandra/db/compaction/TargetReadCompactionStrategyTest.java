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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import junit.framework.TestCase;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.TargetReadCompactionStrategy.SortedRun;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

import static org.apache.cassandra.db.compaction.TargetReadCompactionStrategy.calculateScore;

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

    private SortedRun createRuns(long size, double keySize)
    {
        List<DecoratedKey> keys = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i ++)
        {
            DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes("key_" + i));
            keys.add(dk);
        }
        Collections.sort(keys);

        return new SortedRun(size * 2, size,
                                                          keys.get(0), keys.get((int)keySize * (keys.size() - 1)),
                                                          0);
    }

    public void testScores()
    {
        SortedRun run1 = createRuns(1024 * 1024 * 10, 1.0);
        SortedRun run2 = createRuns(1024 * 1024 * 10, 1.0);
        SortedRun run3 = createRuns(1024 * 1024 * 10, 1.0);
        SortedRun run4 = createRuns(1024 * 1024 * 10, 1.0);
        List<SortedRun> sparse2 = Arrays.asList(run1, run2);
        List<SortedRun> sparse4 = Arrays.asList(run1, run2, run3, run4);

        SortedRun mediumRun1 = createRuns(1024 * 1024 * 25, 1.0);
        SortedRun mediumRun2 = createRuns(1024 * 1024 * 25, 1.0);
        List<SortedRun> medium2 = Arrays.asList(mediumRun1, mediumRun2);


        SortedRun denseRun1 = createRuns(1024 * 1024 * 1000, 1.0);
        SortedRun denseRun2 = createRuns(1024 * 1024 * 1000, 1.0);
        SortedRun denseRun3 = createRuns(1024 * 1024 * 1000, 1.0);
        List<SortedRun> dense2 = Arrays.asList(denseRun1, denseRun2);
        List<SortedRun> dense3 = Arrays.asList(denseRun1, denseRun2, denseRun3);

        double sparseScore = calculateScore(sparse2, 2, v -> true, 10);
        double denseScore = calculateScore(dense2, 2, v -> true, 10);

        assertTrue("Sparse runs should be preferred", sparseScore > denseScore);

        denseScore = calculateScore(dense3, 1, v -> true, 10);
        assertTrue("Sparse runs should be preferred", sparseScore > denseScore);


        sparseScore = calculateScore(sparse4, 2, v -> true, 10);
        denseScore = calculateScore(medium2, 2, v -> true, 10);
        assertTrue("4 small sparse runs should be preferred to 2 medium ones", sparseScore > denseScore);

        sparseScore = calculateScore(sparse4, 2, v -> true, 10);
        denseScore = calculateScore(medium2, 20, v -> true, 10);
        assertTrue("Work in highly overlapping regions should be preferred", denseScore > sparseScore);

        run1 = createRuns(1024 * 1024 * 1000, 1.0);
        run2 = createRuns(1024 * 1024 * 1000, 1.0);
        denseRun1 = createRuns(1024 * 1024 * 1000, 0.5);
        denseRun2 = createRuns(1024 * 1024 * 1000, 0.5);
        sparseScore = calculateScore( Arrays.asList(run1, run2), 2, v -> true, 10);
        denseScore = calculateScore(Arrays.asList(denseRun1, denseRun2), 2, v -> true, 10);
        assertTrue("Sparse runs should be preferred", sparseScore > denseScore);
    }

}