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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class TargetReadCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TargetReadCompactionStrategy.class);

    protected TargetReadCompactionStrategyOptions targetReadOptions;
    protected volatile int estimatedRemainingTasks;
    protected long targetSSTableSize;
    protected int targetRange = 0;
    protected List<Range<Token>> ownedRanges;


    private final Set<SSTableReader> sstables = new HashSet<>();

    public TargetReadCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.targetReadOptions = new TargetReadCompactionStrategyOptions(options);
        this.targetSSTableSize = targetReadOptions.targetSSTableSize;
    }

    private List<SSTableReader> findNewlyFlushedSSTables(Iterable<SSTableReader> candidates)
    {
        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        /*
        1. If we have minThreshold sstables under the target size, always compact them into the overlapping set
        2. Otherwise recalculate targetSize based on max sstables and try to keep us under the count of sstables
        3. If nothing at this point we take a keysample and chose the sstables with the most overlaps to the target
           range, which increases by one each time
         */
        Set<SSTableReader> sizeCandidates = Sets.newHashSet(candidates);

        if (sizeCandidates.size() > targetReadOptions.maxSSTableCount)
        {
            long totalSize = sizeCandidates.stream().mapToLong(SSTableReader::bytesOnDisk).sum();
            targetSSTableSize = (totalSize / targetReadOptions.maxSSTableCount);
        }

        List<SSTableReader> tooSmall = sizeCandidates.stream()
                                                     .filter(s -> s.getSSTableLevel() == 0)
                                                     //.filter(s -> s.onDiskLength() < targetSSTableSize)
                                                     .sorted(SSTableReader.sstableComparator)
                                                     .collect(Collectors.toList());

        long size = tooSmall.stream().mapToLong(SSTable::bytesOnDisk).sum();

        boolean sizeEligible = (size > targetSSTableSize) || tooSmall.size() >= maxThreshold;

        if (tooSmall.size() >= minThreshold && sizeEligible)
            return tooSmall.stream().limit(maxThreshold).collect(Collectors.toList());

        return Collections.emptyList();
    }

    private List<SSTableReader> findOverlappingSSTables(Iterable<SSTableReader> candidates)
    {
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Set<SSTableReader> overlapTargets = new TreeSet<>(SSTableReader.sstableComparator);
        for (SSTableReader sstable : candidates)
        {
            if (sstable.getSSTableLevel() > 0)
                overlapTargets.add(sstable);
        }

        SSTableIntervalTree tree = SSTableIntervalTree.build(overlapTargets);

        Map<Set<SSTableReader>, Long> targets = new HashMap<>();

        for (SSTableReader sstable : overlapTargets)
        {
            Interval<PartitionPosition, SSTableReader> interval = Interval.create(sstable.first, sstable.last, sstable);
            Set<SSTableReader> overlapping = ImmutableSet.copyOf(tree.search(interval));
            if (overlapping.size() > targetReadOptions.targetOverlap && !targets.containsKey(overlapping))
            {
                targets.putIfAbsent(overlapping, getBytesReclaimed(overlapping));
            }
        }

        // Greedily take the worst offenders and compact them.
        Set<SSTableReader> result = new HashSet<>();

        List<Map.Entry<Set<SSTableReader>, Long>> sortedTargets = targets.entrySet()
                                                                         .stream()
                                                                         // reverse value order
                                                                         .sorted((c1, c2) -> c2.getValue().compareTo(c1.getValue()))
                                                                         .collect(Collectors.toList());


        int countOver = 0;
        int index = 0;
        int targetIndex = 0;
        for (Map.Entry<Set<SSTableReader>, Long> target : sortedTargets)
        {
            // TODO: why the arbitrary * 2 ... because otherwise small sstables never get compacted
            if (target.getValue() > targetSSTableSize || target.getKey().size() >= targetReadOptions.targetOverlap * 2)
            {
                if (index == 0)
                    index = targetIndex;
                countOver += 1;
            }
            targetIndex += 1;
        }


        if (countOver > 0)
        {
            estimatedRemainingTasks = countOver;
            for (SSTableReader sstable : sortedTargets.get(0).getKey())
            {
                result.add(sstable);
                if (result.size() >= maxThreshold)
                    break;
            }
        }

        if (result.size() > 0)
        {
            long components = 0;
            for (SSTableReader reader : result)
            {
                long x = SSTableReader.getApproximateKeyCount(Collections.singleton(reader));
                logger.info("reader {} has cardinality {}", reader, x);
                components += x;
            }

            long totalCardinality = SSTableReader.getApproximateKeyCount(result);

            logger.info("Compacted has cardinality {} / {} possible -> {} percent", totalCardinality, components, (double) totalCardinality / (double) components);

            return new ArrayList<>(result);
        }
        return Collections.emptyList();
    }


    private List<SSTableReader> findTombstoneEligibleSSTables(int gcBefore, Iterable<SSTableReader> candidates)
    {
        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.max(sstablesWithTombstones, SSTableReader.sizeComparator));
    }

    private static int getLevel(Iterable<SSTableReader> sstables)
    {
        int maxLevel = 0;
        int count = 0;
        for (SSTableReader sstable : sstables)
        {
            maxLevel = Math.max(maxLevel, sstable.getSSTableLevel());
            count++;
        }

        if (count == 1)
            return maxLevel;
        else
            return maxLevel + 1;
    }

    private static long getBytesReclaimed(Iterable<SSTableReader> sstables)
    {
        long totalCount = 0;
        long totalSize = 0;
        for (SSTableReader sstable: sstables)
        {
            totalCount += SSTableReader.getApproximateKeyCount(Collections.singletonList((sstable)));
            totalSize += sstable.bytesOnDisk();
        }
        long estimatedCombinedCount = SSTableReader.getApproximateKeyCount(sstables);
        double ratio = (double) estimatedCombinedCount / (double) totalCount;
        return (long) (ratio * totalSize);
    }

    private Pair<List<SSTableReader>, Integer> getSSTablesForCompaction(int gcBefore)
    {
        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        List<SSTableReader> sstablesToCompact = findNewlyFlushedSSTables(candidates);
        if (!sstablesToCompact.isEmpty())
            return Pair.create(sstablesToCompact, 1);

        sstablesToCompact = findOverlappingSSTables(candidates);
        if (!sstablesToCompact.isEmpty())
            return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));

        sstablesToCompact = findTombstoneEligibleSSTables(gcBefore, candidates);

        return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));
    }

    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        while (true)
        {
            Pair<List<SSTableReader>, Integer> compactionTarget = getSSTablesForCompaction(gcBefore);
            List<SSTableReader> sstablesToCompact = compactionTarget.left;
            int level = compactionTarget.right;
            long targetSize = targetSSTableSize;

            if (sstablesToCompact.isEmpty())
                return null;

            // This can only happen from small sstables
            if (level == 1) {
                long totalCount = 0;
                long totalSize = 0;
                for (SSTableReader sstable: sstablesToCompact)
                {
                    totalCount += SSTableReader.getApproximateKeyCount(Collections.singletonList((sstable)));
                    totalSize += sstable.bytesOnDisk();
                }
                long estimatedCombinedCount = SSTableReader.getApproximateKeyCount(sstablesToCompact);

                double ratio = (double) estimatedCombinedCount / (double) totalCount;
                targetSize = Math.max(4096, Math.round(((totalSize * ratio) / cfs.getMaximumCompactionThreshold())));

                logger.debug("Level zero compaction yielding ratio of {} and size {} bytes", ratio, targetSize);
            }

            LifecycleTransaction transaction = cfs.getTracker().tryModify(sstablesToCompact, OperationType.COMPACTION);
            if (transaction != null)
                return new LeveledCompactionTask(cfs, transaction, level,
                                                 gcBefore, targetSize, false);
        }
    }

    @SuppressWarnings("resource")
    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Arrays.asList(new LeveledCompactionTask(cfs, txn, getLevel(filteredSSTables),
                                                       gcBefore, targetSSTableSize, false));
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new LeveledCompactionTask(cfs, transaction, getLevel(sstables),
                                         gcBefore, targetSSTableSize, false).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TargetReadCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    @Override
    public boolean shouldDefragment()
    {
        return true;
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    protected Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    public String toString()
    {
        return String.format("TargetReadCompactionStrategy[%s/%s:%s mb]",
                             cfs.getMinimumCompactionThreshold(),
                             cfs.getMaximumCompactionThreshold(), targetSSTableSize / (1024 * 1024));
    }
}
