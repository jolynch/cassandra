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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class TargetReadCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TargetReadCompactionStrategy.class);

    protected TargetReadCompactionStrategyOptions targetReadOptions;
    protected volatile int estimatedRemainingTasks;
    protected long targetSSTableSize;

    private final Set<SSTableReader> sstables = new HashSet<>();

    public TargetReadCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.targetReadOptions = new TargetReadCompactionStrategyOptions(options);
        this.targetSSTableSize = targetReadOptions.targetSSTableSize;
    }

    private List<SSTableReader> findNewlyFlushedSSTables(Set<SSTableReader> candidates)
    {
        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        List<SSTableReader> recentlyFlushed = candidates.stream()
                                                        .filter(s -> s.getSSTableLevel() == 0)
                                                        .sorted(SSTableReader.sizeComparator)
                                                        .collect(Collectors.toList());

        long size = recentlyFlushed.stream().mapToLong(SSTable::bytesOnDisk).sum();

        // Consider flushed sstables eligible for entry into the "levels" if we have enough data to write out a single
        // targetSSTableSize sstable, or we have enough sstables (assuming they all span the whole token range) such
        // that we may exceed the maxOverlap
        boolean sizeEligible = (size > targetSSTableSize) ||
                               ((recentlyFlushed.size() + targetReadOptions.targetOverlap) >= targetReadOptions.maxOverlap);

        if (recentlyFlushed.size() >= minThreshold && sizeEligible)
            return recentlyFlushed.stream().limit(maxThreshold).collect(Collectors.toList());

        return Collections.emptyList();
    }

    private List<SSTableReader> findSmallSSTables(Set<SSTableReader> sizeCandidates)
    {
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        if (sizeCandidates.size() > targetReadOptions.maxSSTableCount)
        {
            long totalSize = sizeCandidates.stream().mapToLong(SSTableReader::onDiskLength).sum();
            targetSSTableSize = Math.max(targetReadOptions.targetSSTableSize,
                                         totalSize / targetReadOptions.maxSSTableCount);
        }

        // Level 0 is special because it's the "flush" zone, and Level 1 is special because it's the the
        // "lots of small sstables" zone.
        Map<Integer, List<SSTableReader>> tooSmall = sizeCandidates.stream()
                                                                   .filter(s -> s.getSSTableLevel() > 1)
                                                                   .filter(s -> s.onDiskLength() < targetSSTableSize)
                                                                   .sorted(SSTableReader.sizeComparator)
                                                                   .collect(Collectors.groupingBy(SSTableReader::getSSTableLevel));
        List<SSTableReader> result = Collections.emptyList();
        for (Map.Entry<Integer, List<SSTableReader>> entry: tooSmall.entrySet())
        {
            if (entry.getValue().size() > result.size())
                result = entry.getValue();
        }

        return result.stream().limit(maxThreshold).collect(Collectors.toList());
    }

    private List<SSTableReader> findOverlappingSSTables(Set<SSTableReader> compactionCandidates)
    {
        int maxThreshold = cfs.getMaximumCompactionThreshold();
        Map<Set<SSTableReader>, Long> targets = new HashMap<>();

        for (SSTableReader sstable : compactionCandidates)
        {
            Collection<SSTableReader> overlappingLive = cfs.getOverlappingLiveSSTables(Collections.singleton(sstable));
            Set<SSTableReader> overlapping = ImmutableSet.copyOf(Sets.intersection(ImmutableSet.copyOf(overlappingLive), compactionCandidates));
            if (!targets.containsKey(overlapping) && overlapping.size() > targetReadOptions.targetOverlap)
            {
                targets.put(overlapping, getBytesReclaimed(overlapping));
            }
        }

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
            if (target.getValue() > targetSSTableSize || target.getKey().size() >= targetReadOptions.maxOverlap)
            {
                if (index == 0)
                    index = targetIndex;
                countOver += 1;
            }
            targetIndex += 1;
        }


        if (countOver > 0)
        {
            logger.info("Choosing sstables {} for compaction, will get {} bytes back", sortedTargets.get(index).getKey(), sortedTargets.get(index).getValue());
            estimatedRemainingTasks = countOver;
            for (SSTableReader sstable : sortedTargets.get(index).getKey())
            {
                result.add(sstable);
                if (result.size() >= maxThreshold)
                    break;
            }
        }

        if (result.size() > 0)
            return new ArrayList<>(result);

        return Collections.emptyList();
    }


    private List<SSTableReader> findTombstoneEligibleSSTables(int gcBefore, Set<SSTableReader> candidates)
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
        int maxGen = 0;
        for (SSTableReader sstable : sstables)
            maxGen = Math.max(maxGen, sstable.descriptor.generation);
        return maxGen;
    }

    private static long getBytesReclaimed(Set<SSTableReader> sstables)
    {
        long totalSize = sstables.stream().mapToLong(SSTableReader::onDiskLength).sum();
        double gain = SSTableReader.estimateCompactionGain(sstables);
        return (long) (totalSize * (1.0 - Math.max(1.0, gain)));
    }

    private Pair<List<SSTableReader>, Integer> getSSTablesForCompaction(int gcBefore)
    {
        Set<SSTableReader> candidatesSet = Sets.newHashSet(filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains)));

        // Handle freshly flushed data first, always try to get that into the levels if possible
        List<SSTableReader> sstablesToCompact = findNewlyFlushedSSTables(candidatesSet);
        if (!sstablesToCompact.isEmpty())
            return Pair.create(sstablesToCompact, 1);

        // Now we're in the levels, where we will pick tables that overlap enough and then rank them by
        // estimated cardinality reduction.
        sstablesToCompact = findOverlappingSSTables(candidatesSet);
        if (!sstablesToCompact.isEmpty())
            return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));

        // Handles re-writing sstables if they've gotten too small
        sstablesToCompact = findSmallSSTables(candidatesSet);
        if (!sstablesToCompact.isEmpty())
            return Pair.create(sstablesToCompact, sstablesToCompact.get(0).getSSTableLevel());

        // If we get here then check if tombstone compaction is available and do that
        sstablesToCompact = findTombstoneEligibleSSTables(gcBefore, candidatesSet);
        if (!sstablesToCompact.isEmpty())
            return Pair.create(sstablesToCompact, sstablesToCompact.get(0).getSSTableLevel());

        return Pair.create(sstablesToCompact, 0);
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
