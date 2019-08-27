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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
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
    protected long lastMajorCompactionTime;

    /** Used to encapsulate a sorted run (aka "Level" of sstables)
     */
    private static class SortedRun
    {
        public final Set<SSTableReader> sstables;
        public final long sizeInBytes;
        public final double tokenRangeSize;

        private SortedRun(Set<SSTableReader> sstables, long sizeInBytes, double tokenRangeSize)
        {
            this.sstables = sstables;
            this.sizeInBytes = sizeInBytes;
            this.tokenRangeSize = tokenRangeSize;
        }
    }

    private final Set<SSTableReader> sstables = new HashSet<>();

    public TargetReadCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.targetReadOptions = new TargetReadCompactionStrategyOptions(options);
        this.targetSSTableSize = targetReadOptions.targetSSTableSize;
        this.lastMajorCompactionTime = 0;
    }

    private List<SSTableReader> findNewlyFlushedSSTables(Set<SSTableReader> candidates)
    {
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        List<SSTableReader> recentlyFlushed = candidates.stream()
                                                        .filter(s -> s.getSSTableLevel() == 0)
                                                        .sorted(SSTableReader.sizeComparator)
                                                        .collect(Collectors.toList());

        long size = recentlyFlushed.stream().mapToLong(SSTableReader::uncompressedLength).sum();

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
        // We never consider the freshly flushed sstables candidates for overlap compaction
        // they _must_ go through the cleaning process in findNewlyFlushedSSTables first
        Set<SSTableReader> overlapCandidates = compactionCandidates.stream()
                                                                   .filter(s -> s.getSSTableLevel() > 0)
                                                                   .collect(Collectors.toSet());

        SSTableIntervalTree tree = SSTableIntervalTree.build(overlapCandidates);

        List<SSTableReader> sortedByFirst = Lists.newArrayList(overlapCandidates);
        sortedByFirst.sort(Comparator.comparing(o -> o.first));

        PartitionPosition last = tree.min();
        List<Range<PartitionPosition>> coveringRanges = new ArrayList<>();

        for (SSTableReader sstable : sortedByFirst)
        {
            // Wait until we get past the last covering range
            if (sstable.last.compareTo(last) < 0)
                continue;

            List<SSTableReader> overlapping = View.sstablesInBounds(sstable.first, sstable.last, tree);
            PartitionPosition min = sstable.first;
            PartitionPosition max = sstable.last;
            for (SSTableReader overlap : overlapping)
            {
                if (overlap.first.compareTo(min) < 0)
                    min = overlap.first;
                if (overlap.last.compareTo(max) > 0)
                    max = overlap.last;
            }
            coveringRanges.add(new Range<>(min, max));
            last = max;
        }

        logger.debug("Found {} covering ranges", coveringRanges.size());

        // 1. Take covering ranges, which should mostly align with freshly flushed L1 sstables, and find
        //    overlapping levels (sorted runs) with those ranges. Find sorted runs in those ranges
        // 2. When we have more than targetOverlap sorted runs, try combining low density sstables
        // 3. Consider the urgency of a given
        //
        //   | | | | | | | |
        //   |     |    |     |
        //   |     |   |     |  |
        //
        List<Pair<List<SSTableReader>, Double>> candidateScores = new ArrayList<>();
        Set<SSTableReader> fullCompactionSStables = new HashSet<>();
        for (Range<PartitionPosition> coveringRange : coveringRanges)
        {
            Map<Integer, Set<SSTableReader>> sstablesByLevel = new HashMap<>();
            for (Range<PartitionPosition> unwrapped : coveringRange.unwrap())
            {
                List<SSTableReader> overlappingInRange = View.sstablesInBounds(unwrapped.left, unwrapped.right, tree);
                for (SSTableReader sstable : overlappingInRange)
                {
                    Set<SSTableReader> sstables = sstablesByLevel.get(sstable.getSSTableLevel());
                    if (sstables == null)
                        sstables = new HashSet<>();
                    sstables.add(sstable);

                    // TODO add major compaction here
                }
            }

            // If we don't have enough sorted ranges to even hit the target overlap skip bucketing
            if (sstablesByLevel.size() < targetReadOptions.targetOverlap)
                continue;

            List<Pair<SortedRun, Long>> runs = createSortedRunDensities(sstablesByLevel);

            List<List<SortedRun>> buckets = SizeTieredCompactionStrategy.getBuckets(
                runs,
                targetReadOptions.tierBucketHigh, targetReadOptions.tierBucketLow, targetReadOptions.minSSTableSize
            );

            // We want buckets which reduce overlap but relatively small in size
            int bucketsFound = 0;
            for (List<SortedRun> bucket : buckets)
            {
                if (bucket.size() < 2)
                    continue;
                bucketsFound += 1;
                candidateScores.add(Pair.create(
                    createCandidate(bucket, maxThreshold),
                    calculateScore(bucket, runs.size())
                ));
            }

            // Edge case where we can't find any density candidates but we're still over
            // maxOverlap so just find the two smallest runs and compact them
            if (bucketsFound == 0 && runs.size() > targetReadOptions.maxOverlap && runs.size() >= 2)
            {
                runs.sort(Comparator.comparing(r -> r.left.sizeInBytes));
                List<SortedRun> bucket = runs.subList(0, 1).stream()
                                             .map(p -> p.left)
                                             .collect(Collectors.toList());

                candidateScores.add(Pair.create(
                    createCandidate(bucket, maxThreshold),
                    calculateScore(bucket, runs.size())
                ));
            }
        }

        candidateScores.sort(Comparator.comparingDouble(s -> s.right));
        Collections.reverse(candidateScores);
        estimatedRemainingTasks = candidateScores.size();
        logger.trace("Found candidates {}", candidateScores);

        if (candidateScores.size() > 0)
            return candidateScores.get(0).left;


        /**
        // Level is a proxy for overwrite factor
        List<Pair<SSTableReader, Long>> overlappingByLevel = overlapping.stream()
                                                                        .map(s -> Pair.create(s, (long) s.getSSTableLevel()))
                                                                        .collect(Collectors.toList());

        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(overlappingByLevel,
                                                                                    targetReadOptions.tierBucketLow,
                                                                                    targetReadOptions.tierBucketHigh,
                                                                                    0);

        View.sstablesInBounds(PartitionPosition left, PartitionPosition right, SSTableIntervalTree intervalTree)
        tree.search()

        //
        Map<Range<Token>, Map<Long, Set<SSTableReader>>>

        SortedMap
        // { overlaps } -> { to compact, num to compact }
        Map<Set<SSTableReader>, Pair<List<SSTableReader>, Long>> targets = new HashMap<>();

        for (SSTableReader sstable : overlapCandidates)
        {
            Collection<SSTableReader> overlappingLive = cfs.getOverlappingLiveSSTables(Collections.singleton(sstable));
            // Only consider overlapping sstables which are candidates for compaction in the first place
            Set<SSTableReader> overlapping = ImmutableSet.copyOf(Sets.intersection(ImmutableSet.copyOf(overlappingLive), overlapCandidates));
            if (!targets.containsKey(overlapping) && overlapping.size() > targetReadOptions.targetOverlap)
            {


                long maxBucketedLevels = 0;
                int maxIndex = 0;
                for (int i = 0; i < buckets.size(); i++)
                {
                    long numLevels = numLevels(buckets.get(i));
                    if (numLevels > maxBucketedLevels)
                    {
                        maxBucketedLevels = numLevels;
                        maxIndex = i;
                    }
                }
                targets.put(overlapping, Pair.create(buckets.get(maxIndex), maxBucketedLevels));
            }
        }

        List<Map.Entry<Set<SSTableReader>, Pair<List<SSTableReader>, Long>>> sortedTargets;
        sortedTargets = targets.entrySet()
                               .stream()
                               // reverse value order
                               .sorted((c1, c2) -> c2.getValue().right.compareTo(c1.getValue().right))
                               .collect(Collectors.toList());

        Set<SSTableReader> result = new HashSet<>();
        int countOver = 0;
        int index = 0;
        int targetIndex = 0;
        for (Map.Entry<Set<SSTableReader>, Pair<List<SSTableReader>, Long>> target : sortedTargets)
        {
            if (target.getValue().left.size() > targetSSTableSize ||
                target.getKey().size() >= targetReadOptions.maxOverlap)
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
            for (SSTableReader sstable : sortedTargets.get(index).getKey())
            {
                result.add(sstable);
                if (result.size() >= maxThreshold)
                    break;
            }
        }

        if (result.size() > 0)
        {
            logger.debug("Choosing {} sstables of tiers {} for compaction, collapsing {} ",
                         result.size(), result.stream().mapToInt(SSTableReader::getSSTableLevel).toArray(),
                         sortedTargets.get(index).getValue());
            return new ArrayList<>(result);
        }

        **/
        return Collections.emptyList();

    }

    private static List<SSTableReader> createCandidate(List<SortedRun> bucket, int maxThreshHold)
    {
        bucket.sort(Comparator.comparing(b -> b.sizeInBytes));

        return bucket.stream()
                     .map(b -> b.sstables).flatMap(Set::stream)
                     .limit(maxThreshHold)
                     .collect(Collectors.toList());
    }
    /**
     * We care about buckets which
     * when overlap is higher than max these buckets should get more heavily weighted
     * when the number of sorted runs is large relative to the size, should score higher
     * let R = number of sorted runs in the bucket (overlap reduction)
     * let O = number of overlaps in the overlap range this bucket came from (overlap count)
     * let B = Normalized number of bytes in this bucket
     * let M = maximum number of overlaps
     *
     * higher scores are better
     * score = R * max((O - M), 1)
     *         -------------------
     *            B
     *
     */
    private double calculateScore(List<SortedRun> bucket, int numOverlaps)
    {
        double value = (bucket.size() * Math.max(targetReadOptions.maxOverlap - numOverlaps, 1));
        double normalizedBytes = bucket.stream()
                                     .mapToLong(sr -> Math.min(1, (long) (sr.sizeInBytes * sr.tokenRangeSize)))
                                     .sum();
        return value / normalizedBytes;
    }


    private static List<Pair<SortedRun, Long>> createSortedRunDensities(Map<Integer, Set<SSTableReader>> sstables)
    {
        List<Pair<SortedRun, Long>> sstableDensityPairs = new ArrayList<>(sstables.size());
        for (Set<SSTableReader> run : sstables.values())
        {
            long sizeInBytes = run.stream().mapToLong(SSTableReader::uncompressedLength).sum();
            double density = getRunDensity(run);
            long effectiveDensity = Math.min(1, (long) (sizeInBytes * density));
            sstableDensityPairs.add(Pair.create(new SortedRun(run, sizeInBytes, density), effectiveDensity));
        }
        return sstableDensityPairs;
    }

    // sstable density is roughly how many bytes are stored in the run divided by how large the range is
    // density ~= size_bytes * size_token_range
    // 100 mebibyte * 0.2 is twice as dense as
    // 100 mebibytes * 0.1
    private static double getRunDensity(Iterable<SSTableReader> run)
    {
        Token min = null, max = null;
        for (SSTableReader sstable : run)
        {
            if (min == null)
                min = sstable.first.getToken();
            if (max == null)
                max = sstable.last.getToken();
            if (sstable.first.getToken().compareTo(min) < 0)
                min = sstable.first.getToken();
            if (sstable.last.getToken().compareTo(max) > 0)
                max = sstable.last.getToken();

        }

        // TODO: is this actually needed?
        return new Range<>(min, max).unwrap().stream().mapToDouble(r -> r.left.size(r.right)).sum();
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
        int maxLevel = 0;
        int count = 0;
        for (SSTableReader sstable : sstables)
        {
            maxLevel = Math.max(maxLevel, sstable.descriptor.generation);
            count++;
        }
        if (count == 1)
            return maxLevel;
        else
            return maxLevel + 1;
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

        // Now we're in the levels, where we will pick tables that overlap enough and then rank them by density
        // tiering
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

            // This can only happen from newly flushed sstables
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

                logger.debug("Level zero compaction yielding {} sstables of size {}mb. Achieving compaction ratio of: {}",
                             totalSize / targetSize, targetSize / (1024 * 1024), ratio);
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
        long currentTime = System.currentTimeMillis();
        // hack to allow cancelling major compactions
        if (currentTime - lastMajorCompactionTime < 60)
        {
            logger.info("Cancelling major compaction due to recent toggle");
            lastMajorCompactionTime = 0;
        }
        else
        {
            logger.info(
            "TargetReadCompactionStrategy does not support blocking compactions, informing background" +
            "toggling full compactions to begin now"
            );
            lastMajorCompactionTime = currentTime;
        }
        return Collections.emptyList();
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
