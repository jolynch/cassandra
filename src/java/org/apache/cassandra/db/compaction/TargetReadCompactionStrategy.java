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
        // that we may exceed the maxReadPerRead
        boolean sizeEligible = (size > targetSSTableSize) ||
                               ((recentlyFlushed.size() + targetReadOptions.targetReadPerRead) >= targetReadOptions.maxReadPerRead);

        if (recentlyFlushed.size() >= minThreshold && sizeEligible)
            return recentlyFlushed.stream().limit(maxThreshold).collect(Collectors.toList());

        return Collections.emptyList();
    }

    private List<SSTableReader> findSmallSSTables(Set<SSTableReader> sizeCandidates)
    {
        final int maxThreshold = cfs.getMaximumCompactionThreshold();

        // We don't want to re-write within a reasonable period of the last re-write
        final long now = System.currentTimeMillis();
        final long min = tombstoneCompactionInterval * 1000;
        Map<Integer, List<SSTableReader>> tooSmall = sizeCandidates.stream()
                                                                   .filter(s -> now > (s.getCreationTimeFor(Component.DATA) + min))
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
        if (overlapCandidates.isEmpty()) return Collections.emptyList();

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
            if (overlapping.size() <= 1)
            {
                last = sstable.last;
                logger.trace("Skipping single overlapping range covered by sstable " + sstable.descriptor.generation);
                continue;
            }

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

        logger.trace("TRCS found {} covering ranges", coveringRanges.size());

        // 1. Take covering ranges, which should mostly align with freshly normalized SSTables, and
        //    find overlapping levels (sorted runs) with those ranges.
        // 2. When we have more than targetReadPerRead sorted runs, try combining low density sstables
        //    giving us our biggest bang for our compaction buck
        //
        //   | | | | | | | | | | | |      Level=22 (10 GiB)
        //   |     |     |   |     |      Level=23 (2 GiB)
        //   |     |      |    |   |      Level=24 (2 GiB)
        //
        //   1 2 3 4 5 6 7 8 9 X Y Z
        //
        //   This has overlapping ranges (1, 4], (4, 7.5], (7.5, X] and (X, Z]
        //   We interested in compacting range (1, 4] and (4, 8] between Level=23
        //   and Level=24 because they have similar density and would yield read
        //   per read reduction while costing little in compaction.
        List<Pair<List<SSTableReader>, Double>> candidateScores = new ArrayList<>();
        Set<SSTableReader> fullCompactionSStables = new HashSet<>();
        for (int i = 0; i < coveringRanges.size(); i++)
        {
            Range<PartitionPosition> coveringRange = coveringRanges.get(i);
            Map<Integer, Set<SSTableReader>> sstablesByLevel = new HashMap<>();
            for (Range<PartitionPosition> unwrapped : coveringRange.unwrap())
            {
                List<SSTableReader> overlappingInRange = View.sstablesInBounds(unwrapped.left, unwrapped.right, tree);
                for (SSTableReader sstable : overlappingInRange)
                {
                    Set<SSTableReader> sstables = sstablesByLevel.computeIfAbsent(sstable.getSSTableLevel(), k -> new HashSet<>());
                    sstables.add(sstable);
                    // Major compaction has been signaled
                    if (sstable.getCreationTimeFor(Component.DATA) < lastMajorCompactionTime) {
                        fullCompactionSStables.add(sstable);
                    }
                }
                // We have a full compaction going on, just yield the qualifying sstables from ranges
                // in their entirety
                // Intentionally do this with smaller pieces of the data so that we don't need
                // 2x additional space to achieve this (should need about ~1/64 the space)
                if (fullCompactionSStables.size() > 1) {
                    logger.debug("TRCS found full compaction candidates {}", fullCompactionSStables);
                    estimatedRemainingTasks = coveringRanges.size() - i;
                    return new ArrayList<>(fullCompactionSStables);
                }
            }

            // If we don't have enough sorted ranges to even hit the target read per read skip bucketing
            if (sstablesByLevel.size() < targetReadOptions.targetReadPerRead)
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
            // maxReadPerRead so just find the two smallest runs and compact them
            if (bucketsFound == 0 && runs.size() > targetReadOptions.maxReadPerRead && runs.size() >= 2)
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

        Comparator<Pair<List<SSTableReader>, Double>> c = Comparator.<Pair<List<SSTableReader>, Double>>
                                                                    comparingDouble(s -> s.right).reversed();
        candidateScores.sort(c);
        estimatedRemainingTasks = candidateScores.size();

        if (candidateScores.size() > 0)
        {
            logger.debug("TRCS found candidates {}", candidateScores);
            return candidateScores.get(0).left;
        } else {
            logger.trace("TRCS yielded no candidates");
            return Collections.emptyList();
        }
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
     * We care about buckets which will reduce read work in ranges that are highly overlapping
     *
     * <pre>
     * let R = number of sorted runs in the bucket (overlap reduction)
     * let O = number of overlaps in the overlap range this bucket came from (overlap count)
     * let B = Normalized number of bytes in this bucket
     * let M = maximum number of reads per read
     *
     * So we <i>increase</i> ranges scores when:
     *     <ul>
     *         <li> R / B: the number of sorted runs in the range is large relative to the size </li>
     *         <li> max((O - M), 1): the number of overlaps in the covering range that exceed the
     *         max read per read target. Note this is almost always 1 </li>
     *     </ul>
     *
     * score = R * max((O - M), 1)
     *         -------------------
     *            B
     * score ~= R / B
     * </pre>
     *
     * Generally speaking <pre>R/B</pre> is a measure of "read reduction bang for your compaction buck". Even
     * if we are only reducing overlap by a small amount (e.g. 2), if it is over a smaller amount of the range
     * we can quickly accomplish that work and reduce overlap.
     */
    private double calculateScore(List<SortedRun> bucket, int overlapsInRange)
    {
        double value = (bucket.size() * Math.max(overlapsInRange - targetReadOptions.maxReadPerRead, 1));
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
            double runSize = getRunSize(run);
            long effectiveDensity = Math.min(1, (long) (sizeInBytes * runSize));
            sstableDensityPairs.add(Pair.create(new SortedRun(run, sizeInBytes, runSize), effectiveDensity));
        }
        return sstableDensityPairs;
    }

    // Tells us how much of the token range (percentage) is spanned by this run
    private static double getRunSize(Iterable<SSTableReader> run)
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

        return new Range<>(min, max).unwrap().stream().mapToDouble(r -> r.left.size(r.right)).sum();
    }

    private List<SSTableReader> findTombstoneEligibleSSTables(int gcBefore, Set<SSTableReader> candidates)
    {
        // if there is no sstable to compact in the normal way, try compacting single sstable whose
        // droppable tombstone ratio is greater than the threshold.
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

        // Adjust the target size to meet the max count goal.
        if (candidatesSet.size() > targetReadOptions.maxSSTableCount)
        {
            long totalSize = candidatesSet.stream().mapToLong(SSTableReader::onDiskLength).sum();
            final double targetSSTableSize = Math.max(targetReadOptions.targetSSTableSize,
                                                      totalSize / (targetReadOptions.maxSSTableCount * 1.10));
            this.targetSSTableSize = (long) Math.ceil(targetSSTableSize);
        }

        // Handle freshly flushed data first, in order to not do a bunch of unneccesary compaction
        // early on we have to gather up a bunch of data before splitting into smaller ranges
        List<SSTableReader> sstablesToCompact = findNewlyFlushedSSTables(candidatesSet);
        if (sstablesToCompact.size() > 1)
        {
            logger.info("TRCS normalizing newly flushed SSTables: {}", sstablesToCompact);
            return Pair.create(sstablesToCompact, 1);
        }

        // The Primary "Target Read" part of the algorithm. Now we consider levels to be sorted runs
        // and look for overlapping sstables, tiered by density (keys / range)
        sstablesToCompact = findOverlappingSSTables(candidatesSet);
        if (sstablesToCompact.size() > 1)
        {
            long readReduction = sstablesToCompact.stream().mapToInt(SSTableReader::getSSTableLevel).distinct().count();
            logger.info("TRCS compacting {} sorted runs to reduce reads per read: {}", readReduction, sstablesToCompact);
            return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));
        }

        // If we have exceeded the target number of SSTables, re-write sorted runs into larger files
        // This keeps the number of tables reasonable even as datasets scale to TiBs of data
        if (candidatesSet.size() > targetReadOptions.maxSSTableCount)
        {
            sstablesToCompact = findSmallSSTables(candidatesSet);
            if (sstablesToCompact.size() > 1)
            {
                logger.info("TRCS re-writing to meet max_sstable_count: {}", sstablesToCompact);
                return Pair.create(sstablesToCompact, sstablesToCompact.get(0).getSSTableLevel());
            }
        }

        // If we get here then check if tombstone compaction is available and do that
        sstablesToCompact = findTombstoneEligibleSSTables(gcBefore, candidatesSet);
        if (sstablesToCompact.size() > 1)
        {
            logger.info("TRCS re-writing to purge tombstones: {}", sstablesToCompact);
            return Pair.create(sstablesToCompact, sstablesToCompact.get(0).getSSTableLevel());
        }

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

            // Flushed sstables create overlaps with essentially all sorted runs (assuming writes are
            // evenly distributed). This will in turn make the covering range essentially the whole
            // range. To get around this we try to split these "bad" sstables into a bunch of small
            // "good" (aka covering smaller range) SSTables during the first compaction.
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
                targetSize = Math.max(4096, Math.round(((totalSize * ratio) / targetReadOptions.splitRange)));

                // At this point we need to "promote" this to a proper sorted run identifier
                level = getLevel(sstablesToCompact);

                logger.debug("TRCS normalization compaction yielding {} sstables of size {}mb. Achieving compaction ratio of: {}",
                             totalSize / targetSize, targetSize / (1024 * 1024), ratio);
            }

            LifecycleTransaction transaction = cfs.getTracker().tryModify(sstablesToCompact, OperationType.COMPACTION);
            if (transaction != null)
            {
                return new LeveledCompactionTask(cfs, transaction, level,
                                                 gcBefore, targetSize, false);
            }
        }
    }

    @SuppressWarnings("resource")
    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        long currentTime = System.currentTimeMillis();
        // hack to allow cancelling major compactions since stopping compaction won't
        // unset this local piece of desire state.
        if ((currentTime - lastMajorCompactionTime) < 60)
        {
            logger.info("Cancelling major compaction due to rapid toggle");
            lastMajorCompactionTime = 0;
        }
        else
        {
            logger.info(
            "TargetReadCompactionStrategy does not support blocking full compactions, " +
            "informing background tasks to beging full compactions of any SSTables older than {}",
            currentTime
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
        return targetSSTableSize * 1024 * 1024;
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
        return String.format("TargetReadCompactionStrategy[%s splits:%s mb]",
                             targetReadOptions.splitRange,
                             targetSSTableSize / (1024 * 1024));
    }
}
