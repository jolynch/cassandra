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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class BoundedReadCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(BoundedReadCompactionStrategy.class);
    private static final Integer NORMALIZATION_COMPACTION = -1;
    private static final Integer CONSOLIDATION_COMPACTION = -2;
    private static final Integer TOMBSTONE_COMPACTION = -3;
    private static final Double MAJOR_COMPACTION = Double.MAX_VALUE;
    private static final double EPSILON = 0.00000001 ;

    // Static purely so we don't loose these on options changes, if we loose them on restarts
    // that's ok.
    protected static volatile long majorCompactionGoalSetAt = 0;
    protected static volatile long nextMajorCompactionTime = 0;
    protected static AtomicLong pendingMajors = new AtomicLong(0);

    protected final ReentrantLock selectionLock = new ReentrantLock();
    protected final int maxSplits;
    protected final int targetRangeSplits;
    protected final BoundedReadCompactionStrategyOptions compactionOptions;
    protected final Comparator<Pair<List<SortedRun>, Double>> scoreComparator = Comparator.<Pair<List<SortedRun>, Double>>
                                                                                           comparingDouble(s -> s.right).reversed();
    protected volatile int estimatedRemainingTasks;
    protected volatile long targetSSTableSizeBytes;
    protected volatile int adjustedMinThreshold;
    protected volatile Pair<Integer, SSTableIntervalTree> cachedTree;
    protected volatile Pair<Integer, List<Pair<List<SortedRun>, Double>>> cachedScores;

    /**
     * Encapsulates a "Sorted Run" (aka Level) of SSTable segments.
     */
    @VisibleForTesting
    static class SortedRun
    {
        public final DecoratedKey first;
        public final DecoratedKey last;
        public final List<SSTableReader> sstables;
        public final long uncompressedSizeInBytes;
        public final long onDiskSizeInBytes;
        public final double keyRangeSize;
        public final long createdAtMillis;
        public final long maxTimestampMillis;
        public final int level;

        @VisibleForTesting
        SortedRun(long uncompressedSize, long onDiskSize, DecoratedKey first, DecoratedKey last, long createdAtMillis)
        {
            this.first = first;
            this.last = last;
            this.uncompressedSizeInBytes = uncompressedSize;
            this.onDiskSizeInBytes = onDiskSize;
            this.keyRangeSize = first.getToken().size(last.getToken());
            this.createdAtMillis = createdAtMillis;
            this.level = 0;
            this.sstables = Collections.emptyList();
            this.maxTimestampMillis = createdAtMillis + 1000;
        }

        SortedRun(Set<SSTableReader> sstables)
        {
            assert sstables.size() > 0;
            this.sstables = Lists.newArrayList(sstables);
            this.sstables.sort(Comparator.comparing(o -> o.first));
            this.level = this.sstables.get(0).getSSTableLevel();
            this.first = this.sstables.get(0).first;
            this.last = this.sstables.get(this.sstables.size() - 1).last;

            long sizeInBytes = 0;
            long onDiskSizeInBytes = 0;
            double keyRangeSize = 0;
            long createdAt = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            for (SSTableReader sst: this.sstables)
            {
                sizeInBytes += sst.uncompressedLength();
                onDiskSizeInBytes += sst.onDiskLength();
                keyRangeSize += sst.first.getToken().size(sst.last.getToken());
                createdAt = Math.min(createdAt, sst.getCreationTimeFor(Component.DATA));
                maxTimestamp = Math.max(maxTimestamp, sst.getMaxTimestamp());
            }
            this.uncompressedSizeInBytes = sizeInBytes;
            this.onDiskSizeInBytes = onDiskSizeInBytes;
            this.keyRangeSize = Math.min(1.0, keyRangeSize);
            this.createdAtMillis = createdAt;
            this.maxTimestampMillis = maxTimestamp;
        }

        public String toString()
        {
            return "SortedRun{" +
                   "level=" + this.level +
                   ", count=" + sstables.size() +
                   ", sizeInMiB=" + uncompressedSizeInBytes / (1024 * 1024) +
                   ", keyRangeSize=" + keyRangeSize +
                   ", createdAt=" + createdAtMillis +
                   '}';
        }
    }

    private final Set<SSTableReader> sstables = new HashSet<>();

    public BoundedReadCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.compactionOptions = new BoundedReadCompactionStrategyOptions(options);
        this.targetSSTableSizeBytes = compactionOptions.targetSSTableSizeBytes;
        this.adjustedMinThreshold = cfs.getMinimumCompactionThreshold();
        this.cachedTree = Pair.create(Collections.emptySet().hashCode(), SSTableIntervalTree.empty());
        this.cachedScores = Pair.create(Collections.emptySet().hashCode(), Collections.emptyList());

        this.maxSplits = (int) (compactionOptions.targetConsolidateSizeInBytes / compactionOptions.minSSTableSizeBytes);
        this.targetRangeSplits = (int) Math.min(maxSplits, Math.max(1, Math.ceil(1 / compactionOptions.maxWorkUnitPercent)));
    }

    @Override
    public void startup()
    {
        super.startup();
        // Hack for the user to signal to stop generating major compactions
        if (compactionOptions.maxLevelAgeSeconds == 0)
            majorCompactionGoalSetAt = 0;
    }

    private Pair<List<SSTableReader>, List<SSTableReader>> findNewlyFlushedSSTables(Set<SSTableReader> candidates)
    {
        int cfsMin = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        List<SSTableReader> recentlyFlushed = candidates.stream()
                                                        .filter(s -> s.getSSTableLevel() == 0)
                                                        .sorted(SSTableReader.sizeComparator)
                                                        .collect(Collectors.toList());

        long size = 0;
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        for (SSTableReader sst: recentlyFlushed)
        {
            size += sst.onDiskLength();
            long createdAt = sst.getCreationTimeFor(Component.DATA);
            minTimestamp = Math.min(minTimestamp, createdAt);
            maxTimestamp = Math.max(maxTimestamp, createdAt);
        }

        long writeIntervalSeconds = (maxTimestamp - minTimestamp) / 1000;
        long consolidationIntervalSeconds = compactionOptions.targetConsolidateIntervalSeconds;
        if (consolidationIntervalSeconds == 0)
        {
            adjustedMinThreshold = cfsMin;
        }
        else if (recentlyFlushed.size() > 4 && writeIntervalSeconds < consolidationIntervalSeconds)
        {
            long flushesToTarget = Math.max(2, (consolidationIntervalSeconds / writeIntervalSeconds) * recentlyFlushed.size());
            // If we are writing rapidly, try to keep the number of compactions before the consolidation interval
            // to no more than 2 (write amplification of 2). To achieve this adjust min threshold according to:
            // log(#flushes) / log(tier) < 2 => tier > sqrt(#flushes)
            int newTier = (int) Math.max(cfsMin, Math.ceil(Math.sqrt(flushesToTarget)));
            newTier = Math.max(cfsMin, Math.min(maxThreshold - cfsMin, Math.min(4 * cfsMin, newTier)));
            logger.trace("Flush pacing observes: newTier={}, flushesToTarget={}, writeIntervalSeconds={}",
                         newTier, flushesToTarget, writeIntervalSeconds);
            if (newTier != adjustedMinThreshold && flushesToTarget < ((long) maxThreshold * maxThreshold))
            {
                logger.debug("BRCS adjusting sparse min_threshold={} due to flushesToTarget={}", newTier, flushesToTarget);
                adjustedMinThreshold = newTier;
            }
        }

        // Consider flushed sstables eligible for entry into the consolidation stage if we have enough data to
        // write out a minThreshold number of targetSSTableSize sstables, or we have enough sstables
        // (assuming they all span the whole token range) such that we may exceed the maxReadPerRead
        //
        // With a 8GiB heap with default settings flushes yield ~225 MiB sstables (2GiB * 0.11). With
        // the defaults of target=1024MiB, minThresholdLevels=4, and maxRead=8 this will almost always
        // yield a compaction after ~8 flushes. The only time we expect to hit the first condition is on write
        // heavy large clusters that have increased maxReadPerRead or on STCS converting to BRCS
        boolean sizeEligible = (size > compactionOptions.targetSSTableSizeBytes * cfsMin) ||
                               (recentlyFlushed.size() >= compactionOptions.maxReadPerRead);

        if ((recentlyFlushed.size() >= adjustedMinThreshold && sizeEligible) || recentlyFlushed.size() >= maxThreshold)
            return Pair.create(recentlyFlushed, recentlyFlushed.stream().limit(maxThreshold).collect(Collectors.toList()));

        return Pair.create(recentlyFlushed, Collections.emptyList());
    }


    private Pair<List<SortedRun>, Set<SSTableReader>> filterSparseSortedRuns(Set<SSTableReader> candidatesSet,
                                                                             double intervalSize)
    {
        Map<Integer, Set<SSTableReader>> sortedRuns;
        sortedRuns = candidatesSet.stream()
                                  .collect(Collectors.groupingBy(SSTableReader::getSSTableLevel,
                                                                 Collectors.toSet()));

        List<Pair<SortedRun, Long>> runs = groupIntoSortedRunWithSize(sortedRuns, intervalSize);

        List<SortedRun> sparseRuns = new ArrayList<>(adjustedMinThreshold);
        List<SortedRun> denseRuns = new ArrayList<>(runs.size());

        long targetSizeBytes = compactionOptions.targetConsolidateSizeInBytes;

        for (Pair<SortedRun, Long> run : runs)
        {
            // Note that the long which came back from groupIntoSortedRunWithSize was the uncompressed size of the run
            // (a good measure of compaction work), but for the sparse runs we want to work towards an on disk size.
            // A run which has 2 GiB spread over 0.1 of the keyRange is as dense as 20GiB over the whole range.
            double effectiveRunSize = run.left.onDiskSizeInBytes * (intervalSize / Math.max(0.00001, run.left.keyRangeSize));

            if (effectiveRunSize >= targetSizeBytes) denseRuns.add(run.left);
            else sparseRuns.add(run.left);
        }
        sparseRuns.sort(Comparator.comparing(sr -> -1 * sr.uncompressedSizeInBytes));
        denseRuns.sort(Comparator.comparing(sr -> -1 * sr.uncompressedSizeInBytes));

        logger.trace("Found {} sparse runs: {}", sparseRuns.size(), sparseRuns);
        logger.trace("Found {} dense runs : {}", denseRuns.size(), denseRuns);

        return Pair.create(sparseRuns, new HashSet<>(sortedRunToSSTables(denseRuns)));
    }

    private List<SSTableReader> findSmallSSTables(Set<SSTableReader> sizeCandidates, long now)
    {
        final int maxThreshold = cfs.getMaximumCompactionThreshold();

        // We don't want to re-write within a reasonable period of the last re-write
        final long min = compactionOptions.targetRewriteIntervalSeconds * 1000;
        Map<Integer, List<SSTableReader>> tooSmall = sizeCandidates.stream()
                                                                   .filter(s -> now > (s.getCreationTimeFor(Component.DATA) + min))
                                                                   .filter(s -> s.onDiskLength() < targetSSTableSizeBytes)
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

    @VisibleForTesting
    static double calculateIntervalSize(List<AbstractBounds<PartitionPosition>> sortedBoundsByFirst)
    {
        // Essentially "non overlapping ranges". We're just interested in finding the range of keys this
        // node holds so we can normalize densities to that.
        double intervalSize = 0.0;
        PeekingIterator<AbstractBounds<PartitionPosition>> it = Iterators.peekingIterator(sortedBoundsByFirst.iterator());
        while (it.hasNext())
        {
            AbstractBounds<PartitionPosition> beginBound = it.next();
            AbstractBounds<PartitionPosition> endBound = beginBound;
            while (it.hasNext() && endBound.right.compareTo(it.peek().left) >= 0)
                endBound = it.next();
            intervalSize += beginBound.left.getToken().size(endBound.right.getToken());
        }

        return Math.min(1.0, intervalSize);
    }

    private List<SSTableReader> findSparseWork(List<SSTableReader> flushedSSTables,
                                               List<SortedRun> sparseRuns,
                                               int minThreshold, int maxThreshold,
                                               long now)
    {
        if (sparseRuns.size() == 0) return Collections.emptyList();

        List<SSTableReader> sstablesToCompact = Collections.emptyList();
        List<SortedRun> candidate = Collections.emptyList();

        SortedRun oldestRun = Collections.min(sparseRuns, Comparator.comparing(sr -> sr.maxTimestampMillis));
        long deadlineMillis = compactionOptions.targetConsolidateIntervalSeconds * 1000;
        boolean deadlineEligible = deadlineMillis > 0 && oldestRun != null &&
                                   // Don't want to just recompact old runs continuously ...
                                   oldestRun.createdAtMillis < (now - deadlineMillis) &&
                                   oldestRun.maxTimestampMillis < (now - deadlineMillis);
        int numSparseRuns = flushedSSTables.size() + sparseRuns.size();
        long size = sparseRuns.stream().mapToLong(sr -> sr.onDiskSizeInBytes).sum();
        boolean sizeEligible = size > compactionOptions.targetConsolidateSizeInBytes;

        if (numSparseRuns > minThreshold ||
            (sparseRuns.size() > maxThreshold || sizeEligible) ||
            (numSparseRuns > 1 && deadlineEligible))
        {

            // If we've got too many small runs or have hit the deadline we need to consolidate them regardless of
            // the dense runs
            if (sparseRuns.size() > maxThreshold)
            {
                logger.trace("Compacting all sparse runs due to max_threshold={}", maxThreshold);
                candidate = sparseRuns;
            }
            else if (sizeEligible)
            {
                logger.debug("Compacting all sparse runs due to hitting {}={}",
                             BoundedReadCompactionStrategyOptions.TARGET_CONSOLIDATE_SIZE,
                             compactionOptions.targetConsolidateSizeInBytes / (1024 * 1024));
                candidate = sparseRuns;
            }
            else if (deadlineEligible)
            {
                logger.debug("Compacting all sparse runs with data newer than {} due to {} of {} seconds." +
                             "Set to 0 to disable.",
                             oldestRun.maxTimestampMillis,
                             BoundedReadCompactionStrategyOptions.TARGET_CONSOLIDATE_INTERVAL_SECS,
                             deadlineMillis / 1000);
                candidate = new ArrayList<>();
                candidate.add(oldestRun);
                for (SortedRun sr : sparseRuns)
                {
                    if (sr.maxTimestampMillis > oldestRun.maxTimestampMillis) candidate.add(sr);
                }
            }
            else
            {
                List<Pair<SortedRun, Long>> sparseRunsBySize = sparseRuns.stream()
                                                                         .map(sr -> Pair.create(sr, sr.uncompressedSizeInBytes))
                                                                         .collect(Collectors.toList());
                List<List<SortedRun>> buckets = SizeTieredCompactionStrategy.getBuckets(sparseRunsBySize,
                                                                                        compactionOptions.levelBucketHigh,
                                                                                        compactionOptions.levelBucketLow,
                                                                                        0);
                for (List<SortedRun> bucket : buckets)
                {
                    if (bucket.size() >= minThreshold)
                    {
                        if (candidate.isEmpty()) candidate = bucket;
                        long candidateSize = candidate.stream().mapToLong(sr -> sr.uncompressedSizeInBytes).sum();
                        long bucketSize = bucket.stream().mapToLong(sr -> sr.uncompressedSizeInBytes).sum();
                        // We always want to choose the largest sparse runs since they're most likely to reach
                        // the levels.
                        if (bucketSize > candidateSize) candidate = bucket;
                    }
                }
            }

            if (!candidate.isEmpty() && (candidate.size() + flushedSSTables.size() > 1))
            {
                candidate.sort(Comparator.comparing(sr -> sr.uncompressedSizeInBytes));
                sstablesToCompact = sortedRunToSSTables(candidate.stream().limit(maxThreshold).collect(Collectors.toList()));
                sstablesToCompact.addAll(flushedSSTables.stream().limit(maxThreshold).collect(Collectors.toList()));
                logger.debug("BRCS consolidating compaction of {} sparse runs: {}", candidate.size(), sstablesToCompact);
            }
        }
        return sstablesToCompact;
    }

    @VisibleForTesting
    static List<AbstractBounds<PartitionPosition>> findWorkRanges(List<AbstractBounds<PartitionPosition>> ranges,
                                                                  double targetRangeSize)
    {
        logger.trace("Splitting interval into {} size", targetRangeSize);
        List<AbstractBounds<PartitionPosition>> workRanges = new ArrayList<>();

        List<PartitionPosition> starts = Lists.newArrayListWithCapacity(ranges.size());
        List<PartitionPosition> ends = Lists.newArrayListWithCapacity(ranges.size());
        for (AbstractBounds<PartitionPosition> range: ranges)
        {
            starts.add(range.left);
            ends.add(range.right);
        }
        Collections.sort(starts);
        Collections.sort(ends);

        PartitionPosition workStart = starts.get(0);
        PartitionPosition workEnd = ends.get(0);
        int nextStart = 0;
        for (int i = 0; i < ends.size(); i++)
        {
            workEnd = ends.get(i);
            // Always advance the start pointer (but not workStart) to the first start greater than the end
            while (nextStart < starts.size() - 1 && starts.get(nextStart).getToken().compareTo(workEnd.getToken()) <= 0)
            {
                nextStart++;
            }

            if (starts.get(nextStart).compareTo(workStart) > 0 &&
                workStart.getToken().size(workEnd.getToken()) >= targetRangeSize)
            {
                workRanges.add(AbstractBounds.bounds(workStart, true, workEnd, true));
                workStart = starts.get(nextStart);
            }
        }

        // Add in the last range, potentially extending it to cover to the end
        if (workRanges.size() > 0 && workRanges.get(workRanges.size() - 1).left.equals(workStart))
            workRanges.remove(workRanges.size() - 1);

        workRanges.add(AbstractBounds.bounds(workStart, true, workEnd, true));

        return workRanges;
    }

    private List<Pair<SortedRun, Long>> groupOverlappingIntoSortedRuns(SSTableIntervalTree tree,
                                                                       AbstractBounds<PartitionPosition> range)
    {
        return groupOverlappingIntoSortedRuns(tree, range, sst -> false);
    }

    private List<Pair<SortedRun, Long>> groupOverlappingIntoSortedRuns(SSTableIntervalTree tree,
                                                                       AbstractBounds<PartitionPosition> range,
                                                                       Predicate<SSTableReader> excludeFilter)
    {
        Map<Integer, Set<SSTableReader>> sortedRuns = new HashMap<>();
        Set<SSTableReader> sortedRun;
        for (SSTableReader sstable : View.sstablesInBounds(range.left, range.right, tree))
        {
            if (excludeFilter.test(sstable)) continue;
            sortedRun = sortedRuns.computeIfAbsent(sstable.getSSTableLevel(), k -> new HashSet<>());
            sortedRun.add(sstable);
        }

        // Note that the interval size here doesn't really matter as long as it's the same for all density
        // candidates. Unlike when splitting sparse from dense we don't need an exact accounting.
        return groupIntoSortedRunWithSize(sortedRuns, tree.min().getToken().size(tree.max().getToken()));
    }

    private List<SSTableReader> findOverlappingSSTables(List<SortedRun> sparseRuns,
                                                        Set<SSTableReader> overlapCandidates,
                                                        List<AbstractBounds<PartitionPosition>> bounds,
                                                        long nowMillis)
    {
        Set<Integer> sparseRunLevels = sparseRuns.stream().map(sr -> sr.level).collect(Collectors.toSet());
        Set<SSTableReader> allSSTables = Sets.union(new HashSet<>(sortedRunToSSTables(sparseRuns)), overlapCandidates);
        if (allSSTables.isEmpty()) return Collections.emptyList();


        // Best effort caching of scores ... if we end up doing it twice it is not a big deal.
        // we're just trying to avoid re-evaluating overlaps all the time if the sstables haven't changed
        if (allSSTables.hashCode() == cachedScores.left)
            return chooseCandidate(nowMillis, cachedScores.right);

        final int maxThreshold = cfs.getMaximumCompactionThreshold();
        final SSTableIntervalTree tree = cachedTree.right;
        final double intervalSize = tree.min().getToken().size(tree.max().getToken());

        // The split ranges just look at the overall covered range
        logger.trace("BRCS operating on interval of size {}", intervalSize);
        List<AbstractBounds<PartitionPosition>> workRanges = findWorkRanges(bounds,intervalSize / targetRangeSplits);

        logger.trace("BRCS found {} work ranges", workRanges.size());
        logger.trace("BRCS current time of {} and next major compaction allowed after {}", nowMillis, nextMajorCompactionTime);
        List<Pair<List<SortedRun>, Double>> candidateScores = new ArrayList<>();
        int rangesWithWork = 0;
        for (AbstractBounds<PartitionPosition> workingRange: workRanges)
        {
            // We first cut the whole range up into pieces about the size of our max work unit
            // This is aimed to reduce the impact of full range compactions and allow us to work on parts of
            // dense runs in parallel.
            List<Pair<SortedRun, Long>> workRuns = groupOverlappingIntoSortedRuns(tree, workingRange);
            if (workRuns.size() < 2) continue;

            boolean rangeHasWork = false;
            // Should we just have an IntervalTree that operates on sorted runs instead of sstables ...?
            for (Pair<SortedRun, Long> run: workRuns)
            {
                AbstractBounds<PartitionPosition> runRange = AbstractBounds.bounds(run.left.first, true,
                                                                                   run.left.last, true);
                List<Pair<SortedRun, Long>> runs = groupOverlappingIntoSortedRuns(tree, runRange);
                if (runs.size() < 2) continue;

                // Handles full compaction and the edge case where we have some really old (probably really dense) sorted
                // runs. If we detect such runs, we perform a full compaction "across the levels". Assuming our
                // consolidation splitting worked properly this should approximately take 1/splitlevel amount of work
                List<Pair<List<SortedRun>, Double>> candidates = handleMajorCompaction(tree, runs, nowMillis);
                if (candidates.size() > 0)
                {
                    candidateScores.addAll(candidates);
                    rangeHasWork = true;
                }
                else
                {
                    // Look at the smallest ranges that together cover the sorted run range. If we just look
                    // at the sorted runs we might compact runs that don't actually overlap. For example if you
                    // have four runs that are disjoint a sparse run might consider those 4 overlapping runs (even
                    // though reads cannot overlap).
                    List<AbstractBounds<PartitionPosition>> subRangeBounds = getSortedBounds(sortedRunToSSTables(runs));
                    List<AbstractBounds<PartitionPosition>> subRanges = findWorkRanges(subRangeBounds, EPSILON);
                    for (AbstractBounds<PartitionPosition> subRange: subRanges)
                    {
                        // Now that we've gotten past any kind of major compaction, remove any sparse runs
                        // from the bucketing calculation to avoid needless write amplification of sparse runs
                        runs = groupOverlappingIntoSortedRuns(tree, subRange,
                                                              sst -> sparseRunLevels.contains(sst.getSSTableLevel()));
                        candidates = handleOverlappingSortedRuns(runs, maxThreshold, tree, nowMillis);
                        if (candidates.size() > 0)
                        {
                            candidateScores.addAll(candidates);
                            rangeHasWork = true;
                        }
                    }
                }
            }
            if (rangeHasWork) rangesWithWork++;
        }

        // Higher scores are more valuable compaction work, do those first
        candidateScores.sort(scoreComparator);

        cachedScores = Pair.create(allSSTables.hashCode(), candidateScores);
        estimatedRemainingTasks = rangesWithWork;
        return chooseCandidate(nowMillis, candidateScores);
    }

    private List<Pair<List<SortedRun>, Double>> handleOverlappingSortedRuns(List<Pair<SortedRun, Long>> runs,
                                                                           int maxThreshold,
                                                                           SSTableIntervalTree tree,
                                                                           long nowMillis)
    {
        int minThresholdLevels = compactionOptions.minThresholdLevels;
        int maxRead = compactionOptions.maxReadPerRead;

        // If we don't have enough sorted runs to even hit the min threshold skip bucketing
        if (runs.size() < minThresholdLevels && runs.size() < maxRead)
            return Collections.emptyList();

        List<Pair<List<SortedRun>, Double>> candidates = new ArrayList<>();

        Predicate<Long> hasSpace = value -> cfs.getDirectories().hasAvailableDiskSpace(1, value);
        if (minThresholdLevels >= 2)
        {
            List<List<SortedRun>> buckets = SizeTieredCompactionStrategy.getBuckets(runs,
                                                                                    compactionOptions.levelBucketHigh,
                                                                                    compactionOptions.levelBucketLow,
                                                                                    0);

            for (List<SortedRun> bucket : buckets)
            {
                if (bucket.size() < minThresholdLevels) continue;

                candidates.add(Pair.create(createCandidate(bucket, maxThreshold),
                                           calculateScore(bucket, runs.size(), hasSpace, compactionOptions.maxReadPerRead)));
            }
        }

        // Edge case where we can't find any density candidates but we're still over
        // maxReadPerRead so just find minThresholdLevels youngest runs and compact them
        if (candidates.size() == 0 && runs.size() > maxRead)
        {
            // If minThresholdLevels is disabled via setting to 0 or 1, we want 2
            // If minThresholdLevels is large (larger than maxThreshold), we want maxThreshold
            final int toCompact = Math.max(2, Math.min(maxThreshold, minThresholdLevels));
            // Presuming new data is coming out of consolidations, wait for at least that time before
            // compacting young runs again. This might lead us to picking older runs but hopefully ones that are
            // close together in time
            final long rewriteMillis = toCompact * compactionOptions.targetConsolidateIntervalSeconds * 1000;

            // Pick "young" data to compact. This allows time series use cases and for very large
            // datasets will generally correlate with smaller less dense runs.
            runs.sort(Comparator.comparing(r -> r.left.maxTimestampMillis));
            runs.removeIf(sr -> sr.left.createdAtMillis > (nowMillis - rewriteMillis));
            Collections.reverse(runs);

            if (runs.size() < 2) return candidates;

            Set<Integer> targetRuns = new HashSet<>();
            PartitionPosition min = runs.get(0).left.first;
            PartitionPosition max = runs.get(0).left.last;
            for (Pair<SortedRun, Long> run: runs.subList(0, Math.min(runs.size(), toCompact)))
            {
                targetRuns.add(run.left.level);
                if (run.left.first.compareTo(min) < 0) min = run.left.first;
                if (run.left.last.compareTo(max) > 0) max = run.left.last;
            }
            AbstractBounds<PartitionPosition> maxRange = AbstractBounds.bounds(min, true, max, true);

            List<SortedRun> fullOverlap = findAllOverlapping(maxRange, tree, sst -> !targetRuns.contains(sst.getSSTableLevel()));
            logger.trace("BRCS hitting {} of {}, compacting to reduce overlap: {}",
                         BoundedReadCompactionStrategyOptions.MAX_READ_PER_READ, maxRead, fullOverlap);

            candidates.add(Pair.create(createCandidate(fullOverlap, maxThreshold),
                                       calculateScore(fullOverlap, runs.size(), hasSpace, compactionOptions.maxReadPerRead)));
        }
        return candidates;
    }

    private List<Pair<List<SortedRun>, Double>> handleMajorCompaction(SSTableIntervalTree tree,
                                                                      List<Pair<SortedRun, Long>> runs,
                                                                      long now)
    {
        if (runs.size() < 2 || now < nextMajorCompactionTime) return Collections.emptyList();

        runs.sort(Comparator.comparing(r -> r.left.createdAtMillis));
        List<SortedRun> bucket = new ArrayList<>();

        SortedRun run;
        for (int i = runs.size(); i-- > 0;)
        {
            run = runs.get(i).left;
            if (run.createdAtMillis < majorCompactionGoalSetAt)
            {
                bucket.add(run);
                logger.debug("Found full compaction candidate: {}", run);
                continue;
            }

            long gcGraceMillis = cfs.gcBefore((int) (now / 1000L)) * 1000L;
            if (run.createdAtMillis < gcGraceMillis &&
                run.createdAtMillis < (now - (compactionOptions.maxLevelAgeSeconds * 1000L)))
            {
                bucket.add(run);
                logger.debug("Found old run {}. Mixing with newer data due to {} of {}. If this is undesirable " +
                             "set {} to 0 to disable.",
                             run,
                             BoundedReadCompactionStrategyOptions.MAX_LEVEL_AGE_SECS,
                             compactionOptions.maxLevelAgeSeconds,
                             BoundedReadCompactionStrategyOptions.MAX_LEVEL_AGE_SECS);
                continue;
            }
            break;
        }

        List<Pair<List<SortedRun>, Double>> candidates = new ArrayList<>();
        if (bucket.size() > 0)
        {
            for (SortedRun range: bucket)
            {
                AbstractBounds<PartitionPosition> runRange = AbstractBounds.bounds(range.first, true, range.last, true);
                List<SortedRun> overlapping = findAllOverlapping(runRange, tree, sst -> false);
                candidates.add(Pair.create(overlapping, MAJOR_COMPACTION));
            }
        }
        return candidates;
    }

    private List<SortedRun> findAllOverlapping(AbstractBounds<PartitionPosition> range,
                                               SSTableIntervalTree tree,
                                               Predicate<SSTableReader> excludeFilter)
    {
        List<Pair<SortedRun, Long>> overlapping = groupOverlappingIntoSortedRuns(tree, range, excludeFilter);

        // We may overlap with a low level sstable that in turn overlaps with higher levels
        // outside this range. In order to make sure all overlapping data compacts we re-query
        PartitionPosition min = overlapping.get(0).left.first;
        PartitionPosition max = overlapping.get(0).left.last;
        for (Pair<SortedRun, Long> overlap: overlapping)
        {
            if (overlap.left.first.compareTo(min) < 0) min = overlap.left.first;
            if (overlap.left.last.compareTo(max) > 0) max = overlap.left.last;
        }

        AbstractBounds<PartitionPosition> maxRange = AbstractBounds.bounds(min, true, max, true);
        return groupOverlappingIntoSortedRuns(tree, maxRange, excludeFilter).stream()
                                                                            .map(sr -> sr.left)
                                                                            .collect(Collectors.toList());
    }

    private List<SSTableReader> chooseCandidate(long now, List<Pair<List<SortedRun>, Double>> candidateScores)
    {
        if (candidateScores.size() > 0)
        {
            List<SortedRun> bestCandidate = candidateScores.get(0).left;
            double bestScore = candidateScores.get(0).right;
            if (bestScore >= MAJOR_COMPACTION)
            {
                long majors = targetRangeSplits;
                long pending = pendingMajors.get();
                if (majors > pending)
                {
                    pendingMajors.compareAndSet(pending, majors);
                }
                long paceMillis = compactionOptions.targetRewriteIntervalSeconds * 1000;
                nextMajorCompactionTime = now + (paceMillis / Math.max(1, pendingMajors.get()));
            }
            else
            {
                pendingMajors.set(0);
            }

            logger.debug("BRCS found {} ranges with candidate compactions, working on sorted_runs={},score={}",
                         estimatedRemainingTasks,
                         bestCandidate,
                         bestScore);

            return sortedRunToSSTables(bestCandidate);
        }
        else
        {
            logger.trace("BRCS yielded zero overlap candidates");
            return Collections.emptyList();
        }
    }

    @VisibleForTesting
    static List<SortedRun> createCandidate(List<SortedRun> bucket, int maxThreshHold)
    {
        // If we're going to be cutoff by maxThreshold, we want to do the smallest runs.
        bucket.sort(Comparator.comparing(b -> b.uncompressedSizeInBytes));

        return bucket.stream()
                     .limit(maxThreshHold)
                     .collect(Collectors.toList());
    }

    static List<SSTableReader> sortedRunToSSTables(Collection<SortedRun> bucket)
    {
        return bucket.stream()
                     .map(sr -> sr.sstables)
                     .flatMap(Collection::stream)
                     .distinct()
                     .collect(Collectors.toList());
    }

    static List<SSTableReader> sortedRunToSSTables(List<Pair<SortedRun, Long>> bucket)
    {
        return sortedRunToSSTables(bucket.stream().map(p -> p.left).collect(Collectors.toList()));
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
     * score = R * max((O - M), 1) / B
     * score ~= R / B
     * </pre>
     *
     * Generally speaking <pre>R/B</pre> is a measure of "read reduction bang for your compaction buck". Even
     * if we are only reducing overlap by a small amount (e.g. 2), if it is over less dense candidates
     * we can quickly accomplish that work and reduce overlap. In order to make a larger compaction worth
     * doing we'd have to get an overlap reduction that is proportionally larger (so e.g. to justify doing a
     * 4 level reduction, we'd have to involve fewer than 4 times the bytes to make it high priority)
     */
    @VisibleForTesting
    static double calculateScore(List<SortedRun> bucket, int overlapsInRange, Predicate<Long> hasSpace, int maxRead)
    {
        double score = (bucket.size() * Math.max(overlapsInRange - maxRead, 1));

        long totalBytes = 0;
        double normalizedBytes = 0;
        for (SortedRun sortedRun : bucket) {
            totalBytes += sortedRun.onDiskSizeInBytes;
            normalizedBytes += Math.max(1, sortedRun.uncompressedSizeInBytes * sortedRun.keyRangeSize);
        }
        if (hasSpace.test(totalBytes))
        {
            return score / normalizedBytes;
        }
        else
        {
            // We may still have to yield a compaction that is too large to try to reclaim some
            // disk space, but try to prefer ones that we know won't do that.
            return 0.001 * (score / normalizedBytes);
        }
    }

    private static List<Pair<SortedRun, Long>> groupIntoSortedRunWithSize(Map<Integer, Set<SSTableReader>> sstables,
                                                                          double intervalSize)
    {
        List<Pair<SortedRun, Long>> sstableDensityPairs = new ArrayList<>(sstables.size());
        for (Set<SSTableReader> run : sstables.values())
        {
            if (run.size() == 0) continue;
            // Since we use a fixed size SSTable size is a good proxy for density
            SortedRun sortedRun = new SortedRun(run);

            // How large this run would be if applied to the whole range
            long normalizedBytes = Math.round(sortedRun.uncompressedSizeInBytes *
                                              (intervalSize / Math.max(EPSILON, sortedRun.keyRangeSize)));
            normalizedBytes = Math.max(1, normalizedBytes);
            sstableDensityPairs.add(Pair.create(sortedRun, normalizedBytes));
        }
        return sstableDensityPairs;
    }

    private List<SSTableReader> findTombstoneEligibleSSTables(int gcBefore, Set<SSTableReader> candidates, int maxThreshold)
    {
        // if there is no sstable to compact in the normal way, try compacting sstables whose
        // droppable tombstone ratio is greater than the threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        // Since we are dealing with sorted runs of small tables, we want to gather up a unit of work larger
        // than a single SSTable otherwise we'll end up with a lot of single file compactions
        long targetSize = compactionOptions.targetConsolidateSizeInBytes;
        long size;
        int count = 0;

        // Take the sstables with the most droppable tombstones first
        sstablesWithTombstones.sort(Comparator.comparing(s -> -1 * s.getEstimatedDroppableTombstoneRatio(gcBefore)));
        for (SSTableReader sst: sstablesWithTombstones)
        {
            size = sst.uncompressedLength();
            count++;
            if (size >= targetSize || count >= maxThreshold) break;
        }
        return sstablesWithTombstones.subList(0, count);
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

    private void adjustTargetSize() {
        Set<SSTableReader> liveSet = Sets.newHashSet(filterSuspectSSTables(cfs.getSSTables(SSTableSet.CANONICAL)));

        if (liveSet.size() > (compactionOptions.maxSSTableCount / 2))
        {
            final long totalSize = liveSet.stream().mapToLong(SSTableReader::onDiskLength).sum();
            final double targetSSTableSize = Math.max(targetSSTableSizeBytes,
                                                      totalSize / (compactionOptions.maxSSTableCount / 2));
            // So we don't have oddly sized tables, just always change in increments of 256 MiB
            final long next256 = 256 * 1024 * 1024;
            final long nextSize = ((long) (Math.ceil(targetSSTableSize / next256))) * (next256);
            if (nextSize != this.targetSSTableSizeBytes) {
                logger.debug("BRCS adjusting target sstable size to {}MiB due to observing {}MiB dataset over {} SSTables",
                             nextSize / 1024 / 1024,
                             totalSize / 1024 / 1024,
                             liveSet.size());
                this.targetSSTableSizeBytes = nextSize;
            }
        }
    }

    private static List<AbstractBounds<PartitionPosition>> getSortedBounds(Collection<SSTableReader> sstables)
    {
        List<SSTableReader> sortedByFirst = Lists.newArrayList(sstables);
        sortedByFirst.sort(Comparator.comparing(s -> s.first));

        return sortedByFirst.stream()
                            .map(s -> AbstractBounds.bounds(s.first, true, s.last, true))
                            .collect(Collectors.toList());
    }

    private Pair<List<SSTableReader>, Integer> getSSTablesForCompaction(int gcBefore)
    {
        // Adjust the target size to meet the max count goal. Do this before we hit the goal so that
        // we can get "ahead" of the problem and hopefully do not need to re-write sorted runs later
        adjustTargetSize();
        Set<SSTableReader> candidatesSet = Sets.newHashSet(filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains)));

        // Handle freshly flushed data first, in order to not do a bunch of unneccesary compaction
        // early on we want to gather up a good amount of data before yielding a sorted run
        Pair<List<SSTableReader>, List<SSTableReader>> filteredFlushes = findNewlyFlushedSSTables(candidatesSet);
        logger.trace("{}", this);

        final int minThreshold = adjustedMinThreshold;
        final int maxThreshold = cfs.getMaximumCompactionThreshold();
        final long nowMillis = System.currentTimeMillis();
        List<SSTableReader> flushedSSTables = filteredFlushes.left;
        List<SSTableReader> sstablesToCompact = filteredFlushes.right;
        if (sstablesToCompact.size() > 1)
        {
            logger.debug("BRCS normalizing compaction of {} newly flushed SSTables: {}", sstablesToCompact.size(), sstablesToCompact);
            return Pair.create(sstablesToCompact, NORMALIZATION_COMPACTION);
        }

        // Once we remove Level 0 (which we _know_ overlaps with each other) all remaining levels should be
        // non overlapping sorted runs within a level (there may be many levels that overlap).
        candidatesSet.removeIf(s -> s.getSSTableLevel() == 0);
        if (candidatesSet.size() == 0) return Pair.create(sstablesToCompact, 0);
        if (candidatesSet.hashCode() != cachedTree.left)
        {
            cachedTree = Pair.create(candidatesSet.hashCode(), SSTableIntervalTree.build(candidatesSet));
        }

        List<AbstractBounds<PartitionPosition>> bounds = getSortedBounds(candidatesSet);
        double intervalSize = calculateIntervalSize(bounds);
        logger.trace("BRCS interval size: {}", intervalSize);

        // Handle sorted runs that are not dense enough yet, keep consolidating such runs until
        // they get big enough to enter the "levels". Basically just doing STCS at this point but
        // with an enforcement that we don't exceed maxThreshold
        Pair<List<SortedRun>, Set<SSTableReader>> sparseAndDenseRuns = filterSparseSortedRuns(candidatesSet, intervalSize);

        sstablesToCompact = findSparseWork(flushedSSTables, sparseAndDenseRuns.left, minThreshold, maxThreshold, nowMillis);
        if (sstablesToCompact.size() > 1)
        {
            return Pair.create(sstablesToCompact, CONSOLIDATION_COMPACTION);
        }

        // The leveling part of the algorithm. Now instead of considering full interval runs, consider levels to be
        // sorted runs and look for overlapping runs, bucketed by density (size / |range|)
        sstablesToCompact = findOverlappingSSTables(sparseAndDenseRuns.left, sparseAndDenseRuns.right, bounds, nowMillis);
        if (sstablesToCompact.size() > 1)
        {
            long readReduction = sstablesToCompact.stream().mapToInt(SSTableReader::getSSTableLevel).distinct().count();
            logger.debug("BRCS leveled compaction of {} dense runs: {}", readReduction, sstablesToCompact);
            return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));
        }

        // If we have exceeded the target number of SSTables, re-write sorted runs into larger files
        // to keep the number of SSTables (-> files) reasonable even as datasets scale to TiBs of data
        if (sstables.size() > compactionOptions.maxSSTableCount)
        {
            sstablesToCompact = findSmallSSTables(candidatesSet, nowMillis);
            if (sstablesToCompact.size() > 1)
            {
                logger.debug("BRCS re-writing to meet max_sstable_count: {}", sstablesToCompact);
                return Pair.create(sstablesToCompact, sstablesToCompact.get(0).getSSTableLevel());
            }
        }

        // If we get here then check if tombstone compaction is available and do that
        sstablesToCompact = findTombstoneEligibleSSTables(gcBefore, candidatesSet, maxThreshold);
        if (sstablesToCompact.size() > 1)
        {
            logger.debug("BRCS re-writing to purge tombstones: {}", sstablesToCompact);
            return Pair.create(sstablesToCompact, TOMBSTONE_COMPACTION);
        }

        return Pair.create(sstablesToCompact, 0);
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        selectionLock.lock();
        try
        {
            for (int tries = 0; tries < 1000; tries++)
            {
                Pair<List<SSTableReader>, Integer> compactionTarget = getSSTablesForCompaction(gcBefore);
                List<SSTableReader> sstablesToCompact = compactionTarget.left;
                if (sstablesToCompact.isEmpty())
                    return null;

                int level = compactionTarget.right;
                long targetSize = targetSSTableSizeBytes;
                OperationType type = OperationType.COMPACTION;

                // Flushed sstables and sparse runs create overlaps with essentially all sorted runs (assuming writes
                // are evenly distributed). This could lead to a major compaction using more than the target
                // unit of work so to get around this we try to split these "bad" sstables into a bunch of small
                // "good" (aka covering smaller range) SSTables while compacting them.
                if (level == CONSOLIDATION_COMPACTION || level == NORMALIZATION_COMPACTION)
                {
                    targetSize = calculateTargetSize(sstablesToCompact);
                    level = getLevel(sstablesToCompact);
                }
                else if (level == TOMBSTONE_COMPACTION)
                {
                    type = OperationType.TOMBSTONE_COMPACTION;
                    level = getLevel(sstablesToCompact);
                }

                LifecycleTransaction transaction = cfs.getTracker().tryModify(sstablesToCompact, type);
                if (transaction != null)
                {
                    return new LeveledCompactionTask(cfs, transaction, level,
                                                     gcBefore, targetSize, false);
                }
            }
            logger.warn("Could not mark sstables for compaction after 1000 tries, giving up");
            return null;
        }
        finally
        {
            selectionLock.unlock();
        }
    }

    private long calculateTargetSize(Collection<SSTableReader> sstablesToCompact)
    {
        long totalCount = 0;
        long totalSize = 0;
        for (SSTableReader sstable: sstablesToCompact)
        {
            totalCount += SSTableReader.getApproximateKeyCount(Collections.singletonList((sstable)));
            totalSize += sstable.bytesOnDisk();
        }
        long estimatedCombinedCount = SSTableReader.getApproximateKeyCount(sstablesToCompact);

        double ratio = (double) estimatedCombinedCount / (double) totalCount;
        long targetSize = Math.max(compactionOptions.minSSTableSizeBytes,
                                   Math.round(((totalSize * ratio) / targetRangeSplits)));
        targetSize = Math.min(targetSSTableSizeBytes, targetSize);
        logger.debug("BRCS splitting sparse run yielding {} sstables of size {}MiB after compaction ratio {}",
                     targetRangeSplits, targetSize / (1024 * 1024), ratio);
        return targetSize;
    }

    @SuppressWarnings("resource")
    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        long currentTime = System.currentTimeMillis();
        boolean acquired = false;
        try
        {
            // Best effort wait for background selections to finish so we don't have to deal with
            // with backgrounds selecting our sstables
            acquired = selectionLock.tryLock(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            logger.warn("Maximal Compaction wasn't able to acquire the selection lock ... trying to compact anyways.");
        }

        try
        {
            logger.info("GeneralCompactionStrategy does not support fully blocking compactions. Immediately " +
                        "compacting sparse runs and informing background tasks to begin full compactions of " +
                        "any dense runs older than {}. Will try to spread dense runs over the {} of {} seconds.",
                        currentTime,
                        BoundedReadCompactionStrategyOptions.TARGET_REWRITE_INTERVAL_SECS,
                        compactionOptions.targetRewriteIntervalSeconds);
            majorCompactionGoalSetAt = currentTime;
            // Delay the first major compaction by enough time for the flush and sparse tables to compact
            nextMajorCompactionTime = currentTime +
                                      (compactionOptions.targetRewriteIntervalSeconds * 1000) / targetRangeSplits;

            Set<SSTableReader> candidatesSet = Sets.newHashSet(filterSuspectSSTables(filter(cfs.getUncompactingSSTables(),
                                                                                            sstables::contains)));

            Pair<List<SSTableReader>, List<SSTableReader>> filteredFlushes = findNewlyFlushedSSTables(candidatesSet);
            double intervalSize = calculateIntervalSize(getSortedBounds(candidatesSet));
            Pair<List<SortedRun>, Set<SSTableReader>> filteredRuns = filterSparseSortedRuns(candidatesSet, intervalSize);

            // Immediately yield all flush products and sparse sstables. We need to get new data into the levels ASAP
            // so they can participate in the major compactions.
            Set<SSTableReader> toCompact = new HashSet<>(filteredFlushes.left);
            toCompact.addAll(sortedRunToSSTables(filteredRuns.left));

            LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION);
            if (txn == null)
                return null;

            return Collections.singletonList(new LeveledCompactionTask(cfs, txn, getLevel(toCompact),
                                                                       gcBefore, calculateTargetSize(toCompact), false));
        }
        finally
        {
            if (acquired) selectionLock.unlock();
        }
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first. " +
                         "You can disable background compactions temporarily if this is a problem",
                         sstables);
            return null;
        }

        return new LeveledCompactionTask(cfs, transaction, getLevel(sstables),
                                         gcBefore, targetSSTableSizeBytes, false).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return targetSSTableSizeBytes * 1024 * 1024;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = BoundedReadCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

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
        return String.format("BoundedReadCompactionStrategy"+
                             "[file_size=%dMiB, compaction_target=%dMiB," +
                             " num_splits:%d, sparse_tier_factor=%d]",
                             targetSSTableSizeBytes / (1024 * 1024),
                             compactionOptions.targetConsolidateSizeInBytes / (1024 * 1024),
                             targetRangeSplits,
                             adjustedMinThreshold);
    }
}
