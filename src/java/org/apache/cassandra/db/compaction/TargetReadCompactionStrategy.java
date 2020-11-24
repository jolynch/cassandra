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
import java.util.concurrent.atomic.AtomicLong;
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

public class TargetReadCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TargetReadCompactionStrategy.class);
    // Static purely so we don't loose these on options changes, if we loose them on restarts
    // that's ok. The only one we might care about keeping across restarts would be
    // targetRangeSplits.
    protected static volatile long majorCompactionGoalSetAt = 0;
    protected static volatile long nextMajorCompactionTime = 0;
    protected static AtomicLong pendingMajors = new AtomicLong(0);
    protected static int targetRangeSpits;

    protected final static Double MAJOR_COMPACTION = Double.MAX_VALUE;

    protected TargetReadCompactionStrategyOptions targetReadOptions;
    protected volatile int estimatedRemainingTasks;
    protected long targetSSTableSizeBytes;
    protected volatile Pair<Integer, SSTableIntervalTree> cachedTree;
    protected volatile Pair<Integer, List<Pair<List<SortedRun>, Double>>> cachedScores;

    /** Used to encapsulate a Sorted Run (aka "Level" of SSTables)
     */
    private static class SortedRun
    {
        public final List<SSTableReader> sstables;
        public final long uncompressedSizeInBytes;
        public final long sizeOnDiskInBytes;
        public final double keyRangeSize;
        public final long createdAt;
        public final int level;

        private SortedRun(Set<SSTableReader> sstables)
        {
            assert sstables.size() > 0;
            this.sstables = Lists.newArrayList(sstables);
            this.sstables.sort(Comparator.comparing(o -> o.first));
            this.level = this.sstables.get(0).getSSTableLevel();

            long sizeInBytes = 0;
            long onDiskSizeInBytes = 0;
            double keyRangeSize = 0;
            long createdAt = Long.MAX_VALUE;
            for (SSTableReader sst: this.sstables)
            {
                sizeInBytes += sst.uncompressedLength();
                onDiskSizeInBytes += sst.onDiskLength();
                keyRangeSize += sst.first.getToken().size(sst.last.getToken());
                createdAt = Math.min(createdAt, sst.getCreationTimeFor(Component.DATA));
            }
            this.uncompressedSizeInBytes = sizeInBytes;
            this.sizeOnDiskInBytes = onDiskSizeInBytes;
            this.keyRangeSize = Math.min(1.0, keyRangeSize);
            this.createdAt = createdAt;
        }

        public String toString()
        {
            return "SortedRun{" +
                   "level=" + this.level +
                   ", count=" + sstables.size() +
                   ", sizeInMiB=" + uncompressedSizeInBytes / (1024 * 1024) +
                   ", keyRangeSize=" + keyRangeSize +
                   ", createdAt=" + createdAt +
                   '}';
        }
    }

    private final Set<SSTableReader> sstables = new HashSet<>();

    public TargetReadCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.targetReadOptions = new TargetReadCompactionStrategyOptions(options);
        this.targetSSTableSizeBytes = targetReadOptions.targetSSTableSizeBytes;
        this.cachedTree = Pair.create(Collections.emptySet().hashCode(), SSTableIntervalTree.empty());
        this.cachedScores = Pair.create(Collections.emptySet().hashCode(), Collections.emptyList());

        int initialSplit = (int) Math.min(1,
                                          targetReadOptions.targetWorkSizeInBytes /
                                          targetReadOptions.targetSSTableSizeBytes);
        targetRangeSpits = Math.max(targetRangeSpits, initialSplit);
    }

    @Override
    public void startup()
    {
        super.startup();
        // How the user can signal to stop major compactions
        if (targetReadOptions.maxLevelAgeSeconds == 0)
            majorCompactionGoalSetAt = 0;
    }

    private Pair<List<SSTableReader>, List<SSTableReader>> findNewlyFlushedSSTables(Set<SSTableReader> candidates)
    {
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        List<SSTableReader> recentlyFlushed = candidates.stream()
                                                        .filter(s -> s.getSSTableLevel() == 0)
                                                        .sorted(SSTableReader.sizeComparator)
                                                        .collect(Collectors.toList());

        long size = recentlyFlushed.stream().mapToLong(SSTableReader::uncompressedLength).sum();

        // Consider flushed sstables eligible for entry into the consolidation stage if we have enough data to
        // write out a minThreshold number of targetSSTableSize sstables, or we have enough sstables
        // (assuming they all span the whole token range) such that we may exceed the maxReadPerRead
        //
        // With a 8GiB heap with default settings flushes yield ~225 MiB sstables (2GiB * 0.11). With
        // the defaults of target=512MiB, minThreshold=4, targetRead=4 and maxRead=12 this will almost always
        // yield a compaction after ~8 flushes. The only time we expect to hit the first condition is on write
        // heavy large clusters that have increased both targetReadPerRead and maxReadPerRead or on STCS converting
        // to TRCS
        boolean sizeEligible = (size > (targetSSTableSizeBytes * minThreshold)) ||
                               ((recentlyFlushed.size() + targetReadOptions.targetReadPerRead) >= targetReadOptions.maxReadPerRead);

        if (recentlyFlushed.size() >= minThreshold && sizeEligible)
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

        List<Pair<SortedRun, Long>> runs = groupIntoSortedRunWithSize(sortedRuns);

        List<SortedRun> sparseRuns = new ArrayList<>(cfs.getMinimumCompactionThreshold());
        List<SortedRun> denseRuns = new ArrayList<>(runs.size());

        long targetSizeBytes = targetSSTableSizeBytes * targetRangeSpits;

        for (Pair<SortedRun, Long> run : runs)
        {
            // A run which has 2 GiB spread over 0.1 of the keyRange is as dense as 20GiB over the whole range.
            double effectiveRunSize = run.left.sizeOnDiskInBytes * (intervalSize / Math.max(0.00001, run.left.keyRangeSize));

            if (effectiveRunSize >= targetSizeBytes) denseRuns.add(run.left);
            else sparseRuns.add(run.left);
        }

        logger.trace("Found {} sparse runs: {}", sparseRuns.size(), sparseRuns);
        logger.trace("Found {} dense runs : {}", denseRuns.size(), denseRuns);

        Set<SSTableReader> toPassOn = new HashSet<>(sortedRunToSSTables(denseRuns));
        return Pair.create(sparseRuns, toPassOn);
    }

    private List<SSTableReader> findSmallSSTables(Set<SSTableReader> sizeCandidates)
    {
        final int maxThreshold = cfs.getMaximumCompactionThreshold();

        // We don't want to re-write within a reasonable period of the last re-write
        final long now = System.currentTimeMillis();
        final long min = targetReadOptions.targetRewriteIntervalSeconds * 1000;
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

    @VisibleForTesting
    static List<AbstractBounds<PartitionPosition>> findWorkRanges(List<AbstractBounds<PartitionPosition>> ranges,
                                                                  double targetRangeSize)
    {
        logger.trace("Splitting interval into {} size", targetRangeSize);
        List<AbstractBounds<PartitionPosition>> workRanges = Lists.newArrayListWithCapacity(targetRangeSpits);

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
        int startPos = 0;
        for (int i = 0; i < ends.size(); i++)
        {
            workEnd = ends.get(i);
            // Always advance the start pointer (but not workStart) to the first start greater than the end
            while (startPos < starts.size() - 1 && starts.get(startPos).getToken().compareTo(workEnd.getToken()) <= 0)
            {
                startPos++;
            }

            if (!starts.get(startPos).equals(workStart) &&
                workStart.getToken().size(workEnd.getToken()) >= targetRangeSize)
            {
                workRanges.add(AbstractBounds.bounds(workStart, true, workEnd, true));
                workStart = starts.get(startPos);
            }
        }

        // Add in the last range, potentially extending it to cover to the end
        if (workRanges.size() > 0 && workRanges.get(workRanges.size() - 1).left.equals(workStart))
        {
            workRanges.remove(workRanges.size() - 1);
        }
        workRanges.add(AbstractBounds.bounds(workStart, true, workEnd, true));

        return workRanges;
    }

    private List<SortedRun> findOverlappingSSTables(List<SortedRun> smallRuns,
                                                    Set<SSTableReader> overlapCandidates,
                                                    List<AbstractBounds<PartitionPosition>> bounds)
    {
        Set<Integer> smallRunLevels = smallRuns.stream().map(sr -> sr.level).collect(Collectors.toSet());
        Set<SSTableReader> allSSTables = Sets.union(new HashSet<>(sortedRunToSSTables(smallRuns)), overlapCandidates);
        if (allSSTables.isEmpty()) return Collections.emptyList();

        long now = System.currentTimeMillis();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        // Best effort caching of scores ... if we end up doing it twice it is not a big deal.
        // we're just trying to avoid re-evaluating overlaps all the time if the sstables haven't changed
        if (allSSTables.hashCode() == cachedScores.left)
            return chooseCandidate(now, cachedScores.right);

        SSTableIntervalTree tree = cachedTree.right;

        // The split ranges just look at the overall covered range
        double intervalSize = tree.min().getToken().size(tree.max().getToken());
        logger.trace("TRCS operating on interval of size {}", intervalSize);
        List<AbstractBounds<PartitionPosition>> workRanges = findWorkRanges(bounds,
                                                                            intervalSize / targetRangeSpits);
        logger.trace("TRCS found {} work ranges", workRanges.size());
        logger.trace("TRCS current time of {} and next major compaction allowed after {}", now, nextMajorCompactionTime);
        List<Pair<List<SortedRun>, Double>> candidateScores = new ArrayList<>();
        for (AbstractBounds<PartitionPosition> workingRange: workRanges)
        {
            Map<Integer, Set<SSTableReader>> sortedRuns = new HashMap<>();
            Set<SSTableReader> sortedRun;
            for (AbstractBounds<PartitionPosition> unwrapped : workingRange.unwrap())
            {
                for (SSTableReader sstable : View.sstablesInBounds(unwrapped.left, unwrapped.right, tree))
                {
                    sortedRun = sortedRuns.computeIfAbsent(sstable.getSSTableLevel(), k -> new HashSet<>());
                    sortedRun.add(sstable);
                }
            }
            List<Pair<SortedRun, Long>> runs = groupIntoSortedRunWithSize(sortedRuns);

            // Handles full compaction and the edge case where we have some really old (probably really dense) sorted
            // runs. If we detect such runs, yield the oldest run and all youngest runs up to that size (essentially a
            // "full" compaction across the runs). We choose the youngest because we are probably trying to get updates
            // from the younger runs (e.g. deletes or updates) into the older ones.
            runs.sort(Comparator.comparing(r -> r.left.createdAt));
            if (runs.size() > 1)
            {
                int pivot = runs.size() - 1;
                List<SortedRun> bucket = new ArrayList<>();

                // We want to delay the first major compaction by some time to try to get the data from L0 and the
                // sparse runs time to compact together into a sparse sorted run.
                long majorSignal = nextMajorCompactionTime + (targetReadOptions.targetRewriteIntervalSeconds * 1000 /
                                                              targetRangeSpits);
                if (majorSignal > 0)
                    logger.trace("TRCS waiting until {} to start major level compaction", majorSignal);

                while (pivot >= 0 &&
                       now > majorSignal &&
                       runs.get(pivot).left.createdAt < majorCompactionGoalSetAt) {
                    // We have a signaled full compaction, just yield the qualifying sorted runs
                    // in their entirety.
                    bucket.add(runs.get(pivot).left);
                    pivot--;
                }

                if (bucket.size() > 1)
                {
                    logger.debug("TRCS found full compaction candidates: {}", bucket);
                }
                else
                {
                    SortedRun oldest = runs.get(pivot).left;
                    if (targetReadOptions.maxLevelAgeSeconds > 0 &&
                        now > nextMajorCompactionTime &&
                        (oldest.createdAt < (now - (targetReadOptions.maxLevelAgeSeconds * 1000))))
                    {
                        bucket.add(oldest);
                        logger.debug("TRCS mixing old run {} with newer data due to {} of {}. If this is undesirable" +
                                     "set {} to 0.",
                                     oldest,
                                     TargetReadCompactionStrategyOptions.MAX_LEVEL_AGE_SECS,
                                     targetReadOptions.maxLevelAgeSeconds,
                                     TargetReadCompactionStrategyOptions.MAX_LEVEL_AGE_SECS);
                    }
                }

                if (bucket.size() > 1)
                {
                    long sizeInBytes = bucket.stream().mapToLong(s -> s.uncompressedSizeInBytes).sum();
                    int runIndex = 0;
                    while (sizeInBytes > 0 && runIndex < pivot)
                    {
                        bucket.add(runs.get(runIndex).left);
                        sizeInBytes -= runs.get(runIndex).left.uncompressedSizeInBytes;
                        runIndex++;
                    }
                    candidateScores.add(Pair.create(
                        createCandidate(bucket, bucket.size()),
                        MAJOR_COMPACTION
                    ));
                }
            }

            // If we don't have enough sorted ranges to even hit the target read per read skip bucketing
            if (runs.size() < targetReadOptions.targetReadPerRead)
                continue;

            // Now that we've gotten past any kind of major compaction, remove any small runs
            // from the bucketing calculation to avoid needless write amplification
            List<Pair<SortedRun, Long>> bucketRuns = new ArrayList<>();
            for (Pair<SortedRun, Long> sr: runs) {
                if (!smallRunLevels.contains(sr.left.level))
                    bucketRuns.add(sr);
            }

            List<List<SortedRun>> buckets = SizeTieredCompactionStrategy.getBuckets(bucketRuns,
                                                                                    targetReadOptions.levelBucketHigh,
                                                                                    targetReadOptions.levelBucketLow,
                                                                                    0);

            // We want buckets which reduce overlap but are relatively small in size
            int bucketsFound = 0;
            int tierFactor = targetReadOptions.minThresholdLevels;
            for (List<SortedRun> bucket : buckets)
            {
                // FIXME: I believe that if splitRange decreases this might compact neighboring sorted runs
                if (bucket.size() < tierFactor)
                    continue;
                bucketsFound += 1;
                candidateScores.add(Pair.create(
                    createCandidate(bucket, maxThreshold),
                    calculateScore(bucket, runs.size())
                ));
            }

            // Edge case where we can't find any density candidates but we're still over
            // maxReadPerRead so just find targetReadPerRead smallest runs and compact them
            if (bucketsFound == 0 &&
                runs.size() > targetReadOptions.maxReadPerRead)
            {
                // We know that maxReadPerRead is always larger than targetReadPerRead
                runs.sort(Comparator.comparing(r -> r.left.uncompressedSizeInBytes));
                List<SortedRun> bucket = runs.subList(0, targetReadOptions.targetReadPerRead).stream()
                                             .map(p -> p.left)
                                             .collect(Collectors.toList());
                logger.debug("TRCS hitting max_read_per_read of {}, compacting to reduce overlap: {}",
                            targetReadOptions.maxReadPerRead,
                            bucket);

                candidateScores.add(Pair.create(
                    createCandidate(bucket, maxThreshold),
                    calculateScore(bucket, runs.size())
                ));
            }
        }

        Comparator<Pair<List<SortedRun>, Double>> c = Comparator.<Pair<List<SortedRun>, Double>>
                                                                 comparingDouble(s -> s.right).reversed();
        candidateScores.sort(c);
        cachedScores = Pair.create(allSSTables.hashCode(), candidateScores);
        estimatedRemainingTasks = candidateScores.size();
        return chooseCandidate(now, candidateScores);
    }

    private List<SortedRun> chooseCandidate(long now, List<Pair<List<SortedRun>, Double>> candidateScores)
    {
        if (candidateScores.size() > 0)
        {
            List<SortedRun> bestCandidate = candidateScores.get(0).left;
            double bestScore = candidateScores.get(0).right;
            if (bestScore >= MAJOR_COMPACTION)
            {
                long majors = candidateScores.stream().filter(cs -> cs.right >= MAJOR_COMPACTION).count();
                long pending = pendingMajors.get();
                if (majors > pending)
                {
                    pendingMajors.compareAndSet(pending, majors);
                }
                long deltaMillis = targetReadOptions.targetRewriteIntervalSeconds * 1000;
                nextMajorCompactionTime = now + (deltaMillis / Math.max(1, pending));
            }
            else
            {
                pendingMajors.set(0);
            }

            logger.debug("TRCS found {} candidate runs, working on sorted_runs={},score={}",
                         candidateScores.size(),
                         bestCandidate,
                         bestScore);

            return bestCandidate;
        }
        else
        {
            logger.trace("TRCS yielded zero overlap candidates");
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

    static List<SSTableReader> sortedRunToSSTables(List<SortedRun> bucket)
    {
        return bucket.stream()
                     .map(sr -> sr.sstables)
                     .flatMap(Collection::stream)
                     .distinct()
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
     * if we are only reducing overlap by a small amount (e.g. 2), if it is over less dense candidates
     * we can quickly accomplish that work and reduce overlap. In order to make a larger compaction worth
     * doing we'd have to get an overlap reduction that is proportionally larger (so e.g. to justify doing a
     * 4 level reduction, we'd have to involve fewer than 4 times the bytes to make it high priority)
     */
    private double calculateScore(List<SortedRun> bucket, int overlapsInRange)
    {
        double value = (bucket.size() * Math.max(overlapsInRange - targetReadOptions.maxReadPerRead, 1));
        long totalBytes = 0;
        double normalizedBytes = 0;
        for (SortedRun sortedRun : bucket) {
            totalBytes += sortedRun.uncompressedSizeInBytes;
            normalizedBytes += Math.min(1, sortedRun.uncompressedSizeInBytes * sortedRun.keyRangeSize);
        }
        if (cfs.getDirectories().hasAvailableDiskSpace(1, totalBytes))
        {
            return value / normalizedBytes;
        }
        else
        {
            // We may still have to yield a compaction that is too large to try to reclaim some
            // disk space, but try to prefer ones that we know won't do that.
            return 0.001 * (value / normalizedBytes);
        }
    }

    private static List<Pair<SortedRun, Long>> groupIntoSortedRunWithSize(Map<Integer, Set<SSTableReader>> sstables)
    {
        List<Pair<SortedRun, Long>> sstableDensityPairs = new ArrayList<>(sstables.size());
        for (Set<SSTableReader> run : sstables.values())
        {
            if (run.size() == 0) continue;
            // Since we use a fixed size SSTable size is a good proxy for density (also nicely handles when a "dense"
            // run moves into a work range with less dense sstables
            SortedRun sortedRun = new SortedRun(run);
            sstableDensityPairs.add(Pair.create(sortedRun, sortedRun.uncompressedSizeInBytes));
        }
        return sstableDensityPairs;
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

    private void adjustTargetSizeAndWorkSplits() {
        Set<SSTableReader> liveSet = Sets.newHashSet(filterSuspectSSTables(cfs.getSSTables(SSTableSet.CANONICAL)));

        if (liveSet.size() > (targetReadOptions.maxSSTableCount / 2))
        {
            final long totalSize = liveSet.stream().mapToLong(SSTableReader::onDiskLength).sum();
            final double targetSSTableSize = Math.max(targetSSTableSizeBytes,
                                                      totalSize / (targetReadOptions.maxSSTableCount / 2));
            // So we don't have oddly sized tables, just always change in increments of 256 MiB
            final long next256 = 256 * 1000 * 1000;
            final long nextSize = ((long) (Math.ceil(targetSSTableSize / next256))) * (next256);
            if (nextSize > this.targetSSTableSizeBytes) {
                logger.debug("TRCS adjusting target sstable size to {}MiB due to observing {}MiB dataset over {} SSTables",
                             nextSize / 1024 / 1024,
                             totalSize / 1024 / 1024,
                             liveSet.size());
                this.targetSSTableSizeBytes = nextSize;
            }
        }

        // Power of 2 and only increasing so we don't have the boundaries drastically shifting around
        // as data grows. When we do shift an overlap boundary we want to do so significantly so as not to
        // disturb high density levels. Note that if data shrinks significantly and we shift back down
        // this could cause neighboring runs to compact together.
        long datasetSize = liveSet.stream().mapToLong(SSTableReader::uncompressedLength).sum();
        int targetSplits = (int) Math.min(65536, Math.max(1, datasetSize / targetReadOptions.targetWorkSizeInBytes));
        int nextSplits = 1 << 32 - Integer.numberOfLeadingZeros(targetSplits - 1);
        if (nextSplits > targetRangeSpits)
        {
            logger.debug("TRCS adjusting work splits to {} due to datasetSize={} and targetWorkSize={}",
                         nextSplits,
                         datasetSize,
                         targetReadOptions.targetWorkSizeInBytes);
            targetRangeSpits = nextSplits;
        }
    }

    private Pair<List<SSTableReader>, Integer> getSSTablesForCompaction(int gcBefore)
    {
        logger.trace("TRCS choosing candidates: {}", this);

        int minThreshold = cfs.getMinimumCompactionThreshold();
        // Adjust the target size to meet the max count goal. Do this before we hit the goal so that
        // we can get "ahead" of the problem and hopefully do not need to re-write sorted runs later
        // Also adjusts the split ranges based on the observed data footprint to keep our unit of
        // compaction work roughy even.
        adjustTargetSizeAndWorkSplits();

        Set<SSTableReader> candidatesSet = Sets.newHashSet(filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains)));

        // Handle freshly flushed data first, in order to not do a bunch of unneccesary compaction
        // early on we have to gather up a bunch of data before yielding our first sorted run
        Pair<List<SSTableReader>, List<SSTableReader>> filteredFlushes = findNewlyFlushedSSTables(candidatesSet);
        List<SSTableReader> flushedSSTables = filteredFlushes.left;
        List<SSTableReader> sstablesToCompact = filteredFlushes.right;
        if (sstablesToCompact.size() > 1)
        {
            logger.debug("TRCS normalizing newly flushed SSTables: {}", sstablesToCompact);
            return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));
        }

        candidatesSet.removeIf(s -> s.getSSTableLevel() == 0);
        if (candidatesSet.size() == 0) return Pair.create(sstablesToCompact, 0);
        if (candidatesSet.hashCode() != cachedTree.left)
        {
            cachedTree = Pair.create(candidatesSet.hashCode(), SSTableIntervalTree.build(candidatesSet));
        }

        List<SSTableReader> sortedByFirst = Lists.newArrayList(candidatesSet);
        sortedByFirst.sort(Comparator.comparing(s -> s.first));

        List<AbstractBounds<PartitionPosition>> bounds = sortedByFirst.stream()
                                                                      .map(s -> AbstractBounds.bounds(s.first, true, s.last, true))
                                                                      .collect(Collectors.toList());
        double intervalSize = calculateIntervalSize(bounds);
        logger.trace("TRCS interval size: {}", intervalSize);

        // Handle sorted runs that are not dense enough yet, keep consolidating such runs until
        // they get big enough to enter the "levels". Basically just doing STCS at this point but
        // with an enforcement that we don't exceed maxReadPerRead
        Pair<List<SortedRun>, Set<SSTableReader>> filteredRuns = filterSparseSortedRuns(candidatesSet, intervalSize);
        int numSmallRuns = flushedSSTables.size() + filteredRuns.left.size();

        long flushedAge = flushedSSTables.stream().mapToLong(s -> s.getCreationTimeFor(Component.DATA)).min().orElse(Long.MAX_VALUE);
        long sparseAge = filteredRuns.left.stream().mapToLong(sr -> sr.createdAt).min().orElse(Long.MAX_VALUE);
        long smallRunAge = Math.min(flushedAge, sparseAge);

        if (smallRunAge < majorCompactionGoalSetAt ||
            numSmallRuns > minThreshold ||
            (numSmallRuns > 1 && numSmallRuns + targetReadOptions.targetReadPerRead > targetReadOptions.maxReadPerRead))
        {
            List<SortedRun> candidate = null;

            // If we've got a lot of small runs we need to compact them regardless of the large runs
            if (numSmallRuns + targetReadOptions.targetReadPerRead > targetReadOptions.maxReadPerRead ||
                smallRunAge < majorCompactionGoalSetAt)
            {
                candidate = filteredRuns.left;
            }
            else
            {
                List<Pair<SortedRun, Long>> sparseRunsBySize = filteredRuns.left.stream()
                                                                                .map(sr -> Pair.create(sr, sr.uncompressedSizeInBytes))
                                                                                .collect(Collectors.toList());
                List<List<SortedRun>> buckets = SizeTieredCompactionStrategy.getBuckets(sparseRunsBySize,
                                                                                        targetReadOptions.levelBucketHigh,
                                                                                        targetReadOptions.levelBucketLow,
                                                                                        0);
                for (List<SortedRun> bucket : buckets)
                {
                    if (bucket.size() >= minThreshold)
                    {
                        if (candidate == null) candidate = bucket;
                        long candidateSize = candidate.stream().mapToLong(sr -> sr.uncompressedSizeInBytes).sum();
                        long bucketSize = bucket.stream().mapToLong(sr -> sr.uncompressedSizeInBytes).sum();
                        if (bucketSize > candidateSize) candidate = bucket;
                    }
                }
            }

            if (candidate != null)
            {
                sstablesToCompact = sortedRunToSSTables(candidate);
                sstablesToCompact.addAll(flushedSSTables);
                logger.debug("TRCS consolidating sparse runs: {}", sstablesToCompact);
                return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));
            }
        }

        // The Primary "Target Read" part of the algorithm. Now we consider levels to be sorted runs
        // and look for overlapping runs, bucketed by density (size / |range|)
        sstablesToCompact = sortedRunToSSTables(findOverlappingSSTables(filteredRuns.left,
                                                                        filteredRuns.right,
                                                                        bounds));
        if (sstablesToCompact.size() > 1)
        {
            long readReduction = sstablesToCompact.stream().mapToInt(SSTableReader::getSSTableLevel).distinct().count();
            logger.debug("TRCS compacting {} sorted runs to reduce reads per read: {}", readReduction, sstablesToCompact);
            return Pair.create(sstablesToCompact, getLevel(sstablesToCompact));
        }


        // If we have exceeded the target number of SSTables, re-write sorted runs into larger files
        // to keep the number of SSTables (-> files) reasonable even as datasets scale to TiBs of data
        if (sstables.size() > targetReadOptions.maxSSTableCount)
        {
            sstablesToCompact = findSmallSSTables(candidatesSet);
            if (sstablesToCompact.size() > 1)
            {
                logger.debug("TRCS re-writing to meet max_sstable_count: {}", sstablesToCompact);
                return Pair.create(sstablesToCompact, sstablesToCompact.get(0).getSSTableLevel());
            }
        }

        // If we get here then check if tombstone compaction is available and do that
        sstablesToCompact = findTombstoneEligibleSSTables(gcBefore, candidatesSet);
        if (sstablesToCompact.size() > 1)
        {
            logger.debug("TRCS re-writing to purge tombstones: {}", sstablesToCompact);
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
            if (sstablesToCompact.isEmpty())
                return null;

            int level = compactionTarget.right;
            long targetSize = targetSSTableSizeBytes;

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

        logger.info("TargetReadCompactionStrategy does not support blocking full compactions, " +
                    "informing background tasks to begin full compactions of any SSTables older than {} attempting" +
                    "to spread them over the {} of {} seconds.",
                    currentTime,
                    TargetReadCompactionStrategyOptions.TARGET_REWRITE_INTERVAL_SECS,
                    targetReadOptions.targetRewriteIntervalSeconds);
        majorCompactionGoalSetAt = currentTime;
        nextMajorCompactionTime = currentTime;

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
        return String.format("TargetReadCompactionStrategy[%d mb segment:%d mb compactions:%d splits]",
                             targetSSTableSizeBytes / (1024 * 1024),
                             targetReadOptions.targetWorkSizeInBytes / (1024 * 1024),
                             targetRangeSpits);
    }
}
