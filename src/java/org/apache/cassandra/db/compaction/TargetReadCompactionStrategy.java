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
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.StorageService;
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

    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
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
        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        Set<SSTableReader> sizeCandidates = Sets.newHashSet(candidates);
        List<SSTableReader> tooSmall = sizeCandidates.stream()
                                                     .filter(s -> s.onDiskLength() < targetSSTableSize)
                                                     .collect(Collectors.toList());

        if (tooSmall.size() >= minThreshold)
            return tooSmall.stream().limit(maxThreshold).collect(Collectors.toList());

        if (sizeCandidates.size() > targetReadOptions.maxSSTableCount)
        {
            long totalSize = sizeCandidates.stream().mapToLong(SSTableReader::onDiskLength).sum();
            targetSSTableSize = (totalSize / targetReadOptions.maxSSTableCount);
        }

        ownedRanges = new ArrayList<>(StorageService.instance.getLocalAndPendingRanges(cfs.keyspace.getName()));

        List<DecoratedKey> keys = new ArrayList<>();
        int targetKeys = 100;

        for (int i = 0; i < 10 && keys.size() < targetKeys; i++)
        {
            targetRange = (targetRange + 1) % ownedRanges.size();
            Iterables.addAll(keys, cfs.keySamples(ownedRanges.get(targetRange % ownedRanges.size())));
        }

        List<Set<SSTableReader>> overlaps = new ArrayList<>();

        Set<SSTableReader> overlapping = new HashSet<>();
        for (DecoratedKey key : keys)
        {
            overlapping.clear();
            for (SSTableReader sstable : sizeCandidates)
            {
                if (sstable.getPosition(key, SSTableReader.Operator.EQ, false) != null)
                {
                    overlapping.add(sstable);
                }
            }
            if (overlapping.size() >= targetReadOptions.targetOverlap)
                overlaps.add(ImmutableSet.copyOf(overlapping));
        }

        // Greedily take the worst offenders and compact them.
        Set<SSTableReader> result = new HashSet<>();
        overlaps.sort(Comparator.comparing(Set::size));
        for (Set<SSTableReader> sstables : overlaps)
        {
            for (SSTableReader sstable : sstables)
            {
                result.add(sstable);
                if (result.size() >= maxThreshold)
                    break;
            }
            if (result.size() >= maxThreshold) {
                // TODO: fix estimatedRemainingTasks here
                break;
            }
        }

        if (result.size() > 0)
        {
            return new ArrayList<>(result);
        }

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
        for (SSTableReader sstable : sstables)
            maxLevel = Math.max(maxLevel, sstable.getSSTableLevel());
        return maxLevel + 1;
    }

    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        while (true)
        {
            List<SSTableReader> sstablesToCompact = getNextBackgroundSSTables(gcBefore);

            if (sstablesToCompact.isEmpty())
                return null;

            LifecycleTransaction transaction = cfs.getTracker().tryModify(sstablesToCompact, OperationType.COMPACTION);
            if (transaction != null)
                return new LeveledCompactionTask(cfs, transaction, getLevel(sstablesToCompact),
                                                 gcBefore, targetSSTableSize, false);
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
