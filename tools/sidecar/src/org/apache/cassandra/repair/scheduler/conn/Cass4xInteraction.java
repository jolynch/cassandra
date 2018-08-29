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

package org.apache.cassandra.repair.scheduler.conn;

import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.scheduler.TaskUtil;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.entity.RepairSplitStrategy;
import org.apache.cassandra.repair.scheduler.entity.TableRepairRangeContext;
import org.apache.cassandra.utils.EstimatedHistogram;

import static com.google.common.base.Preconditions.checkNotNull;

public class Cass4xInteraction extends CassandraInteractionBase
{
    public Cass4xInteraction(TaskSchedulerConfig config)
    {
        super(config);
    }

    //==================================================================================================================
    // A quick note on Endpoints.
    // The definition of "Endpoint" has changed in 4.0 to be an address with a port e.g. 127.0.0.1:9042,
    // prior it was just an address e.g. 127.0.0.1. This means that all the following endpoint methods are
    // now coupled to the version. The rest of Repair Scheduler doesn't care though since all interactoin goes
    // through these methods, Strings are just Strings as far as the rest is concerned. Also we never persist
    // Endpoints anywhere (node ids are just node ids, not Endpoints).
    //==================================================================================================================

    @Override
    public Map<String, String> getEndpointToHostIdMap()
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        return ssProxy.getEndpointWithPortToHostId();
    }


    @Override
    public Map<Range<Token>, List<String>> getRangeToEndpointMap(String keyspace)
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        Map<List<String>, List<String>> rangeToEndpointMap = ssProxy.getRangeToEndpointWithPortMap(keyspace);
        // Only consider valid ranges, ignoring incorrect datacenter definitions
        // Or zero replication factor keyspaces
        return rangeToEndpointMap.entrySet().stream()
                                            .filter(e -> e.getKey().size() == 2)
                                            .filter(e -> e.getValue().size() > 0)
                                            .collect(Collectors.toMap(entry ->
                                                                      tokenRangeFromStrings(entry.getKey().get(0),
                                                                                            entry.getKey().get(1)),
                                                                      Map.Entry::getValue));
    }

    @Override
    public String getLocalEndpoint()
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        return ssProxy.getHostIdToEndpointWithPort().get(ssProxy.getLocalHostId());
    }

    @Override
    public Map<String, String> getSimpleStates()
    {
        return fdProxy.getSimpleStatesWithPort();
    }


    //==================================================================================================================
    // A quick note on Token range splits.
    //
    // The proper way to generate token range splits varies wildly between versions. In 2.1, 3.0 and 3.11 you can use
    // the Thrift describe_splits_ex method which works really well but only supports splitting on partitions. In 3.0,
    // 3.11 and 4.0 you can use the system.size_estimates table which sort of works, but is not very accurate.
    //
    // So we just implement a mostly version agnostic version based on Token.size and IPartitioner.split. The first
    // API is available in 3.0+ and the second in 3.11+. Just copying an dpasting the IPartitioner.split methods is
    // most likely the easiest path forward for 3.0. For 3.11 and 4.0 this is a pretty robust technique.
    //
    // Why not size_estimates? Because in my testing it is less accurate than just weighting the table metrics. The
    // table metrics are much more accurate in all versions of Cassandra tested (2.1, 3.0, 3.11, 4.0) and both
    // RandomPartitioner and Murmur3Partioner do a good enough job of equally hashing keys that the estimation
    // technique here appears quite good.
    //==================================================================================================================


    @Override
    public List<Range<Token>> getTokenRangeSplits(TableRepairRangeContext rangeContext, Range<Token> range, RepairSplitStrategy strategy)
    {
        TaskUtil.checkState(partitioner != null, "Partitioner not setup yet");
        List<Range<Token>> splits = new ArrayList<>(Collections.singleton(range));

        try
        {
            partitioner.getMaximumToken();
            range.left.size(range.right);
            new BigInteger(range.left.toString());
            new BigInteger(range.right.toString());
        }
        catch (UnsupportedOperationException | NumberFormatException e)
        {
            logger.debug("{} partitioner does not support splitting ranges, not splitting this range.",
                         partitioner.getClass().getCanonicalName());
            return splits;
        }

        List<Long> countAndSize = getPartitionCountAndSize(rangeContext, range);

        long partitionsInRange = countAndSize.get(0);
        long kilobytesInRange = countAndSize.get(1) / (1024);

        if (partitionsInRange == 0 || strategy.getStrategy() == RepairSplitStrategy.Strategy.DISABLED)
        {
            logger.debug("Zero partitions in range or splits disabled for {}.{}, not splitting this range.",
                         rangeContext.keyspace, rangeContext.table);
            return splits;
        }

        int numSplits = 1;

        if (strategy.getStrategy() == RepairSplitStrategy.Strategy.ADAPTIVE)
        {
            // TODO: Can we facotr out the magic constants or provide a pluggable calculation strategy?
            // This is an overstreaming factor of 32x with the default Merkle tree leaf size
            final int repairMaxPartitionCount = 1048576;

            // First get a number of splits based on partitions
            numSplits = (int) Math.max(1, partitionsInRange / repairMaxPartitionCount);

            // Then adjust for very wide partitions, allowing splits due to data size as well.
            // Cassandra repairs very slowly, we (Netflix) observe around ~2 megabytes / second
            final int repairRateKBps = 2048;
            final int repairMaxSizeKB = (repairRateKBps * (config.getDefaultTaskTimeoutInS()));
            numSplits = Math.max(numSplits, (int) Math.max(1, kilobytesInRange / repairMaxSizeKB));

            // Ultimately, we also have to balance against too many small sstables overloading compaction.
            // if we're doing more than 150 splits with the default number of merkle tree leaves we will
            // likely create way too many sstables
            numSplits = Math.min(numSplits, 150);
        }
        else if (strategy.getStrategy() == RepairSplitStrategy.Strategy.SIZE)
        {
            numSplits = Math.max(1, (int) (kilobytesInRange / strategy.getValue()));
        }
        else if (strategy.getStrategy() == RepairSplitStrategy.Strategy.PARTITION)
        {
            numSplits = Math.max(1, (int) (partitionsInRange / strategy.getValue()));
        }

        if (numSplits == 1)
        {
            logger.debug("No need to split {}.{} range {}", rangeContext.keyspace, rangeContext.table, range);
        }
        else if (strategy.isDryRun())
        {
            logger.info("DRYRUN: Would have split %s.%s %s into %d subranges", rangeContext.keyspace, rangeContext.table,
                        range, numSplits);
        }
        else
        {
            Range<Token> initialRange = splits.remove(0);
            Token end = initialRange.right;

            Token left = range.left;
            for (double i = 1; i < numSplits; i++)
            {
                // This is only supported in 3.11 and 4.0, Cass3x will have to do it differently
                Token split = partitioner.split(range.left, range.right, i / numSplits);
                Token right = split;
                splits.add(new Range<>(left, right));
                left = right;
            }
            splits.add(new Range<>(left, end));
        }

        return splits;
    }

    /**
     * Ideally we'd like to use system.size_estimates but that is not a robust implementation on most version
     * of Cassandra. On the other hand the column family metrics are quite robust and have existed for many
     * versions so while they may not be exactly accurate for vnodes we still use them.
     *
     * @param keyspace The keyspace to get context on
     * @param table The table to get context on
     * @return A filled TableRepairRangeContext which can be passed around to not repeat these operations
     */
    @Override
    public TableRepairRangeContext getTableRepairRangeContext(String keyspace, String table)
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");

        long estimatedCount = 1;
        long estimatedSize = 0;

        List<Range<Token>> localRanges =  getTokenRanges(keyspace, false);
        List<Range<Token>> primaryRanges =  getTokenRanges(keyspace, true);
        try
        {
            ObjectName epcObjectName = new ObjectName(
            String.format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=EstimatedPartitionCount",
                          keyspace, table));
            estimatedCount = (long) JMX.newMBeanProxy(mbeanServer, epcObjectName,
                                                      CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
            ObjectName epsObjectName = new ObjectName(
            String.format("org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=EstimatedPartitionSizeHistogram",
                          keyspace, table));
            long[] epsHistogram = (long[]) JMX.newMBeanProxy(mbeanServer, epsObjectName,
                                                             CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
            if (epsHistogram.length > 0)
            {
                EstimatedHistogram eh = new EstimatedHistogram(epsHistogram);
                estimatedSize = eh.mean() * estimatedCount;
            }

        }
        catch (MalformedObjectNameException e)
        {
            logger.error("Failed to get partition counts using metrics, assuming one partition with zero size", e);
        }

        return new TableRepairRangeContext(keyspace, table, localRanges, primaryRanges, estimatedCount, estimatedSize);
    }

    /**
     * Given a fully provided rangeContext, tries to estimate the number of partitions and size in bytes
     * oif the provided range. Note that the best way to calculate this varies between Cassandra versions, e.g.
     * Cassandra 2.1 and 3.0 probably want to use the provided rangeContext, while 4.0+ may want to use the
     * size_estimates table.
     *
     * @param rangeContext A filled out TableRepairRangeContext to use when calculating the partitions
     * @param range The range to calculate partition count and size for
     * @return A list containing two longs, the first is the estimated partition count in the provided token range
     * and the second is the estimated size in bytes of the range.
     */
    @VisibleForTesting
    List<Long> getPartitionCountAndSize(TableRepairRangeContext rangeContext, Range<Token> range)
    {
        List<Long> partitionCountAndSize = new ArrayList<>(Collections.nCopies(2, 0L));
        long countEstimate, sizeEstimate;

        if (rangeContext.localRanges.size() > 1)
        {
            double totalOwnedRangeSize = rangeContext.localRanges.stream()
                                                                 .mapToDouble(r -> r.left.size(r.right))
                                                                 .sum();

            double thisRangeSize = range.left.size(range.right);

            double rangeFraction = thisRangeSize / Math.max(0.0001, totalOwnedRangeSize);

            countEstimate = Math.max(1, (long) ((double) rangeContext.estimatedLocalPartitions * rangeFraction));
            sizeEstimate = (long) (rangeContext.estimatedLocalSizeBytes * rangeFraction);
        }
        else
        {
            countEstimate = Math.max(1, rangeContext.estimatedLocalPartitions);
            sizeEstimate = Math.max(0, rangeContext.estimatedLocalSizeBytes);
        }

        partitionCountAndSize.set(0, countEstimate);
        partitionCountAndSize.set(1, sizeEstimate);
        return partitionCountAndSize;
    }

    @Override
    public boolean isCassandraHealthy()
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");

        // Check if the node is available for clients
        try
        {
            return (ssProxy.isGossipRunning() &&
                    ssProxy.isNativeTransportRunning());
        }
        catch (Exception e)
        {
            logger.error("Error determining if Cassandra is healthy", e);
        }
        // If uncertain, assume it's running
        return true;
    }

    @Override
    public boolean isRepairRunning(boolean defaultValue)
    {
        try
        {
            ObjectName epcObjectName = new ObjectName(
                "org.apache.cassandra.metrics:name=ActiveTasks,path=internal,scope=Repair-Task,type=ThreadPools"
            );

            int estimatedRunnable = (int) JMX.newMBeanProxy(mbeanServer, epcObjectName,
                                                            CassandraMetricsRegistry.JmxGaugeMBean.class).getValue();
            return estimatedRunnable > 0;
        }
        catch (MalformedObjectNameException | UndeclaredThrowableException e)
        {
            logger.error("Couldn't determine if repair was running, returning " + defaultValue, e);
        }
        return defaultValue;
    }

    @Override
    public void cancelAllRepairs()
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        try
        {
            ssProxy.forceTerminateAllRepairSessions();
        }
        catch (RuntimeException exp)
        {
            logger.warn("Failed to terminate all repair sessions. Cassandra might be down or unresponsive");
        }
    }

    public int triggerRepair(List<Range<Token>> ranges, RepairParallelism repairParallelism, boolean fullRepair, String keyspace, String table)
    {
        TaskUtil.checkState(tryGetConnection(), "JMXConnection is broken or not able to connect");
        checkNotNull(ssProxy, "ssProxy is not properly connected");
        String msg = String.format("Triggering [%s] repair for table [%s.%s] with ranges [%s] and repair parallelism [%s]",
                                   fullRepair ? "full" : "incremental",
                                   keyspace, table, ranges, repairParallelism);
        logger.info(msg);

        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, repairParallelism.toString());
        options.put(RepairOption.PRIMARY_RANGE_KEY, "true");
        options.put(RepairOption.COLUMNFAMILIES_KEY, table);

        // Add in ranges if it's not the whole ring
        if (ranges.size() > 0 && !ranges.get(0).left.equals(ranges.get(0).right))
        {
            String rangeDesc = ranges.stream()
                                     .map(r -> String.format("%s:%s", r.left, r.right))
                                     .collect(Collectors.joining(","));
            options.put(RepairOption.RANGES_KEY, rangeDesc);
        }

        if (fullRepair)
        {
            options.put(RepairOption.SUB_RANGE_REPAIR_KEY, "true");
            options.put(RepairOption.INCREMENTAL_KEY, "false");
        }
        else
        {
            options.put(RepairOption.SUB_RANGE_REPAIR_KEY, "false");
            options.put(RepairOption.INCREMENTAL_KEY, "true");
        }
        return ssProxy.repairAsync(keyspace, options);
    }
}
