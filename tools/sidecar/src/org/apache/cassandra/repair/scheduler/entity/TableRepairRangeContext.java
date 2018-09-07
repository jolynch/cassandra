package org.apache.cassandra.repair.scheduler.entity;

import java.util.List;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * While manipulating token ranges we frequently need this information and getting it every time is expensive,
 * so we allow wrapping it up into this context object and passing it around.
 */
public class TableRepairRangeContext
{
    public final String keyspace;
    public final String table;
    public final List<Range<Token>> localRanges;
    public final List<Range<Token>> primaryRanges;
    public final long estimatedLocalPartitions;
    public final long estimatedLocalSizeBytes;

    public TableRepairRangeContext(String keyspace, String table,
                                   List<Range<Token>> localRanges, List<Range<Token>> primaryRanges,
                                   long estimatedLocalPartitions, long estimatedLocalSizeBytes)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.localRanges = localRanges;
        this.primaryRanges = primaryRanges;
        this.estimatedLocalPartitions = estimatedLocalPartitions;
        this.estimatedLocalSizeBytes = estimatedLocalSizeBytes;
    }
}
