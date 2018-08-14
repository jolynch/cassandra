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

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.NotificationListener;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.scheduler.entity.RepairSplitStrategy;
import org.apache.cassandra.repair.scheduler.entity.TableRepairRangeContext;

/**
 * The layer of abstraction on top of Cassandra's management APIs. As the management APIs change frequently
 * between versions this interface serves to allow us to plug in support for different versions without significant
 * coupling to those management APIs.
 *
 * We try to encapsulate all possible repair related management  APIs that change a lot between versions of Cassandra
 * into this interface. Note that this doesn't encapsulate the entire Cassandra dependency, indeed the rest of the
 * Repair Scheduler depends on e.g. the implementation of Token, Range, etc ... but those APIs rarely change compared
 * to the ones covered by this interface.
 */
public interface CassandraInteraction
{
    //==================================================================================================================
    // Parts of the interface that are unlikely to change too much by version
    //==================================================================================================================

    /**
     * Sets up the provided listener to receive Repair related notifications. The format of the specification is
     * that provided by Cassandra itself.
     * @param listener
     */
    void addRepairNotificationListener(NotificationListener listener);

    /**
     * Removes the provided listener from receiving Repair related notifications.
     * @param listener
     */
    void removeRepairNotificationListener(NotificationListener listener);

    /**
     * Returns a Set of {@link javax.management.NotificationListener} objects that are currently listening to
     * Repair related notifications.
     * @return The Set of {@link javax.management.NotificationListener} objects that are currently listening to Repair
     * related notifications.
     */
    Set<NotificationListener> getOutstandingRepairNotificationListeners();

    /**
     * Returns the local Cassandra node's Host ID (e.g. 43262282-a8a9-486a-9b90-1f83e7605129). This is used to
     * uniquely identify this host.
     * @return A String representation of the local Cassandra node's Host ID.
     */
    String getLocalHostId();

    /**
     * Returns a String representation of the local Cassandra load (aka disk usage).
     * @return A String representation of local Cassandra load (disk usage).
     */
    String getLocalLoadString();

    /**
     * Returns the name of the cluster that the local Cassandra node belongs to.
     * @return The name of the cluster that the local Cassandra node belongs to.
     */
    String getClusterName();

    /**
     * Using the local partitioner, constructs a Token Range and returns it. Use this instead of coupling
     * to the underlying partitioner. This is useful for converting between the Strings found in the database
     * with the actual Token Range objects that most methods assume.
     *
     * @param leftToken The String representation of the left token
     * @param rightToken The String representation of the right token
     * @return The local partitioner representation of the provided Token Range
     */
    Range<Token> tokenRangeFromStrings(String leftToken, String rightToken);

    /**
     * Returns the List of Token Ranges for a given keyspace. If {@code primaryOnly} is true then only Primary
     * token ranges (meaning that this local node's endpoint appears first in the replica mapping) are returned.
     *
     * Note that implementations of this method are expected to filter out any non replicated ranges, so if
     * the keyspace has an invalid definition, the result of this method will be empty.
     *
     * @param keyspace The keyspace to get local Token Ranges for.
     * @param primaryOnly Whether all local Token Ranges or just primary Token Ranges should be returned
     * @return A List of Token Ranges for the provided keyspace owned by the local Cassandra node.
     */
    List<Range<Token>> getTokenRanges(String keyspace, boolean primaryOnly);

    void triggerCompaction(String keyspace, String... tables);

    /**
     * Initiates local cleanup of the provided keyspace and tables
     * @param jobs
     * @param keyspace
     * @param tables
     */
    void triggerCleanup(int jobs, String keyspace, String... tables);
    
    /**
     * Determine if the interaction connection to the local node is active. Internally there is caching on this
     * call since it is called so much so if you want the most up to date information, you must explicitly tell
     * it not to use a cache.
     * @param useCache If true, allow stale data (up to ~30s), otherwise check healthy immediately.
     * @return False if the local connection to Cassandra is dead and cannot work. True indicates that the
     * connection is most likely alive, although it can be stale.
     */
    boolean isConnectionAlive(boolean useCache);

    /**
     * Initiate async background connection to Cassandra and connection monitoring
     */
    void connectAsync();

    /**
     * Synchronously connect to the local Cassandra node.
     * @return True if the connection succeeded to the local Cassandra node, false otherwise.
     */
    boolean connectSync();

    @VisibleForTesting
    void setPartitioner(IPartitioner partitioner);

    @VisibleForTesting
    void triggerFlush(String keyspace, String... tables);
    
    //==================================================================================================================
    // VERSION SPECIFIC Part of the Interface. These methods will likely differ between versions of Cassandra
    //==================================================================================================================

    /**
     * Returns a String representation of the local Cassandra endpoint. In 3.x this is the node's Address,
     * in 4.x this is the node's Address and Port (InetAddressAndPort). The value changes but it will always be
     * the same String used in any API that returns mappings of endpoints to other things (like ranges or Host IDs)
     * @return a String representation of the local Cassandra endpoint (either InetAddress or InetAddressAndPort).
     */
    String getLocalEndpoint();

    /**
     * Returns the mapping of Cassandra Endpoints to their locally deduced state (e.g. UP, DOWN, etc ...)
     * @return A Map of String to String which maps Cassandra Endpoints to their locally deduced state (e.g. UP, DOWN,
     * etc ...
     */
    Map<String, String> getSimpleStates();

    /**
     * Finds the mapping of Endpoint strings to HostID strings. Generally Endpoints are either IP Address
     * or in 4.x IP Address plus a port.
     *
     * @return A Map of String to String mapping Endpoints (InetAddress in 3.x, InetAddressAndPort in 4.x)
     * to their node ids.
     */
    Map<String, String> getEndpointToHostIdMap();

    /**
     * For a given keyspace, returns the mapping of Token Ranges to the one or more endpoints that own that
     * range. All invalid Token Ranges (either due to mispelled datacenter names or explicit RFs of zero)
     * are filtered out.
     *
     * @param keyspace The keyspace to get the replication map for
     * @return A Map of Token Ranges to a List of Endpoint Strings. An empty map means that no nodes own
     * the keyspace (typicaly due to mistyped datacenter name)
     */
    Map<Range<Token>, List<String>> getRangeToEndpointMap(String keyspace);

    /**
     * Gets token range splits of the provided range for the keyspace and table provided in the rangeContext based on
     * the passed {@link RepairSplitStrategy}.
     *
     * This should never be called on incremental repair tables, and for most vnode clusters it will generally return
     * just the provided range back (since it will probably be smaller than the split criteria) unless range joining was able to create such a big
     * range that it actually needs to be split.
     *
     * Different versions may implement this in different ways , e.g. 3.x may use the Thrift describe_splits_ex
     * API, or 4.x might use system.size_estimates ...
     *
     * @param rangeContext The full context about the keyspace/table we're generating splits for. You can get this
     *                     using {@link #getTableRepairRangeContext(String, String)} once and then pass it for all
     *                     the splits.
     * @param range The token range to split up per the strategy.
     * @param strategy A strategy for splitting this range, we support spliting on partitions, size of those partitions
     *                 or adaptively figuring out the best split to balance overstreaming/compaction load.
     * @return A List of Token Ranges that together form the originally provided range.
     */
    List<Range<Token>> getTokenRangeSplits(TableRepairRangeContext rangeContext, Range<Token> range, RepairSplitStrategy strategy);

    /**
     * Fills in partition and sizing information relevant to a table in repair. This allows the scheduler to get
     * this information once and then pass it around instead of re-calculating every time. Information included:
     *  - Local primary token ranges
     *  - All owned token ranges
     *  - Estimated partition count of all owned ranges
     *  - Estimated size in bytes for the whole table on disk
     *
     *  This is particularly useful for {@link #getTokenRangeSplits(TableRepairRangeContext, Range, RepairSplitStrategy)}
     *
     * @param keyspace The keyspace to get context on
     * @param table The table to get context on
     * @return A filled TableRepairRangeContext which can be passed around to not repeat these operations
     */
    TableRepairRangeContext getTableRepairRangeContext(String keyspace, String table);

    /**
     * Determines if the local Cassandra node is healthy for repair. This typically means that Gossip and the
     * appropriate client services (Native, Thrift, etc ) are up and running.
     *
     * @return True if the local Cassandra node is healthy and ready to repair
     */
    boolean isCassandraHealthy();

    /**
     * Determines if repair is actively running on the local Cassandra node.
     * @param defaultValue If we cannot determine the state, return this value. Allows callers to fail closed or
     *                     open depending on the caller's need.
     * @return True if another repair is running locally.
     */
    boolean isRepairRunning(boolean defaultValue);

    /**
     * Cancels all currently running repair on the local Cassandra node.
     */
    void cancelAllRepairs();

    /**
     * Runs repair on the local Cassandra node on the provided range using the provided arguments. This is the
     * superset of options provided by all the various versions of Cassandra and will naturally be implemented in
     * different ways for each.
     *
     * TODO: Consider moving this to the Map approach Cassandra 4.x is using instead
     *
     * @param ranges The token ranges to run repair on
     * @param repairParallelism What kind of repair parallelism to use
     * @param fullRepair If this repair is full or incremental, true indicates full repair.
     * @param keyspace The keyspace to run repair on
     * @param table The table to run repair on
     * @return The repair command returned by the local Cassandra Daemon. A negative number means failure.
     */
    int triggerRepair(List<Range<Token>> ranges, RepairParallelism repairParallelism, boolean fullRepair, String keyspace, String table);
}
