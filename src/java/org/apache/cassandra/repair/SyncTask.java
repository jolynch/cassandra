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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MerkleTrees;

/**
 * SyncTask will calculate the difference of MerkleTree between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    protected final RepairJobDesc desc;
    protected final InetAddress firstEndpoint;
    protected final InetAddress secondEndpoint;

    // Use a List so that we can allow the (potentially large) trees to collected
    // after the difference calculation is complete in run(). Intentionally private
    // so that subclassess never get access to the trees (which they shouldn't use).
    private final List<TreeResponse> responsesToDiff;

    protected volatile SyncStat stat;

    public SyncTask(RepairJobDesc desc, TreeResponse r1, TreeResponse r2)
    {
        this.desc = desc;
        this.firstEndpoint = r1.endpoint;
        this.secondEndpoint = r2.endpoint;

        this.responsesToDiff = new ArrayList<>();
        this.responsesToDiff.add(r1);
        this.responsesToDiff.add(r2);
    }

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        // This is the potentially CPU intensive part, so we do it here rather than before to get full parallelism
        List<Range<Token>> differences = MerkleTrees.difference(responsesToDiff.get(0).trees, responsesToDiff.get(1).trees);

        // Allow GC of the Merkle trees, see CASSANDRA-14096
        responsesToDiff.clear();

        stat = new SyncStat(new NodePair(firstEndpoint, secondEndpoint), differences.size());

        // choose a repair method based on the significance of the difference
        String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", desc.sessionId, firstEndpoint, secondEndpoint, desc.columnFamily);
        if (differences.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}", firstEndpoint, secondEndpoint, desc.columnFamily);
            set(stat);
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
        Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", firstEndpoint, differences.size(), secondEndpoint, desc.columnFamily);
        startSync(differences);
    }

    public SyncStat getCurrentStat()
    {
        return stat;
    }

    protected abstract void startSync(List<Range<Token>> differences);
}
