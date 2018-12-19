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
import java.util.List;

import com.google.common.util.concurrent.AbstractFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tracing.Tracing;

/**
 * SyncTask will calculate the difference of MerkleTree between two nodes
 * and perform necessary operation to repair replica.
 */
public abstract class SyncTask extends AbstractFuture<SyncStat> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(SyncTask.class);

    protected final RepairJobDesc desc;
    protected final InetAddress endpoint1;
    protected final InetAddress endpoint2;
    protected final List<Range<Token>> rangesToSync;

    protected volatile SyncStat stat;

    public SyncTask(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, List<Range<Token>> rangesToSync)
    {
        this.desc = desc;
        this.endpoint1 = endpoint1;
        this.endpoint2 = endpoint2;
        this.rangesToSync = rangesToSync;
    }

    /**
     * Compares trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        stat = new SyncStat(new NodePair(endpoint1, endpoint2), rangesToSync.size());

        // choose a repair method based on the significance of the difference
        String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", desc.sessionId, endpoint1, endpoint2, desc.columnFamily);
        if (rangesToSync.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            Tracing.traceRepair("Endpoint {} is consistent with {} for {}", endpoint1, endpoint2, desc.columnFamily);
            set(stat);
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + rangesToSync.size() + " range(s) out of sync"));
        Tracing.traceRepair("Endpoint {} has {} range(s) out of sync with {} for {}", endpoint1, rangesToSync.size(), endpoint2, desc.columnFamily);
        startSync(rangesToSync);
    }

    public SyncStat getCurrentStat()
    {
        return stat;
    }

    protected abstract void startSync(List<Range<Token>> differences);
}
