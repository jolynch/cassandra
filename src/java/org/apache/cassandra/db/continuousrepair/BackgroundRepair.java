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
package org.apache.cassandra.db.continuousrepair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;


/**
 * Run repair all the time in the background
 */
public class BackgroundRepair implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundRepair.class);
    public static final BackgroundRepair instance = new BackgroundRepair();

    private static int maxRRs;

    public void run()
    {
        initiateReadRepair();
        // TODO table config or jmx or auto-deducing or somethin
        maxRRs = 2000;
    }

    private void initiateReadRepair()
    {
        String queryRowsForRepair = String.format("SELECT * FROM system.%s WHERE cfId = ?", SystemKeyspace.READ_REPAIR_HINTS);
        String removeRowsForRepair = String.format(
            "DELETE FROM system.%s WHERE cfId = ? AND mutation_primary_key = ? AND mutation_ts = ?",
            SystemKeyspace.READ_REPAIR_HINTS
        );
        int done = 0;
        for (String ks : Schema.instance.getKeyspaces())
        {
            for (CFMetaData cfmd : Schema.instance.getTablesAndViews(ks))
            {
                UntypedResultSet needsRepair = executeInternalWithPaging(queryRowsForRepair, 200, cfmd.cfId);
                for (UntypedResultSet.Row row : needsRepair)
                {
                    if (done >= maxRRs) {
                        return;
                    }
                    DecoratedKey key = cfmd.decorateKey(row.getBlob("mutation_primary_key"));
                    int nowInSec = FBUtilities.nowInSeconds();
                    SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfmd, nowInSec, key);
                    PartitionIterator results = StorageProxy.read(SinglePartitionReadCommand.Group.one(cmd), ConsistencyLevel.ALL, null, System.nanoTime());
                    try (RowIterator result = PartitionIterators.getOnlyElement(results, cmd))
                    {
                        //logger.info("deleting row {}", key);
                        executeInternal(
                            removeRowsForRepair,
                            row.getUUID("cfid"), row.getBlob("mutation_primary_key"), row.getUUID("mutation_ts"));
                        result.forEachRemaining(r -> {});
                    } catch (Exception e) {
                        logger.error("Could not repair key: {}", e);
                    }
                    done++;
                }

            }
        }

    }
}
