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

import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.UUIDGen;

public class MutationTracker
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundRepair.class);
    public static final MutationTracker instance = new MutationTracker();

    private final ExecutorService offload;

    private MutationTracker()
    {
        offload = new ThreadPoolExecutor(
            1, 4, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10), new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void recordPartitionUpdate(PartitionUpdate upd, long mutationTime) {
        offload.submit(new Runnable()
        {
            public void run()
            {
                // logger.info("Writing dirty bit for {} table", upd.metadata().cfName);
                List<UUID> endpoints = Lists.newArrayList(StorageService.instance.getNaturalAndPendingEndpoints(
                upd.metadata().ksName, upd.partitionKey().getToken()).iterator()).stream()
                                            .map(StorageService.instance::getHostIdForEndpoint)
                                            .collect(Collectors.toList());

                PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(SystemKeyspace.ReadRepairHints, upd.metadata().cfId);
                builder.row(upd.partitionKey().getKey(), UUIDGen.getTimeUUID(mutationTime))
                       .timestamp(mutationTime)
                       .add("dirty_replicas", new HashSet<>(endpoints));
                // I believe that this does not need to be durable because it is derived from an existing
                // durable write, might have to tie into flushes to guarantee this but ...
                builder.buildAsMutation().apply(false);
            }
        });
    }
}