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
package org.apache.cassandra.cache;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AutoSavingCacheTest
{
    private static final String KEYSPACE1 = "AutoSavingCacheTest1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    private static ColumnFamilyStore cfs1;
    private static ColumnFamilyStore cfs2;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD1)
                                                      .addPartitionKey("pKey", AsciiType.instance)
                                                      .addRegularColumn("col1", AsciiType.instance)
                                                      .build(),
                                    CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD2)
                                                      .addPartitionKey("pKey", AsciiType.instance)
                                                      .addRegularColumn("col1", AsciiType.instance)
                                                      .build());

        cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1);
        cfs1.disableAutoCompaction();
        cfs2.disableAutoCompaction();
    }

    @AfterClass
    public static void cleanup()
    {
        cfs1.disableAutoCompaction();
        cfs2.disableAutoCompaction();
    }

    @Test
    public void testSerializeAndLoadKeyCache() throws Exception
    {
        doTestSerializeAndLoadKeyCache(2, cfs1);
    }

    @Test
    public void testSerializeAndLoadKeyCacheMultipleCfsAndSSTables() throws Exception
    {
        doTestSerializeAndLoadKeyCache(12, cfs1, cfs2);
    }

    private static void doTestSerializeAndLoadKeyCache(int numSSTables, ColumnFamilyStore ... columnFamilyStores) throws Exception
    {
        for (ColumnFamilyStore cfs: columnFamilyStores)
        {
            cfs.truncateBlocking();
            for (int i = 0; i < numSSTables; i++)
            {
                ColumnDefinition colDef = ColumnDefinition.regularDef(cfs.metadata, ByteBufferUtil.bytes("col1"), AsciiType.instance);
                RowUpdateBuilder rowBuilder = new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(), "key1");
                rowBuilder.add(colDef, "val1");
                rowBuilder.build().apply();
                cfs.forceBlockingFlush();
            }

            Assert.assertEquals(numSSTables, cfs.getLiveSSTables().size());

            // preheat key cache
            for (SSTableReader sstable : cfs.getLiveSSTables())
                sstable.getPosition(Util.dk("key1"), SSTableReader.Operator.EQ);
        }

        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;

        // serialize to file
        int expected = keyCache.size();
        keyCache.submitWrite(keyCache.size()).get();
        keyCache.clear();

        Assert.assertEquals(0, keyCache.size());

        // then load saved
        int numLoaded = keyCache.loadSavedAsync().get();
        for (ColumnFamilyStore cfs : columnFamilyStores)
            for (SSTableReader sstable : cfs.getLiveSSTables())
                Assert.assertNotNull(keyCache.get(new KeyCacheKey(cfs.metadata.ksAndCFName, sstable.descriptor, ByteBufferUtil.bytes("key1"))));

        Assert.assertEquals(expected, numLoaded);
    }
}
