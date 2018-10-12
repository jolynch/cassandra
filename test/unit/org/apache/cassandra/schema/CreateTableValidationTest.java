/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class CreateTableValidationTest extends CQLTester
{
    private static final String KEYSPACE1 = "CreateTableValidationTest";

    @Test
    public void testInvalidBloomFilterFPRatio() throws Throwable
    {
        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001");
            fail("Expected an fp chance of 0.0000001 to be rejected");
        }
        catch (ConfigurationException exc) { }

        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1");
            fail("Expected an fp chance of 1.1 to be rejected");
        }
        catch (ConfigurationException exc) { }

        // sanity check
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1");
    }

    @Test
    public void testCanCreateFromDescribe()
    {
        // Describe is implemented client side not server side, so we can't just run a local DESCRIBE
        // and then ensure that we can create that again. So ... we copy pasta a describe call
        // See CASSANDRA-14822 for why the read_repair_chances are still there
        String describeOutput = "CREATE TABLE %s (\n" +
                                "    key text PRIMARY KEY,\n" +
                                "    value text\n" +
                                ") WITH bloom_filter_fp_chance = 0.01\n" +
                                "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                                "    AND comment = ''\n" +
                                "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                                "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                                "    AND crc_check_chance = 1.0\n" +
                                "    AND dclocal_read_repair_chance = 0.0\n" +
                                "    AND default_time_to_live = 0\n" +
                                "    AND gc_grace_seconds = 864000\n" +
                                "    AND max_index_interval = 2048\n" +
                                "    AND memtable_flush_period_in_ms = 0\n" +
                                "    AND min_index_interval = 128\n" +
                                "    AND read_repair_chance = 0.0\n" +
                                "    AND speculative_retry = '99p';";
        createTable(describeOutput);
    }
}

