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

package org.apache.cassandra.repair.scheduler;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.conn.Cass4xInteraction;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.dao.cass.CassDaoUtil;
import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.GuidGenerator;


public class EmbeddedUnitTestBase extends CQLTester
{
    protected RepairSchedulerContext context = null;
    protected Set<String> sysKs = ImmutableSet.of("system_auth", "system_distributed", "system_schema");
    protected static final String TEST_CLUSTER_NAME = "Test Cluster";
    protected static final String REPAIR_SCHEDULER_KS_NAME = "system_distributed";
    protected static Session session;
    protected static int repairableTables = 0;
    protected static String TEST_REPAIR_KS = "test_repair", NO_REPAIR_TBL_NAME="no_repair", SUBRANGE_TEST_TBL_NAME = "subrange_test",
    DEFAULT_TEST_TBL_NAME = "default_test", INCREMENTAL_TEST_TBL_NAME = "incremental_test";
    private static EmbeddedCassandraService cassandra;
    private static String initialJmxPortValue;
    private static final int JMX_PORT = 7188;
    private static final int MASK = (-1) >>> 1; // all ones except the sign bit
    static
    {
    }
    @BeforeClass
    public static void setup() throws IOException
    {
        // Set system property to enable JMX port on localhost for embedded server
        initialJmxPortValue = System.getProperty("cassandra.jmx.local.port");
        System.setProperty("cassandra.jmx.local.port", String.valueOf(JMX_PORT));

        SchemaLoader.prepareServer();
        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setRefreshSchemaIntervalMillis(0);
        queryOptions.setRefreshNodeIntervalMillis(0);
        queryOptions.setRefreshNodeListIntervalMillis(0);

        Cluster cluster = com.datastax.driver.core.Cluster.builder()
                                                          .addContactPoints(DatabaseDescriptor.getRpcAddress().getHostName())
                                                          .withPort(DatabaseDescriptor.getNativeTransportPort())
                                                          .withQueryOptions(queryOptions)
                                                          .build();
        session = cluster.connect();
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        cassandra.stop();
        if (initialJmxPortValue != null)
        {
            System.setProperty("cassandra.jmx.local.port", initialJmxPortValue);
        }
    }

    protected Session getSession()
    {
        return session;
    }

    public CassDaoUtil getCassDaoUtil()
    {
        return new CassDaoUtil(getContext().getConfig(), this::getSession);
    }

    public Session getLocaldb()
    {
        return getSession();
    }

    public RepairSchedulerContext getContext()
    {
        if (context == null)
            context = new TestRepairSchedulerContext();
        return context;
    }

    protected class TestRepairSchedulerConfig extends RepairSchedulerConfig
    {
        public String getRepairKeyspace()
        {
            return "system_distributed";
        }

        @Override
        public int getLocalJmxPort()
        {
            return JMX_PORT;
        }

        @Override
        public String getRepairNativeEndpoint()
        {
            return DatabaseDescriptor.getRpcAddress().getHostName() + ':' +
                   DatabaseDescriptor.getNativeTransportPort();
        }

        @Override
        public List<String> getRepairStatePersistenceEndpoints()
        {
            return Collections.singletonList(DatabaseDescriptor.getRpcAddress().getHostName() + ':' +
                                             DatabaseDescriptor.getNativeTransportPort());
        }

        @Override
        public ConsistencyLevel getReadCl()
        {
            return ConsistencyLevel.LOCAL_ONE;
        }

        @Override
        public ConsistencyLevel getWriteCl()
        {
            return ConsistencyLevel.LOCAL_ONE;
        }
    }

    protected class TestRepairSchedulerContext implements RepairSchedulerContext
    {
        protected RepairSchedulerConfig config;
        protected CassandraInteraction interaction;


        public Session localSession()
        {
            return getLocaldb();
        }

        public RepairSchedulerConfig getConfig()
        {
            if (this.config == null) this.config = new TestRepairSchedulerConfig();
            return this.config;
        }

        @Override
        public CassandraInteraction getCassInteraction()
        {
            if (this.interaction == null)
            {
                this.interaction = new Cass4xInteraction(getConfig());
            }
            return this.interaction;
        }
    }

    protected void loadDataset(int count)
    {
        session.execute("TRUNCATE system_distributed.repair_process;");
        session.execute("TRUNCATE system_distributed.repair_sequence;");
        session.execute("TRUNCATE system_distributed.repair_status;");
        session.execute("TRUNCATE system_distributed.repair_hook_status;");

        session.execute("CREATE KEYSPACE IF NOT exists "+TEST_REPAIR_KS+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        session.execute("CREATE TABLE IF NOT exists "+TEST_REPAIR_KS+"."+SUBRANGE_TEST_TBL_NAME+" (" +
                        "    key text PRIMARY KEY," +
                        "    value text," +
                        ");" +
                        "");
        session.execute("CREATE TABLE IF NOT exists "+TEST_REPAIR_KS+"."+NO_REPAIR_TBL_NAME+" (" +
                        "    key text PRIMARY KEY," +
                        "    value text," +
                        ");" +
                        "");
        session.execute("CREATE TABLE IF NOT exists "+TEST_REPAIR_KS+"."+INCREMENTAL_TEST_TBL_NAME+" (" +
                        "    key text PRIMARY KEY," +
                        "    value text," +
                        ");" +
                        "");
        session.execute("CREATE TABLE IF NOT exists "+TEST_REPAIR_KS+"."+DEFAULT_TEST_TBL_NAME+" (" +
                        "    key text PRIMARY KEY," +
                        "    value text," +
                        ");"
        );

        PreparedStatement subRangeStatement = session.prepare("INSERT INTO "+TEST_REPAIR_KS+"."+SUBRANGE_TEST_TBL_NAME+" (key, value) VALUES (?, ?)");
        PreparedStatement incrementalStatement = session.prepare("INSERT INTO "+TEST_REPAIR_KS+"."+INCREMENTAL_TEST_TBL_NAME+" (key, value) VALUES (?, ?)");


        IntStream.rangeClosed(1, count).parallel()
                 .mapToObj(Integer::toString).forEach(v -> {
            BoundStatement boundStmt = subRangeStatement.bind(v, v);
            BoundStatement boundIncStatement = incrementalStatement.bind(v, v);
            session.execute(boundStmt);
            session.execute(boundIncStatement);
        });
        repairableTables = 4;
    }

    protected int getRandomRepairId()
    {
        return new Random().nextInt() & MASK;
    }

    protected int nextRandomPositiveInt()
    {
        return new Random().nextInt() & MASK;
    }
}