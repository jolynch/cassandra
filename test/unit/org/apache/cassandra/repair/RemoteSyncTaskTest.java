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
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.ObjectSizes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoteSyncTaskTest extends SchemaLoader
{
    private static final long TEST_TIMEOUT_S = 60;
    private static final IPartitioner partirioner = Murmur3Partitioner.instance;
    public static final String KEYSPACE1 = "RemoteDifferencer";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
    }

    /**
     * Regression test for CASSANDRA-14096. We should not retain memory after running
     */
    @Test
    public void testNoTreesRetainedAfterDifference() throws Throwable
    {
        CompletableFuture<MessageOut> outgoingMessageSink = registerOutgoingMessageSink();

        Range<Token> range = new Range<>(partirioner.getMinimumToken(), partirioner.getRandomToken());
        UUID parentRepairSession = UUID.randomUUID();
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddress(), Arrays.asList(cfs), Arrays.asList(range), false, System.currentTimeMillis(), false);

        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, UUID.randomUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

        MerkleTrees tree1 = createInitialTree(desc);
        MerkleTrees tree2 = createInitialTree(desc);

        // change a range in one of the trees
        Token token = partirioner.midpoint(range.left, range.right);
        tree1.invalidate(token);
        tree1.get(token).hash("non-empty hash!".getBytes());

        // difference the trees
        TreeResponse r1 = new TreeResponse(InetAddress.getByName("127.0.0.2"), tree1);
        TreeResponse r2 = new TreeResponse(InetAddress.getByName("127.0.0.3"), tree2);
        TreeDifferenceTask differenceTask = new TreeDifferenceTask(r1, r2);
        long sizeOfTrees = ObjectSizes.measureDeep(differenceTask);

        differenceTask.run();
        TreeDifference diff = differenceTask.get();

        SyncTask task = new RemoteSyncTask(desc, diff.endpoint1, diff.endpoint2, diff.ranges);
        task.run();
        long sizeOfSync = ObjectSizes.measureDeep(task);

        // Since there was a difference we should get a sync request
        MessageOut message = outgoingMessageSink.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);
        assertEquals(MessagingService.Verb.REPAIR_MESSAGE, message.verb);
        RepairMessage m = (RepairMessage) message.payload;
        assertEquals(RepairMessage.Type.SYNC_REQUEST, m.messageType);

        // Should retain practically no memory after the run
        assertTrue(sizeOfSync < (sizeOfTrees * 0.01));
    }

    private MerkleTrees createInitialTree(RepairJobDesc desc)
    {
        MerkleTrees tree = new MerkleTrees(partirioner);
        tree.addMerkleTrees((int) Math.pow(2, 15), desc.ranges);
        tree.init();
        for (MerkleTree.TreeRange r : tree.invalids())
        {
            r.ensureHashInitialised();
        }
        return tree;
    }

    private CompletableFuture<MessageOut> registerOutgoingMessageSink()
    {
        final CompletableFuture<MessageOut> future = new CompletableFuture<>();
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                future.complete(message);
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });
        return future;
    }
}
