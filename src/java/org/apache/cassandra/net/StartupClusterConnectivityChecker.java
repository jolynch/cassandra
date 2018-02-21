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

package org.apache.cassandra.net;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.stream.Collectors.groupingBy;
import static org.apache.cassandra.net.MessagingService.Verb.PING;

public class StartupClusterConnectivityChecker
{
    private static final Logger logger = LoggerFactory.getLogger(StartupClusterConnectivityChecker.class);

    enum State { CONTINUE, FINISH_SUCCESS, FINISH_TIMEOUT }

    private int downCountLocalDC = Integer.MAX_VALUE;
    private int downCountPerDC = Integer.MAX_VALUE;
    private int downCountAllDCS = Integer.MAX_VALUE;
    private final int timeoutSecs;
    private final Predicate<InetAddressAndPort> gossipStatus;
    private final Function<InetAddressAndPort, String> getDatacenter;

    public StartupClusterConnectivityChecker(int downCountLocalDC, int downCountEachDC, int downCountAllDCS, int timeoutSecs, Predicate<InetAddressAndPort> gossipStatus, Function<InetAddressAndPort, String> getDatacenter)
    {
        this.downCountLocalDC = Math.max(0, downCountLocalDC);
        this.downCountPerDC = Math.max(0, downCountEachDC);
        this.downCountAllDCS = Math.max(0, downCountAllDCS);

        if (timeoutSecs > 100)
        {
            logger.warn("setting the block-for-peers timeout (in seconds) to {} might be a bit excessive, but using it nonetheless", timeoutSecs);
        }
        this.timeoutSecs = Math.max(1, timeoutSecs);


        this.gossipStatus = gossipStatus;
        this.getDatacenter = getDatacenter;
    }

    public void execute(Set<InetAddressAndPort> peers)
    {
        InetAddressAndPort myPeer = FBUtilities.getBroadcastAddressAndPort();
        String localDC = getDatacenter.apply(myPeer);
        Set<String> allDCS = peers.stream().map(getDatacenter).collect(Collectors.toSet());

        if (peers == null || Math.min(Math.min(downCountLocalDC, downCountPerDC), downCountAllDCS) == Integer.MAX_VALUE)
            return;

        // remove current node from the set
        peers = peers.stream()
                     .filter(peer -> !peer.equals(myPeer))
                     .collect(Collectors.toSet());

        // don't block if there's no other nodes in the cluster (or we don't know about them)
        if (peers.size() <= 0)
            return;

        logger.info("choosing to block until fewer than [{},{},{}] peers in [local,each,all] dcs are down or do not have connections established; max time to wait = {} seconds",
                    downCountLocalDC, downCountPerDC, downCountAllDCS, timeoutSecs);

        // first, send out a ping message to open up the non-gossip connections
        final Map<InetAddressAndPort, AtomicInteger> connectedCounts = sendPingMessages(peers);

        final long startNanos = System.nanoTime();
        final long expirationNanos = startNanos + TimeUnit.SECONDS.toNanos(timeoutSecs);
        int completedRounds = 0;
        while (checkStatus(peers, localDC, allDCS, connectedCounts, startNanos, expirationNanos < System.nanoTime(), completedRounds) == State.CONTINUE)
        {
            completedRounds++;
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    State checkStatus(Set<InetAddressAndPort> peers, String localDC, Set<String> allDCs, Map<InetAddressAndPort, AtomicInteger> connectedCounts, final long startNanos, boolean beyondExpiration, final int completedRounds)
    {
        AtomicInteger defaultConnections = new AtomicInteger(0);
        Set<InetAddressAndPort> downPeers = peers.stream()
                                                 .filter(peer -> !gossipStatus.test(peer))
                                                 .collect(Collectors.toSet());
        Map<String, Long> downPeersPerDC = downPeers.stream()
                                                    .collect(groupingBy(getDatacenter, Collectors.counting()));
        Set<InetAddressAndPort> notConnectedPeers = peers.stream()
                                                      .filter(peer -> connectedCounts.getOrDefault(peer, defaultConnections).get() <= 0)
                                                      .collect(Collectors.toSet());
        Map<String, Long> notConnectedPeersPerDC = notConnectedPeers.stream()
                                                              .collect(groupingBy(getDatacenter, Collectors.counting()));

        Set<Long> downCounts = new HashSet<>(downPeersPerDC.values());
        downCounts.add(0L);
        long currentDownLocal = downPeersPerDC.getOrDefault(localDC, 0L);
        long currentDownEachDC = Collections.max(downCounts);
        long currentDownAllDCS = downCounts.stream().mapToLong(Long::longValue).sum();

        Set<Long> disconnectedCounts = new HashSet<>(notConnectedPeersPerDC.values());
        disconnectedCounts.add(0L);
        long currentDisconnectedLocal = notConnectedPeersPerDC.getOrDefault(localDC, 0L);
        long currentDisconnectedEachDC = Collections.max(disconnectedCounts);
        long currentDisconnectedAllDCS = disconnectedCounts.stream().mapToLong(Long::longValue).sum();

        if ((currentDownLocal <= downCountLocalDC && currentDisconnectedLocal <= downCountLocalDC) &&
            (currentDownEachDC <= downCountPerDC && currentDisconnectedEachDC <= downCountPerDC) &&
            (currentDownAllDCS <= downCountAllDCS && currentDisconnectedAllDCS <= downCountAllDCS))
        {
            logger.info("after {} milliseconds, found [{},{},{}]/{} of peers still marked down, " +
                        "and [{},{},{}]/{} disconnected peers, " +
                        "both of which are below the desired threshold of [local, any dc, all dcs] <= [{},{},{}]",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                        currentDownLocal, currentDownEachDC, currentDownAllDCS, peers.size(),
                        currentDisconnectedLocal, currentDisconnectedEachDC, currentDisconnectedAllDCS, peers.size(),
                        downCountLocalDC, downCountPerDC, downCountAllDCS);
            return State.FINISH_SUCCESS;
        }

        // perform at least two rounds of checking, else this is kinda useless (and the operator set the aliveTimeoutSecs too low)
        if (completedRounds >= 2 && beyondExpiration)
        {
            logger.info("after {} milliseconds, found [{},{},{}]/{} of peers still marked down, " +
                        "and [{},{},{}]/{} disconnected peers, " +
                        "one of which are above the desired threshold of [local, any dc, all dcs] <= [{},{},{}]",
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                        currentDownLocal, currentDownEachDC, currentDownAllDCS, peers.size(),
                        currentDisconnectedLocal, currentDisconnectedEachDC, currentDisconnectedAllDCS, peers.size(),
                        downCountLocalDC, downCountPerDC, downCountAllDCS);
            return State.FINISH_TIMEOUT;
        }
        return State.CONTINUE;
    }

    /**
     * Sends a "connection warmup" message to each peer in the collection, on every {@link OutboundConnectionIdentifier.ConnectionType}
     * used for internode messaging.
     */
    private Map<InetAddressAndPort, AtomicInteger> sendPingMessages(Set<InetAddressAndPort> peers)
    {
        return peers.stream().collect(Collectors.toMap(Function.identity(), this::sendPingMessage));
    }

    private AtomicInteger sendPingMessage(InetAddressAndPort peer)
    {
        AtomicInteger connectedCount = new AtomicInteger(0);
        IAsyncCallback responseHandler = new IAsyncCallback()
        {
            @Override
            public boolean isLatencyForSnitch()
            {
                return false;
            }

            @Override
            public void response(MessageIn msg)
            {
                connectedCount.incrementAndGet();
            }
        };

        MessageOut<PingMessage> smallChannelMessageOut = new MessageOut<>(PING, PingMessage.smallChannelMessage, PingMessage.serializer);
        MessageOut<PingMessage> largeChannelMessageOut = new MessageOut<>(PING, PingMessage.largeChannelMessage, PingMessage.serializer);
        MessagingService.instance().sendRR(smallChannelMessageOut, peer, responseHandler);
        MessagingService.instance().sendRR(largeChannelMessageOut, peer, responseHandler);

        return connectedCount;
    }
}
