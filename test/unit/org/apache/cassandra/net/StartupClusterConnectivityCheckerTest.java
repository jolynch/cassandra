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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.net.InetAddresses;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;

public class StartupClusterConnectivityCheckerTest
{
    @Test
    public void testConnectivitySimpleHappyPath() throws UnknownHostException
    {
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, addr -> true, addr -> "dc1");
        int count = 10;
        Set<InetAddressAndPort> peers = createNodes(count, 1);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 2);
        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
    }

    @Test
    public void testConnectivitySimpleContinue() throws UnknownHostException
    {
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, addr -> true, addr -> "dc1");
        int count = 10;
        Set<InetAddressAndPort> peers = createNodes(count, 1);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 0);
        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
    }

    @Test
    public void testConnectivityLocalDC() throws UnknownHostException
    {
        Set<InetAddressAndPort> peers = createNodes(4, 3);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 0);
        Function<InetAddressAndPort, String> getDc = new DCFunction().setDcs(3);

        Map<String, List<InetAddressAndPort>> nodesByDc = peers.stream()
                                                               .collect(Collectors.groupingBy(getDc));
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, addr -> true, getDc);

        // All nodes are down in remote datacenters. Should only care about dc1
        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").forEach(addr -> connectedCounts.get(addr).set(0));
        nodesByDc.get("dc1").stream().skip(2).forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        // All nodes are up in remote datacenters. Should still only care about dc1
        nodesByDc.get("dc2").forEach(addr -> connectedCounts.get(addr).set(2));
        nodesByDc.get("dc3").forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

    }

    @Test
    public void testConnectivityEachDC() throws UnknownHostException
    {
        Set<InetAddressAndPort> peers = createNodes(4, 3);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 0);
        Function<InetAddressAndPort, String> getDc = new DCFunction().setDcs(3);

        Map<String, List<InetAddressAndPort>> nodesByDc = peers.stream()
                                                         .collect(Collectors.groupingBy(getDc));
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, 2, Integer.MAX_VALUE, 10, addr -> true, getDc);

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc2").stream().skip(2).forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
        nodesByDc.get("dc3").stream().skip(2).forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

    }

    @Test
    public void testConnectivityAllDC() throws UnknownHostException
    {
        Set<InetAddressAndPort> peers = createNodes(4, 3);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 0);
        Function<InetAddressAndPort, String> getDc = new DCFunction().setDcs(3);

        Map<String, List<InetAddressAndPort>> nodesByDc = peers.stream()
                                                               .collect(Collectors.groupingBy(getDc));
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, 2, 1, 10, addr -> true, getDc);

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> connectedCounts.get(addr).set(1));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc2").forEach(addr -> connectedCounts.get(addr).set(2));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
        nodesByDc.get("dc3").stream().skip(1).forEach(addr -> connectedCounts.get(addr).set(1));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").forEach(addr -> connectedCounts.get(addr).set(1));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
    }

    @Test
    public void testConnectivityTimeout() throws UnknownHostException
    {
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, addr -> true, addr -> "dc1");
        int count = 10;
        Set<InetAddressAndPort> peers = createNodes(count, 1);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 0);
        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 4));
        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_TIMEOUT,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), true, 5));
    }

    @Test
    public void testConnectivitySimpleUpdating() throws UnknownHostException
    {
        UpdatablePredicate predicate = new UpdatablePredicate();
        final int count = 100;
        final int thresholdPercentage = 1;
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, predicate, addr -> "dc1");
        Set<InetAddressAndPort> peers = createNodes(count, 1);
        List<InetAddressAndPort> peerList = new ArrayList<>(peers);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 0);

        for (int i = count - 1; i >= 0; i--)
        {
            predicate.reset(count - i);
            connectedCounts.put(peerList.get(i), new AtomicInteger(2));
            StartupClusterConnectivityChecker.State expectedState = i > thresholdPercentage ?
                                                                    StartupClusterConnectivityChecker.State.CONTINUE :
                                                                    StartupClusterConnectivityChecker.State.FINISH_SUCCESS;
            Assert.assertEquals("failed on iteration " + i,
                                expectedState, connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, i));
        }
    }

    @Test
    public void testAliveLocalDC() throws UnknownHostException
    {
        Set<InetAddressAndPort> peers = createNodes(5, 2);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 2);
        Function<InetAddressAndPort, String> getDc = new DCFunction().setDcs(3);
        AlivePredicate isAlive = new AlivePredicate();
        Map<String, List<InetAddressAndPort>> nodesByDc = peers.stream()
                                                               .collect(Collectors.groupingBy(getDc));
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, isAlive, getDc);

        // All nodes are down in remove datacenters. Should only care about dc1
        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").forEach(addr -> isAlive.setState(addr, false));
        nodesByDc.get("dc1").stream().skip(2).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        // All nodes are up in remove datacenters. Should still only care about dc1
        nodesByDc.get("dc2").forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

    }

    @Test
    public void testAliveEachDC() throws UnknownHostException
    {
        Set<InetAddressAndPort> peers = createNodes(5, 3);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 2);
        Function<InetAddressAndPort, String> getDc = new DCFunction().setDcs(3);
        AlivePredicate isAlive = new AlivePredicate();
        Map<String, List<InetAddressAndPort>> nodesByDc = peers.stream()
                                                               .collect(Collectors.groupingBy(getDc));
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, 2, Integer.MAX_VALUE, 10, isAlive, getDc);

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc2").stream().skip(2).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
        nodesByDc.get("dc3").stream().skip(2).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

    }

    @Test
    public void testAliveAllDC() throws UnknownHostException
    {
        Set<InetAddressAndPort> peers = createNodes(7, 3);
        Map<InetAddressAndPort, AtomicInteger> connectedCounts = nodeConnectionCounts(peers, 1);
        Function<InetAddressAndPort, String> getDc = new DCFunction().setDcs(3);
        AlivePredicate isAlive = new AlivePredicate();
        Map<String, List<InetAddressAndPort>> nodesByDc = peers.stream()
                                                               .collect(Collectors.groupingBy(getDc));
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(1, 2, 1, 10, isAlive, getDc);

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").stream().skip(1).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc2").forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
        nodesByDc.get("dc3").stream().skip(1).forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));

        nodesByDc.get("dc1").forEach(addr -> isAlive.setState(addr, true));

        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, "dc1", connectedCounts, System.nanoTime(), false, 0));
    }

    /**
     * returns true until index = threshold, then returns false.
     */
    private class UpdatablePredicate implements Predicate<InetAddressAndPort>
    {
        int index;
        int threshold;

        void reset(int threshold)
        {
            index = 0;
            this.threshold = threshold;
        }

        @Override
        public boolean test(InetAddressAndPort inetAddressAndPort)
        {
            index++;
            return index <= threshold;
        }
    }

    private static Set<InetAddressAndPort> createNodes(int count, int dcs) throws UnknownHostException
    {
        Set<InetAddressAndPort> nodes = new HashSet<>();

        if (count < 1)
            Assert.fail("need at least *one* node in the set!");

        InetAddressAndPort node = InetAddressAndPort.getByName("127.0.0.1");
        nodes.add(node);
        for (int i = 1; i < count * dcs; i++)
        {
            node = InetAddressAndPort.getByAddress(InetAddresses.increment(node.address));
            nodes.add(node);
        }
        return nodes;
    }

    private class DCFunction implements Function<InetAddressAndPort, String>
    {
        int dcs;

        Function<InetAddressAndPort, String> setDcs(int dcs)
        {
            this.dcs = dcs;
            return this;
        }

        @Override
        public String apply(InetAddressAndPort inetAddressAndPort)
        {
            return "dc" + (1 + InetAddresses.coerceToInteger(inetAddressAndPort.address) % dcs);
        }
    }

    private class AlivePredicate implements Predicate<InetAddressAndPort>
    {
        Set<InetAddressAndPort> alive = new HashSet<>();

        Predicate<InetAddressAndPort> setState(InetAddressAndPort addr, boolean alive)
        {
            if (alive)
                this.alive.add(addr);
            else
                this.alive.remove(addr);
            return this;
        }

        @Override
        public boolean test(InetAddressAndPort inetAddressAndPort)
        {
            return this.alive.contains(inetAddressAndPort);
        }
    }

    private static Map<InetAddressAndPort, AtomicInteger> nodeConnectionCounts(Set<InetAddressAndPort> addresses, int count) {
        return addresses.stream().collect(Collectors.toMap(a -> a, a -> new AtomicInteger(count)));
    }

}
