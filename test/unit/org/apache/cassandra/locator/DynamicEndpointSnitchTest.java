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

package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.LatencyUsableForSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamicEndpointSnitchTest
{
    private static DynamicEndpointSnitch dsnitch;
    private static InetAddressAndPort[] hosts;

    @BeforeClass
    public static void setupDD() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        // do this because SS needs to be initialized before DES can work properly.
        StorageService.instance.unsafeInitialize();
        SimpleSnitch ss = new SimpleSnitch();

        hosts = new InetAddressAndPort[] {
            FBUtilities.getBroadcastAddressAndPort(),
            InetAddressAndPort.getByName("127.0.0.2"),
            InetAddressAndPort.getByName("127.0.0.3"),
            InetAddressAndPort.getByName("127.0.0.4"),
            InetAddressAndPort.getByName("127.0.0.5"),
        };


        dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
    }

    @Before
    public void prepareDES()
    {
        for (InetAddressAndPort host : hosts)
        {
            Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
        }
        dsnitch.reset(true);
    }

    @After
    public void resetDES()
    {
        dsnitch.reset(true);
    }

    private static void setScores(DynamicEndpointSnitch dsnitch, int rounds, List<InetAddressAndPort> hosts, Integer... scores) throws InterruptedException
    {
        for (int round = 0; round < rounds; round++)
        {
            for (int i = 0; i < hosts.size(); i++)
                dsnitch.receiveTiming(hosts.get(i), scores[i]);
        }
        // Slightly higher than the 100ms update interval, allowing latencies to actually count
        Thread.sleep(150);
    }

    @Test
    public void testSnitch() throws InterruptedException, IOException, ConfigurationException
    {
        InetAddressAndPort self = hosts[0];
        List<InetAddressAndPort> allHosts = Arrays.asList(hosts[1], hosts[2], hosts[3]);

        // first, make all hosts equal
        setScores(dsnitch, 1, allHosts, 10, 10, 10);
        List<InetAddressAndPort> order = Arrays.asList(hosts[1], hosts[2], hosts[3]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3])));

        // make hosts[1] a little worse
        setScores(dsnitch, 1, allHosts, 20, 10, 10);
        order = Arrays.asList(hosts[2], hosts[3], hosts[1]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3])));

        // make hosts[2] as bad as hosts[1]
        setScores(dsnitch, 2, allHosts, 15, 20, 10);
        order = Arrays.asList(hosts[3], hosts[1], hosts[2]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3])));

        // make hosts[3] the worst
        setScores(dsnitch, 3, allHosts, 10, 10, 30);
        order = Arrays.asList(hosts[1], hosts[2], hosts[3]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3])));

        // make hosts[3] equal to the others
        setScores(dsnitch, 5, allHosts, 10, 10, 10);
        order = Arrays.asList(hosts[1], hosts[2], hosts[3]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3])));

        /// Tests CASSANDRA-6683 improvements
        // make the scores differ enough from the ideal order that we sort by score; under the old
        // dynamic snitch behavior (where we only compared neighbors), these wouldn't get sorted
        setScores(dsnitch, 20, allHosts, 10, 70, 20);
        order = Arrays.asList(hosts[1], hosts[3], hosts[2]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3])));

        order = Arrays.asList(hosts[4], hosts[1], hosts[3], hosts[2]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3], hosts[4])));

        setScores(dsnitch, 20, allHosts, 10, 10, 10);
        order = Arrays.asList(hosts[4], hosts[1], hosts[2], hosts[3]);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(hosts[1], hosts[2], hosts[3], hosts[4])));

        /// Tests CASSANDRA-14459 improvements
        // On reset ensure all but the minimums are kept so we don't lose everything on resets
        // Record high and low latencies to test sorting before and after the reset
        dsnitch.reset(true);
        dsnitch.receiveTiming(hosts[1], 2);
        dsnitch.receiveTiming(hosts[2], 1);
        dsnitch.receiveTiming(hosts[3], 3);
        dsnitch.receiveTiming(hosts[1], 30);
        dsnitch.receiveTiming(hosts[1], 30);
        dsnitch.receiveTiming(hosts[2], 50);
        dsnitch.receiveTiming(hosts[2], 50);
        dsnitch.receiveTiming(hosts[3], 10);
        dsnitch.receiveTiming(hosts[3], 10);

        // Allow update to happen in the DS
        Thread.sleep(200);

        List<InetAddressAndPort> orderBefore = Arrays.asList(hosts[3], hosts[1], hosts[2]);
        List<InetAddressAndPort> orderAfter = Arrays.asList(hosts[2], hosts[1], hosts[3]);

        assertEquals(orderBefore, dsnitch.getSortedListByProximity(self, allHosts));

        // Reset the snitch, allow it to update to the minimum latencies
        dsnitch.reset(false);
        Thread.sleep(200);

        assertEquals(orderAfter, dsnitch.getSortedListByProximity(self, allHosts));
    }

    @Test
    public void testReceiveTiming() throws InterruptedException, IOException, ConfigurationException
    {
        List<Double> timings;

        dsnitch.receiveTiming(hosts[1], 1, LatencyUsableForSnitch.NO);
        dsnitch.receiveTiming(hosts[1], 2, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(hosts[1], 3, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(hosts[1], 4, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(hosts[1].getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(2.0, 3.0), timings);

        dsnitch.reset(true);

        dsnitch.receiveTiming(hosts[1], 1, LatencyUsableForSnitch.MAYBE);
        dsnitch.receiveTiming(hosts[1], 2, LatencyUsableForSnitch.MAYBE);
        dsnitch.receiveTiming(hosts[1], 3, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(hosts[1], 4, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(hosts[1].getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(1.0, 2.0, 3.0), timings);

        dsnitch.reset(true);
        dsnitch.receiveTiming(hosts[1], 1, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(hosts[1], 2, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(hosts[1], 3, LatencyUsableForSnitch.MAYBE);
        dsnitch.receiveTiming(hosts[1], 4, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(hosts[1].getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(1.0, 2.0), timings);

        // Once a single MAYBE comes in, we should stop needing latency measurements
        dsnitch.reset(false);
        assertTrue(dsnitch.needsTiming(hosts[1]));
        dsnitch.receiveTiming(hosts[1], 1, LatencyUsableForSnitch.MAYBE);
        assertFalse(dsnitch.needsTiming(hosts[1]));
        dsnitch.receiveTiming(hosts[1], 2, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(hosts[1].getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(1.0, 1.0), timings);
    }

    @Test
    public void testReset() throws IOException, ConfigurationException
    {
        dsnitch.receiveTiming(hosts[1], 0);
        dsnitch.receiveTiming(hosts[1], 2);
        dsnitch.receiveTiming(hosts[1], 2);
        dsnitch.receiveTiming(hosts[2], 10);
        dsnitch.reset(false);
        assertEquals(Arrays.asList(0.0), dsnitch.dumpTimings(hosts[1].getHostAddress(false)));
        assertTrue(dsnitch.needsTiming(hosts[2]));
        assertFalse(dsnitch.needsTiming(hosts[1]));

        // If the latency distribution then shifts and a host is just always really slow, gradually we should
        // reset to that latency.

        dsnitch.receiveTiming(hosts[1], 100);
        dsnitch.reset(false);
        assertEquals(Arrays.asList(50.0), dsnitch.dumpTimings(hosts[1].getHostAddress(false)));

        dsnitch.receiveTiming(hosts[1], 100);
        dsnitch.reset(false);
        assertEquals(Arrays.asList(75.0), dsnitch.dumpTimings(hosts[1].getHostAddress(false)));
        assertTrue(dsnitch.needsTiming(hosts[1]));
        assertTrue(dsnitch.needsTiming(hosts[2]));

        // Removing an instance from gossip should remove it from tracking
        Gossiper.instance.replacedEndpoint(hosts[2]);
        dsnitch.reset(false);
        assertTrue(dsnitch.needsTiming(hosts[1]));
        assertFalse(dsnitch.needsTiming(hosts[2]));
    }
}
