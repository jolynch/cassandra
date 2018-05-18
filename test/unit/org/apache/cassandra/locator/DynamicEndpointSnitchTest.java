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
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.LatencyUsableForSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class DynamicEndpointSnitchTest
{
    private static DynamicEndpointSnitch dsnitch;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        // do this because SS needs to be initialized before DES can work properly.
        StorageService.instance.unsafeInitialize();
        SimpleSnitch ss = new SimpleSnitch();
        dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
    }

    @Before
    public void resetSnitchBefore()
    {
        dsnitch.reset(true);
    }

    @After
    public void resetSnitchAfter()
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
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        InetAddressAndPort host1 = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort host2 = InetAddressAndPort.getByName("127.0.0.3");
        InetAddressAndPort host3 = InetAddressAndPort.getByName("127.0.0.4");
        InetAddressAndPort host4 = InetAddressAndPort.getByName("127.0.0.5");
        List<InetAddressAndPort> hosts = Arrays.asList(host1, host2, host3);

        // first, make all hosts equal
        setScores(dsnitch, 1, hosts, 10, 10, 10);
        List<InetAddressAndPort> order = Arrays.asList(host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host1 a little worse
        setScores(dsnitch, 1, hosts, 20, 10, 10);
        order = Arrays.asList(host2, host3, host1);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host2 as bad as host1
        setScores(dsnitch, 2, hosts, 15, 20, 10);
        order = Arrays.asList(host3, host1, host2);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host3 the worst
        setScores(dsnitch, 3, hosts, 10, 10, 30);
        order = Arrays.asList(host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host3 equal to the others
        setScores(dsnitch, 5, hosts, 10, 10, 10);
        order = Arrays.asList(host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        /// Tests CASSANDRA-6683 improvements
        // make the scores differ enough from the ideal order that we sort by score; under the old
        // dynamic snitch behavior (where we only compared neighbors), these wouldn't get sorted
        setScores(dsnitch, 20, hosts, 10, 70, 20);
        order = Arrays.asList(host1, host3, host2);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        order = Arrays.asList(host4, host1, host3, host2);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3, host4)));

        setScores(dsnitch, 20, hosts, 10, 10, 10);
        order = Arrays.asList(host4, host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3, host4)));

        /// Tests CASSANDRA-14459 improvements
        // On reset ensure all but the minimums are kept so we don't lose everything on resets
        // Record high and low latencies to test sorting before and after the reset
        dsnitch.reset(true);
        dsnitch.receiveTiming(host1, 2);
        dsnitch.receiveTiming(host2, 1);
        dsnitch.receiveTiming(host3, 3);
        dsnitch.receiveTiming(host1, 30);
        dsnitch.receiveTiming(host1, 30);
        dsnitch.receiveTiming(host2, 50);
        dsnitch.receiveTiming(host2, 50);
        dsnitch.receiveTiming(host3, 10);
        dsnitch.receiveTiming(host3, 10);

        // Allow update to happen in the DS
        Thread.sleep(200);

        List<InetAddressAndPort> orderBefore = Arrays.asList(host3, host1, host2);
        List<InetAddressAndPort> orderAfter = Arrays.asList(host2, host1, host3);

        assertEquals(orderBefore, dsnitch.getSortedListByProximity(self, hosts));

        // Reset the snitch, allow it to update to the minimum latencies
        dsnitch.reset(false);
        Thread.sleep(200);

        assertEquals(orderAfter, dsnitch.getSortedListByProximity(self, hosts));
    }

    @Test
    public void testReceiveTiming() throws InterruptedException, IOException, ConfigurationException
    {
        InetAddressAndPort host1 = InetAddressAndPort.getByName("127.0.0.2");
        List<Double> timings;

        dsnitch.receiveTiming(host1, 1, LatencyUsableForSnitch.NO);
        dsnitch.receiveTiming(host1, 2, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(host1, 3, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(host1, 4, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(host1.getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(2.0, 3.0), timings);

        dsnitch.reset(true);
        dsnitch.receiveTiming(host1, 1, LatencyUsableForSnitch.MAYBE);
        dsnitch.receiveTiming(host1, 2, LatencyUsableForSnitch.MAYBE);
        dsnitch.receiveTiming(host1, 3, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(host1, 4, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(host1.getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(1.0, 2.0, 3.0), timings);

        dsnitch.reset(true);
        dsnitch.receiveTiming(host1, 1, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(host1, 2, LatencyUsableForSnitch.YES);
        dsnitch.receiveTiming(host1, 3, LatencyUsableForSnitch.MAYBE);
        dsnitch.receiveTiming(host1, 4, LatencyUsableForSnitch.MAYBE);
        timings = dsnitch.dumpTimings(host1.getHostAddress(false));
        Collections.sort(timings);
        assertEquals(Arrays.asList(1.0, 2.0), timings);
    }

    @Test
    public void testReset() throws InterruptedException, IOException, ConfigurationException
    {
        InetAddressAndPort host1 = InetAddressAndPort.getByName("127.0.0.2");

        dsnitch.receiveTiming(host1, 0);
        dsnitch.receiveTiming(host1, 2);
        dsnitch.receiveTiming(host1, 2);
        dsnitch.reset(false);
        assertEquals(Arrays.asList(0.0), dsnitch.dumpTimings(host1.getHostAddress(false)));

        // If the latency distribution then shifts and a host is just always really slow, gradually we should
        // reset to that latency.

        dsnitch.receiveTiming(host1, 100);
        dsnitch.reset(false);
        assertEquals(Arrays.asList(50.0), dsnitch.dumpTimings(host1.getHostAddress(false)));

        dsnitch.receiveTiming(host1, 100);
        dsnitch.reset(false);
        assertEquals(Arrays.asList(75.0), dsnitch.dumpTimings(host1.getHostAddress(false)));
    }
}
