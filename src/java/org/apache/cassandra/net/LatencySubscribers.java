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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Callback that {@link org.apache.cassandra.locator.DynamicEndpointSnitch} listens to in order
 * to update host scores.
 *
 * FIXME: rename/specialise, since only used by DES?
 */
public class LatencySubscribers
{
    public interface Subscriber
    {
        void receiveTiming(InetAddressAndPort address, long latency, TimeUnit unit, LatencyMeasurementType latencyMeasurementType);
    }

    private final Set<Subscriber> subscribers = new CopyOnWriteArraySet<>();

    public void subscribe(Subscriber subscriber)
    {
        subscribers.add(subscriber);
    }

    public void unsubscribe(Subscriber subscriber)
    {
        subscribers.remove(subscriber);
    }

    @VisibleForTesting
    public Set<Subscriber> getSubscribers()
    {
        return Collections.unmodifiableSet(subscribers);
    }

    public void recordLatency(InetAddressAndPort address, long latency, TimeUnit unit, LatencyMeasurementType type)
    {
        for (Subscriber subscriber : subscribers)
            subscriber.receiveTiming(address, latency, unit, type);
    }
}
