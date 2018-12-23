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

package org.apache.cassandra.dht.tokenallocator;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;

public class AutomaticTokenProvider implements TokenProvider
{
    private static final Logger logger = LoggerFactory.getLogger(AutomaticTokenProvider.class);

    private final Map<String, String> args;
    private final Integer numTokens;
    private final String allocationKeyspace;
    private final Integer replicationFactor;

    public AutomaticTokenProvider(Map<String, Object> args)
    {
        this.args = args.keySet().stream()
                        .collect(Collectors.toMap(k -> k, Object::toString));

        numTokens = (Integer) (args.getOrDefault("num_tokens", 1));
        allocationKeyspace = (String) args.get("keyspace");
        replicationFactor = (Integer) args.getOrDefault("replication_factor", 1);

        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");
    }

    @Override
    public Map<String, String> getArguments()
    {
        return this.args;
    }

    @Override
    public Collection<Token> allocateTokens(TokenMetadata metadata, InetAddressAndPort address)
    {
        AbstractReplicationStrategy rs = null;

        if (allocationKeyspace == null)
        {
            if (replicationFactor > 0)
            {
                Map<String, String> replication = Datacenters.getValidDatacenters().stream()
                                                             .collect(Collectors.toMap(k -> k, k -> this.replicationFactor.toString()));
                rs = new NetworkTopologyStrategy("bootstrap_keyspace", metadata, DatabaseDescriptor.getEndpointSnitch(), replication);
            }
            else
            {
                logger.info("No keyspace or replication_factor speficied for Token allocation to use, defaulting to the highest replication present");
                int maxReplication = -1;
                for (String keyspaceName : Schema.instance.getKeyspaces())
                {
                    Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);
                    if (keyspace.getReplicationStrategy().getReplicationFactor().allReplicas > maxReplication)
                        rs = keyspace.getReplicationStrategy();
                }
            }
        }
        else
        {
            Keyspace ks = Keyspace.open(allocationKeyspace);
            if (ks == null)
                throw new ConfigurationException("Problem opening token allocation keyspace " + allocationKeyspace);
            rs = ks.getReplicationStrategy();
        }

        if (rs == null)
        {
            throw new ConfigurationException("Could not find any suitable way to allocate tokens, failing.");
        }

        return allocateTokens(metadata, address, rs, numTokens);
    }


    static public Collection<Token> allocateTokens(final TokenMetadata metadata, InetAddressAndPort address,
                                            AbstractReplicationStrategy replicationStrategy, int numTokens)
    {
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        return TokenAllocation.allocateTokens(metadata, replicationStrategy, address, numTokens);
    }
}
