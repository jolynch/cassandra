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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;

public class RandomTokenProvider implements TokenProvider
{
    private static final Logger logger = LoggerFactory.getLogger(RandomTokenProvider.class);

    private final Map<String, String> args;
    private final int numTokens;

    public RandomTokenProvider(Map<String, String> args)
    {
        this.args = Collections.unmodifiableMap(args);
        numTokens = Integer.valueOf(args.getOrDefault("num_tokens",
                                                      String.valueOf(DatabaseDescriptor.getNumTokens())));
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
        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode. You should probably add more vnodes and/or use the automatic token allocation mechanism.");

        return getRandomTokens(metadata, numTokens);
    }

    public static Collection<Token> getRandomTokens(TokenMetadata metadata, int numTokens)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = metadata.partitioner.getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }

        logger.info("Generated random tokens. tokens are {}", tokens);
        return tokens;
    }
}
