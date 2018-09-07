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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

/**
 * Util class for repair scheduler
 */
public class RepairUtil
{
    static final String DATE_PATTERN = "MM/dd/yyyy HH:mm:ss.SSS";
    private static final Logger logger = LoggerFactory.getLogger(RepairUtil.class);

    /**
     * Get Keyspace and table name in Keyspace.TableName format
     *
     * @param keyspace keyspace name
     * @param table    table name
     * @return keyspace.tablename
     */
    public static String getKsTbName(String keyspace, String table)
    {
        return keyspace + "." + table;
    }

    /**
     * Check the state of expression and throw exception as needed
     *
     * @param expression   Expression to check for
     * @param errorMessage Error message to be used in throwing exception
     */
    public static void checkState(boolean expression, Object errorMessage)
    {
        if (!expression)
        {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    /**
     * Initializes the session to given C* endpoints
     *
     * @param contactPointsWithPorts Contact points to talk to C*, preferably local region endpoints. Format is IP:Port
     * @param isLocal                To limit the load balancing policy to talk to only local C* node
     * @return Session object
     */
    static Session initSession(List<String> contactPointsWithPorts, boolean isLocal)
    {
        try
        {
            List<InetSocketAddress> coordinatorAddressLst = contactPointsWithPorts
                                                            .stream()
                                                            .map(s -> {
                                                                HostAndPort hap = HostAndPort.fromString(s);
                                                                return new InetSocketAddress(hap.getHost(), hap.getPort());
                                                            }).collect(Collectors.toList());

            Cluster.Builder mainClusterBuilder = Cluster.builder()
                                                        .addContactPointsWithPorts(coordinatorAddressLst);

            if (isLocal)
            {
                mainClusterBuilder.withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), coordinatorAddressLst));
            }
            else
            {
                mainClusterBuilder.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder().build());
            }

            Cluster mainCluster = mainClusterBuilder.build();
            Session session = mainCluster.connect();
            logger.info("Connected to Cassandra cluster [{}] on [{}] with local mode: [{}].",
                        session.getCluster().getClusterName(), session.getCluster().getMetadata().getAllHosts(),
                        isLocal);
            return session;
        }
        catch (Exception e)
        {
            logger.error("Could not initialize the session, perhaps the address of C* node and/ or port is wrong. Here are the contact points used - [{}]", String.join(",", contactPointsWithPorts), e);
        }
        return null;
    }
}
