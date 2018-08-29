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

import java.lang.reflect.Constructor;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerConfig;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerConfigurationLoader;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerYamlConfigLoader;
import org.apache.cassandra.repair.scheduler.conn.Cass4xInteraction;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteraction;
import org.apache.cassandra.repair.scheduler.conn.CassandraInteractionBase;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.repair.scheduler.TaskUtil.initSession;

/**
 * TaskSchedulerContext holds C* session object to repairing cluster, config object
 * and C* interaction object. RepairScheduler needs these objects in many places,
 * instead of constructing them and passing them around, this Context class makes the life easier to access these objects
 */
public class TaskSchedulerContextImpl implements TaskSchedulerContext
{
    private static final Logger logger = LoggerFactory.getLogger(TaskSchedulerContextImpl.class);

    private final TaskSchedulerConfig config;
    private CassandraInteraction cassandraInteraction;
    private Session localSession;

    public TaskSchedulerContextImpl()
    {
        this.config = loadConfig();
    }

    /**
     * Cassandra session object to repair state persistence cluster
     *
     * @return Session
     */
    @Override
    public Session localSession()
    {
        if (localSession == null)
        {
            localSession = initSession(Collections.singletonList(config.getRepairNativeEndpoint()), true);
            logger.info("Initiated LocalSession with C*");
        }
        return localSession;
    }

    /**
     * Returns the TaskSchedulerConfig object
     *
     * @return TaskSchedulerConfig
     */
    @Override
    public TaskSchedulerConfig getConfig()
    {
        return config;
    }

    /**
     * Returns CassandraInteraction object
     *
     * @return CassandraInteraction
     */
    @Override
    public CassandraInteraction getCassInteraction()
    {
        if (cassandraInteraction == null)
        {
            cassandraInteraction = loadCassandraInteraction();
        }
        return cassandraInteraction;
    }

    /**
     * Loads config using YamlConfig Loader. It can also load custom config loaders set via "repair.config.loader"
     * system property.
     *
     * @return TaskSchedulerConfig
     * @throws ConfigurationException If there are issues in loading configuration
     */
    private TaskSchedulerConfig loadConfig() throws ConfigurationException
    {
        String loaderClass = System.getProperty("repair.config.loader");
        RepairSchedulerConfigurationLoader loader = loaderClass == null
                                                    ? new RepairSchedulerYamlConfigLoader()
                                                    : FBUtilities.construct(loaderClass, "configuration loading");

        return loader.loadConfig();
    }

    /**
     * Constructs CassandraInteraction class based on config value provided in yaml. In case of exception in
     * constructing user provided/ config class, we construct default Cass4xInteraction.
     */
    private CassandraInteraction loadCassandraInteraction()
    {
        try
        {
            Class<CassandraInteractionBase> cls = FBUtilities.classForName(getConfig().getCassandraInteractionClass(), "CassandraInteraction");
            Constructor<?> constructor = cls.getConstructor(TaskSchedulerConfig.class);
            logger.info("Constructed class [{}] for CassandraInteraction", getConfig().getCassandraInteractionClass());
            return (CassandraInteraction) constructor.newInstance(getConfig());
        }
        catch (Exception e)
        {
            logger.error("Exception in initializing CassandraInteraction class, probably a config issue? - [{}]." +
                         " Using default Cass4xInteraction.",
                         getConfig().getCassandraInteractionClass(), e);
            return new Cass4xInteraction(getConfig());
        }
    }
}
