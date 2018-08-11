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
package org.apache.cassandra.repair.scheduler.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.repair.scheduler.RepairController;
import org.apache.cassandra.repair.scheduler.RepairDaoManager;
import org.apache.cassandra.repair.scheduler.RepairManager;
import org.apache.cassandra.repair.scheduler.RepairSchedulerContextImpl;
import org.apache.cassandra.repair.scheduler.config.RepairSchedulerContext;
import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;
import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.TableRepairConfig;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * RepairSchedulerDaemon is a singleton, which is responsible for running repair scheduler as a daemon.
 * This class holds the scheduled executor which calls repair via RepairManager.
 * All nodes periodically (e.g. every 2 minutes) wake up and enter state (A), where they determine if there
 * is a currently running repair via the repair_process table.
 * This class also holds the glue between REST API and repair controller/ daos.
 * Refer to CASSANDRA-14346 design document for more info on state machine
 */
public class RepairSchedulerDaemon
{
    private static final Logger logger = LoggerFactory.getLogger(RepairSchedulerDaemon.class);
    private static RepairSchedulerDaemon instance;
    private final RepairSchedulerContext context;
    private final RepairController repairController;
    private final RepairManager repairManager;
    // Instead of calling C* - JMX clusterName on every call, we could cache it here and reuse and sure enough
    // cluster name does not change the during the life cycle of sidecar/ repair scheduler
    private String clusterName;

    /**
     * Singleton so that more than one caller can use it without initializing repair scheduler multiple times
     */
    private RepairSchedulerDaemon()
    {
        this.context = new RepairSchedulerContextImpl();
        final RepairDaoManager daoManager = new RepairDaoManager(context);
        this.repairController = new RepairController(context, daoManager);
        this.repairManager = new RepairManager(context);

        int initialDelayInMin = ThreadLocalRandom.current().nextInt(1, 5);
        logger.info("Scheduling Repair Scheduler sidecar with a 2 min interval task with initial delay of {} minutes", initialDelayInMin);
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(new RepairScheduler(), initialDelayInMin, 2, TimeUnit.MINUTES);
    }

    /**
     * Gets a singleton instance for RepairSchedulerDaemon
     *
     * @return RepairSchedulerDaemon
     */
    public static synchronized RepairSchedulerDaemon getInstance()
    {
        if (instance == null)
        {
            instance = new RepairSchedulerDaemon();
        }
        return instance;
    }

    /**
     * Gets RepairSchedulerContext
     *
     * @return RepairSchedulerContext
     */
    public RepairSchedulerContext getContext()
    {
        return context;
    }

    /**
     * Status of current repair
     *
     * @return Repair metadata which represents of the status of a repair
     */
    public List<RepairMetadata> getRepairStatus()
    {
        Optional<ClusterRepairStatus> crs = repairController.getClusterRepairStatus();
        return crs
               .map(clusterRepairStatus -> repairController.getRepairStatusDao()
                                                           .getRepairHistory(clusterRepairStatus.getRepairId()))
               .orElse(null);
    }

    /**
     * Get repair status of a particular repair id
     *
     * @param repairId Repair Id to get the status for
     * @return List of RepairMetadata which represents the status of repair
     */
    public List<RepairMetadata> getRepairStatus(int repairId)
    {
        return repairController.getRepairStatusDao().getRepairHistory(repairId);
    }

    /**
     * Gets repair history for the current cluster
     *
     * @return List<RepairMetadata>
     */
    public List<RepairMetadata> getRepairHistory()
    {
        return repairController.getRepairStatusDao().getRepairHistory();
    }

    /**
     * Get table repair configurations/ overrides for all schedules
     *
     * @return Map of TableRepairConfigs list keyed by schedule name
     */
    public Map<String, List<TableRepairConfig>> getRepairConfig()
    {
        return repairController.getRepairConfigDao().getRepairConfigs();
    }

    /**
     * Get table repair configurations/ overrides for a given schedule
     *
     * @param schedule Schedule name to get table repair configs for
     * @return return repair configs list
     */
    public List<TableRepairConfig> getRepairConfig(String schedule)
    {
        return repairController.getRepairConfigDao().getRepairConfigs(schedule);
    }

    /**
     * Updates repair config of table in a given schedule
     *
     * @param schedule Schedule name to be used for updating
     * @param config   TableConfig
     * @return boolean indicating the update operation result
     */
    public boolean updateRepairConfig(String schedule, TableRepairConfig config)
    {
        Set<String> validSchedules = repairController.getRepairConfigDao().getAllRepairSchedules();
        if (validSchedules.contains(schedule))
        {
            return repairController.getRepairConfigDao().saveRepairConfig(schedule, config);
        }
        throw new RuntimeException(
        String.format("Can not save config to unknown schedule[%s]. Available schedules are: [%s]",
                      schedule, String.join(",", validSchedules)));
    }

    /**
     * Stops currently running repair by calling JMX endpoint forceTerminateAllRepairSessions
     *
     * @return true indicating the result of stop operation, exception in case of failed operation
     */
    public boolean stopRepair()
    {
        logger.info("Stopping Repair Scheduler");
        repairManager.pauseRepairOnCluster();
        getContext().getConfig().setRepairSchedulerEnabled(false);
        return true;
    }

    /**
     * Starts repair on current node. This methods flips the config property, next scheduled (2 min interval)
     * runRepair method will be starting the repair
     *
     * @return true indicating the result of start operation, exception in case of failed operation
     */
    public boolean startRepair()
    {
        logger.info("Starting Repair Scheduler");
        getContext().getConfig().setRepairSchedulerEnabled(true);
        return true;
    }

    /**
     * Returns cached cluster name or get it from JMX
     *
     * @return Cluster Name
     */
    private String getClusterName()
    {
        if (clusterName == null)
        {
            clusterName = this.context.getCassInteraction().getClusterName();
        }
        return clusterName;
    }

    /**
     * Repair Scheduler runnable to call repairManager.runRepairOnCluster() periodically. This checks for
     * repairScheduleEnabled property before starting runRepairOnCluster.
     * This uses NoSpamLogger to avoid excessive logging as this code path gets executed every 2 min.
     */
    private class RepairScheduler implements Runnable
    {

        @Override
        public void run()
        {
            if (context.getConfig().isRepairSchedulerEnabled())
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 24, TimeUnit.HOURS,
                                 "Repair scheduler is enabled on this instance.");
                repairManager.runRepairOnCluster();
            }
            else
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 24, TimeUnit.HOURS,
                                 "Repair scheduler is disabled on this instance.  see repair_scheduler_enabled property");
            }
        }
    }
}
