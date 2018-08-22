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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.repair.scheduler.api.RepairSchedulerApiUtil.addRepairSchedulerHeaders;
import static org.apache.cassandra.repair.scheduler.api.RepairSchedulerApiUtil.get;
import static org.apache.cassandra.repair.scheduler.api.RepairSchedulerApiUtil.getParamSchedule;
import static org.apache.cassandra.repair.scheduler.api.RepairSchedulerApiUtil.post;
import static org.apache.cassandra.repair.scheduler.api.RepairSchedulerDaemon.getInstance;
import static org.apache.cassandra.repair.scheduler.api.RepairSchedulerJsonAdapter.deserializeJson;
import static spark.Spark.after;
import static spark.Spark.port;

/**
 * The main building block for the Spark application to start the service and expose REST APIs
 * This is the main class for starting the service.
 */
public class RepairSchedulerApp
{
    private static final String PATH = "/v1/repair";
    private static final Logger logger = LoggerFactory.getLogger(RepairSchedulerApp.class);

    public static void main(String[] args)
    {

        logSystemInfo();
        System.out.println(" _____                               _              _____ _     _                     \n" +
                           "/  __ \\                             | |            /  ___(_)   | |                    \n" +
                           "| /  \\/ __ _ ___ ___  __ _ _ __   __| |_ __ __ _   \\ `--. _  __| | ___  ___ __ _ _ __ \n" +
                           "| |    / _` / __/ __|/ _` | '_ \\ / _` | '__/ _` |   `--. \\ |/ _` |/ _ \\/ __/ _` | '__|\n" +
                           "| \\__/\\ (_| \\__ \\__ \\ (_| | | | | (_| | | | (_| |  /\\__/ / | (_| |  __/ (_| (_| | |   \n" +
                           " \\____/\\__,_|___/___/\\__,_|_| |_|\\__,_|_|  \\__,_|  \\____/|_|\\__,_|\\___|\\___\\__,_|_|   \n" +
                           "                                                                                      \n" +
                           "                                                                                      ");

        //Setup the RepairAPI port for the spart to start the service on
        port(getInstance().getContext().getConfig().getRepairAPIPort());
        get(PATH + "/hello", (req, res) -> "Helo, I am from cassandra_trunk sidecar");

        post(PATH + "/config/:schedule", (req, res) ->
                                         getInstance().updateRepairConfig(getParamSchedule(req), deserializeJson(TableTaskConfig.class, req.body())));

        get(PATH + "/config", (req, res) -> getInstance().getRepairConfig());

        get(PATH + "/config/:schedule", (req, res) -> getInstance().getRepairConfig(getParamSchedule(req)));

        get(PATH + "/status", (req, res) -> getInstance().getRepairStatus());

        get(PATH + "/status/:repairid", (req, res) -> getInstance().getRepairStatus(Integer.parseInt(req.params(":repairid"))));

        get(PATH + "/history", (req, res) -> getInstance().getRepairHistory());

        post(PATH + "/start", (req, res) -> getInstance().startRepair());

        post(PATH + "/stop", (req, res) -> getInstance().stopRepair());

        after("*", addRepairSchedulerHeaders);
    }

    // Copied from CassandraDaemon
    private static void logSystemInfo()
    {
        if (logger.isInfoEnabled())
        {
            try
            {
                logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
            }
            catch (UnknownHostException e1)
            {
                logger.info("Could not resolve local host");
            }

            logger.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version"));
            logger.info("Heap size: {}/{}",
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().totalMemory()),
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().maxMemory()));

            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans())
                logger.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());

            logger.info("Classpath: {}", System.getProperty("java.class.path"));

            logger.info("JVM Arguments: {}", ManagementFactory.getRuntimeMXBean().getInputArguments());
        }
    }
}

