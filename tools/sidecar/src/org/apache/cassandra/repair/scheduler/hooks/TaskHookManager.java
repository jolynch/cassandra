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
package org.apache.cassandra.repair.scheduler.hooks;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * This class is responsible for translation user specified compaction hooks to actionable tasks to repair scheduler.
 * TaskHookManager is a glue between table repair configurations and repair scheduler
 */
public class TaskHookManager
{
    private static final Logger logger = LoggerFactory.getLogger(TaskHookManager.class);

    /**
     * Gets repairHook from class name
     *
     * @param repairHookClassName class name for the repair hook, it could be fully qualified class name or
     *                            simple class name
     * @return RepairHook if the classname matches with any class path
     */
    static Optional<ITaskHook> getRepairHook(String repairHookClassName)
    {
        try
        {
            if (!repairHookClassName.contains("."))
                repairHookClassName = "org.apache.cassandra.repair.scheduler.hooks." + repairHookClassName;
            return Optional.of(FBUtilities.construct(repairHookClassName, "RepairHook"));
        }
        catch (ConfigurationException ex)
        {
            logger.error("Did not find RepairHookClassName: [{}] in classpath, hence using No-Op/ null implementation of RepairHook", repairHookClassName);
            return Optional.empty();
        }
    }

    /**
     * Read post repair hook types for the table from config, Load valid repair hooks from them
     *
     * @param repairHookClassNames List of repair hook class names from config
     * @return Valid repair hooks
     */
    public static List<ITaskHook> getRepairHooks(List<String> repairHookClassNames)
    {
        List<ITaskHook> repairHooks = new LinkedList<>();
        for (String repairHookClassName : repairHookClassNames)
        {
            Optional<ITaskHook> repairHook = TaskHookManager.getRepairHook(repairHookClassName);

            if (repairHook.isPresent())
            {
                repairHooks.add(repairHook.get());
            }
            else
            {
                logger.warn("Found invalid repair hook class name [{}]", repairHookClassName);
            }
        }
        return repairHooks;
    }
}
