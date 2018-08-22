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

package org.apache.cassandra.repair.scheduler.dao.model;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.repair.scheduler.entity.RepairMetadata;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;

/**
 * Interface to manage repair hook metadata status (started, finished etc.,)
 */
public interface IRepairHookDao
{
    /**
     * Gets local cluster's repair hook statuses across all nodes for a give repair id
     * @param repairId Repair Id to query the repair hook status for
     * @return List of {@link RepairMetadata} with repair hook status
     */
    List<RepairMetadata> getLocalRepairHookStatus(int repairId);

    /**
     * Gets local cluster's repair hook status for a give repair id and node id
     * @param repairId Repair Id to query the repair hook status for
     * @param nodeId Nodes Id to query the repair hook status for
     * @return {@link RepairMetadata} object with repair hook status
     */
    RepairMetadata getLocalRepairHookStatus(int repairId, String nodeId);

    /**
     * Marks the post repair hook started for local node with a given repair id
     * @param repairId Repair Id to mark the repair hook status
     * @return boolean indicating the result of operation
     */
    boolean markLocalPostRepairHookStarted(int repairId);

    /**
     * Marks the post repair hook ended for local node with a given repair id and hook(s) status
     * @param repairId Repair Id to mark the repair hook status
     * @param status Repair hook status
     * @param hookSuccess Hook(s) status
     * @return boolean indicating the result of operation
     */
    boolean markLocalPostRepairHookEnd(int repairId, TaskStatus status, Map<String, Boolean> hookSuccess);
}
