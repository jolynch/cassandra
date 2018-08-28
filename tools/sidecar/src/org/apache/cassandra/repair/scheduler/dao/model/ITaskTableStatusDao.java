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

import org.apache.cassandra.repair.scheduler.entity.TaskMetadata;

/**
 * Interface to manage repair_status table (Table and subrange level repair status).
 * Repair status holds te data for a particular subrange of a table within a node within a cluster
 */
public interface ITaskTableStatusDao
{
    /**
     * Update repair status with a given {@link TaskMetadata} in repair_status table
     * @param taskMetadata TaskMetadata
     * @return boolean indicating the result of this operation
     */
    boolean markTaskStatusChange(TaskMetadata taskMetadata);

    /**
     * Marks the repair as canceled in repair_status table
     * @param repairId Repair Id to mark the repair as canceled in repair_status table
     * @param nodeId NodeId to mark the repair as canceled in repair_status table
     * boolean indicating the result of this operation
     */
    boolean markTaskCancelled(int repairId, String nodeId);

    /**
     * Gets repair history from repair_status table
     * @param taskId RepairId to query for
     * @param keyspace Keyspace name to query for
     * @param table Table name to query for
     * @param nodeId NodeId to query for
     * @return List of {@link TaskMetadata} with repair_status records
     */
    List<TaskMetadata> getTaskHistory(int taskId, String keyspace, String table, String nodeId);

    /**
     * Gets repair history from repair_status table for a given repair id
     * @param taskId RepairId to query for
     * @return List of  {@link TaskMetadata} with repair_status records
     */
    List<TaskMetadata> getTaskHistory(int taskId);

    /**
     * Gets repair history from repair_status table without any filters. This could be very big depending on the
     * age of cluster, number of tables and age of the repair scheduler
     * @return List of {@link TaskMetadata} with repair_status records
     */
    List<TaskMetadata> getTaskHistory();

    /**
     * Gets repair history from repair_status table for a given repair and node id
     * @param repairId RepairId to query for
     * @param nodeId NodeId to query for
     * @return {@link TaskMetadata} with repair_status records
     */
    List<TaskMetadata> getTaskHistory(int repairId, String nodeId);
}
