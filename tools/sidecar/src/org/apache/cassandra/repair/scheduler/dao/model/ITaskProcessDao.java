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

import java.util.Optional;

import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;

/**
 * Interface to manage task_process table (Cluster level task metadata table)
 */
public interface ITaskProcessDao
{
    /**
     * Gets the latest task status on the cluster.
     * This gives the task status of entire cluster, task not necessarily need to run on the current instance
     *
     * @return Cluster task status information from task_process table, this does contain node level task status
     */
    Optional<ClusterTaskStatus> getClusterTaskStatus();

    /**
     * Get the cluster task status for a given task id
     *
     * @param taskId Repair Id to query task_process table for
     * @return Cluster task status information from task_process table, this does contain node level task status
     */
    Optional<ClusterTaskStatus> getClusterTaskStatus(int taskId);

    /**
     * This is the only coordination place in the task scheduler, Tries to insert a record with local cluster name
     * and task id into task_process table with IF NOT EXISTS (LWT) with a SERIAL consistency.
     *
     * @param taskId Repair Id to insert
     *                 boolean indicating the result of LWT operation
     */
    boolean acquireTaskInitLock(int taskId);

    /**
     * Marking Cluster task status completed on a task Id in task_process table
     *
     * @param taskId Repair Id to mark it as completed
     * @return boolean indicating the result of operation
     */
    boolean markClusterTaskFinished(int taskId);

    /**
     * Deletes Cluster task status for a task Id from task_process table
     *
     * @param taskId Repair Id to delete the status for
     * @return boolean indicating the result of operation
     */
    boolean deleteClusterTaskStatus(int taskId);

    /**
     * Updates Cluster task status for a task Id in task_process table
     *
     * @param clusterTaskStatus task_process table entry/ row
     * @return boolean indicating the result of operation
     */
    boolean updateClusterTaskStatus(ClusterTaskStatus clusterTaskStatus);
}
