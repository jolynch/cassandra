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

import org.apache.cassandra.repair.scheduler.entity.ClusterRepairStatus;

/**
 * Interface to manage repair_process table (Cluster level repair metadata table)
 */
public interface IRepairProcessDao
{
    /**
     * Gets the latest repair status on the cluster.
     * This gives the repair status of entire cluster, repair not necessarily need to run on the current instance
     *
     * @return Cluster repair status information from repair_process table, this does contain node level repair status
     */
    Optional<ClusterRepairStatus> getClusterRepairStatus();

    /**
     * Get the cluster repair status for a given repair id
     *
     * @param repairId Repair Id to query repair_process table for
     * @return Cluster repair status information from repair_process table, this does contain node level repair status
     */
    Optional<ClusterRepairStatus> getClusterRepairStatus(int repairId);

    /**
     * This is the only coordination place in the repair scheduler, Tries to insert a record with local cluster name
     * and repair id into repair_process table with IF NOT EXISTS (LWT) with a SERIAL consistency.
     *
     * @param repairId Repair Id to insert
     *                 boolean indicating the result of LWT operation
     */
    boolean acquireRepairInitLock(int repairId);

    /**
     * Marking Cluster repair status completed on a repair Id in repair_process table
     *
     * @param repairId Repair Id to mark it as completed
     * @return boolean indicating the result of operation
     */
    boolean markClusterRepairFinished(int repairId);

    /**
     * Deletes Cluster repair status for a repair Id from repair_process table
     *
     * @param repairId Repair Id to delete the status for
     * @return boolean indicating the result of operation
     */
    boolean deleteClusterRepairStatus(int repairId);

    /**
     * Updates Cluster repair status for a repair Id in repair_process table
     *
     * @param clusterRepairStatus repair_process table entry/ row
     * @return boolean indicating the result of operation
     */
    boolean updateClusterRepairStatus(ClusterRepairStatus clusterRepairStatus);
}
