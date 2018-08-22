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
import java.util.Optional;
import java.util.SortedSet;

import org.apache.cassandra.repair.scheduler.entity.RepairHost;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;

/**
 * Interface to manage repair_sequence table ( Node level repair metadata table).
 * This manages repair status for each node in a cluster. Each node is identified with Sequence number
 * This sequence number might change for every round of repair, so it is not constant for node
 */
public interface IRepairSequenceDao
{
    /**
     * Marks the Node repair status as started for a repair Id and sequence
     *
     * @param repairId Repair Id to mark the status
     * @param seq      Sequence number to be used in the status update
     * @return boolean indicating the result of this operation
     */
    boolean markRepairStartedOnInstance(int repairId, int seq);

    /**
     * Marks the Node repair status as finished for a repair Id and sequence
     *
     * @param repairId Repair Id to mark the status
     * @param seq      Sequence number to be used in the status update
     * @param status
     * @return boolean indicating the result of this operation
     */
    boolean markNodeDone(int repairId, int seq, TaskStatus status);

    /**
     * Get the node sequence number of local node in a Repair Id
     *
     * @param repairId Repair Id to query for sequence
     * @return RepairSequence/ repair_sequence table row for local node
     */
    Optional<RepairSequence> getMyNodeStatus(int repairId);

    /**
     * Save sequence numbers for given hosts in a repair id and schedule name.
     * Sequence numbers are given in the order hosts are added to list
     *
     * @param repairId     Repair Id to persist the sequence information for
     * @param scheduleName Schedule name to persist the sequence data
     * @param allHosts     List of hosts in the order needed for sequencing. This is the same order the repairs
     *                     are going to be executed in this round of repair
     */
    void persistEndpointSeqMap(int repairId, String scheduleName, List<RepairHost> allHosts);

    /**
     * Gets the repair sequence data (node ids, seq etc.,) ordered by sequence number.
     *
     * @param repairId Repair Id to get the sequence data for
     * @return Ordered Repair Sequence/ node data, sorting order is based on sequence number
     */
    SortedSet<RepairSequence> getRepairSequence(int repairId);

    /**
     * Updates the heartbeat in repair_sequence table. This is critical to find out if the repair is stuck on a node
     *
     * @param repairId Repair Id to persist the heart beat for
     * @param seq      Sequence number of a node
     * @return boolean indicating the result of this operation
     */
    boolean updateHeartBeat(int repairId, int seq);

    /**
     * Marks the repair as cancelled for a given repair id and sequence number
     *
     * @param repairId Repair Id to cancel the repair for
     * @param seq      Sequence ( equivalent to node id) to mark the repair as canceled
     * @return boolean indicating the result of this operation
     */
    boolean cancelRepairOnNode(int repairId, Integer seq);
}
