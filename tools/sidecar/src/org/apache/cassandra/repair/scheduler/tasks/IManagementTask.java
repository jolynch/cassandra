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

package org.apache.cassandra.repair.scheduler.tasks;

import java.util.Optional;

import org.apache.cassandra.repair.scheduler.dao.model.IRepairConfigDao;
import org.apache.cassandra.repair.scheduler.entity.ClusterTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.LocalRepairState;
import org.apache.cassandra.repair.scheduler.entity.RepairSequence;
import org.apache.cassandra.repair.scheduler.entity.TableTaskConfig;
import org.apache.cassandra.repair.scheduler.hooks.IRepairHook;

public interface IManagementTask
{
    /** Generally defined by the implementation **/

    boolean canRunTask(int taskId);

    public boolean amIReadyForPostTaskHook(int repairId);

    Optional<RepairSequence> amINextInSequenceOrDone(int repairId);

    void prepareForTaskOnNode(int repairId, RepairSequence seq);

    boolean cancelTaskOnNode(int taskId, String reason, RepairSequence seq);

    /** Generally left to the base class **/

    public boolean runHook(IRepairHook hook, TableTaskConfig tableConfig)

    int getTaskId();

    boolean isTaskDoneOnCluster(int repairId);

    Optional<ClusterTaskStatus> getClusterTaskStatus();

    boolean isSequencePausedOnCluster(int taskId);

    boolean isSequenceRunningOnCluster(int taskId);

    boolean isSequenceGenerationStuckOnCluster(int taskId);

    boolean attemptClusterTaskStart(int proposedRepairId);

    void populateTaskSequence(int repairId, String scheduleName);

    void abortTaskOnCluster(int taskId);

    void pauseTaskOnCluster(int taskId);

    Optional<RepairSequence> getStuckSequence(int taskId);

    void abortTaskOnStuckSequence(RepairSequence sequence);

    boolean finishClusterRepair(LocalRepairState repairState);

    IRepairConfigDao getRepairConfigDao();

}
