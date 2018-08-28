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

import org.apache.cassandra.repair.scheduler.TaskDaoManager;
import org.apache.cassandra.repair.scheduler.config.TaskSchedulerContext;
import org.apache.cassandra.repair.scheduler.entity.LocalTaskStatus;
import org.apache.cassandra.repair.scheduler.entity.TaskSequence;
import org.apache.cassandra.repair.scheduler.entity.TaskStatus;

public class NoOpTask extends BaseTask
{
    public NoOpTask(TaskSchedulerContext context, TaskDaoManager daoManager)
    {
        super(context, daoManager);
    }

    @Override
    public TaskStatus runTask(LocalTaskStatus state)
    {
        return TaskStatus.FINISHED;
    }

    @Override
    public boolean amIReadyForPostTaskHook(int taskId)
    {
        return true;
    }

    @Override
    public boolean cancelTaskOnNode(int taskId, String reason, TaskSequence seq)
    {
        return false;
    }
}
