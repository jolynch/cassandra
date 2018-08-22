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
package org.apache.cassandra.repair.scheduler.entity;

import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.Minutes;

public class ClusterTaskStatus
{
    private TaskStatus taskStatus;
    private String scheduleName;
    private int taskId = -1;
    private Date startTime, endTime, pauseTime;

    public TaskStatus getTaskStatus()
    {
        return taskStatus;
    }

    public ClusterTaskStatus setTaskStatus(TaskStatus taskStatus)
    {
        this.taskStatus = taskStatus;

        return this;
    }

    public ClusterTaskStatus setTaskStatus(String taskStatus)
    {

        this.taskStatus = TaskStatus.valueOf(taskStatus);
        return this;
    }

    public String getScheduleName()
    {
        return scheduleName;
    }

    public ClusterTaskStatus setScheduleName(String scheduleName)
    {
        this.scheduleName = scheduleName;
        return this;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public ClusterTaskStatus setTaskId(int repairId)
    {
        this.taskId = repairId;
        return this;
    }

    public Date getStartTime()
    {
        return startTime;
    }

    public ClusterTaskStatus setStartTime(Date startTime)
    {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime()
    {
        return endTime;
    }

    public ClusterTaskStatus setEndTime(Date endTime)
    {
        this.endTime = endTime;
        return this;
    }

    public Date getPauseTime()
    {
        return pauseTime;
    }

    public ClusterTaskStatus setPauseTime(Date pauseTime)
    {
        this.pauseTime = pauseTime;
        return this;
    }

    public int getTaskDurationInMinutes()
    {
        return Math.abs(Minutes.minutesBetween(new DateTime(startTime), new DateTime(endTime)).getMinutes());
    }
}
