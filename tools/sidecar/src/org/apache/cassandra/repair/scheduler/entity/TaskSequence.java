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
import java.util.Map;

import org.joda.time.DateTime;

public class TaskSequence implements Comparable<TaskSequence>
{
    private String clusterName;
    private int taskId;
    private Integer seq;
    private String nodeId;
    private Date creationTime;
    private Date startTime;
    private Date endTime;
    private Date pauseTime;
    private TaskStatus status;
    private Date lastHeartbeat;
    private String scheduleName;
    private Map<String, String> lastEvent;

    public TaskSequence()
    {
    }

    public TaskSequence(String clusterName, int taskId)
    {
        this.clusterName = clusterName;
        this.taskId = taskId;
    }

    String getClusterName()
    {
        return clusterName;
    }

    public TaskSequence setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public TaskSequence setTaskId(int taskId)
    {
        this.taskId = taskId;
        return this;
    }

    public Integer getSeq()
    {
        return seq;
    }

    public TaskSequence setSeq(Integer seq)
    {
        this.seq = seq;
        return this;
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public TaskSequence setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    private Date getCreationTime()
    {
        return creationTime;
    }

    public TaskSequence setCreationTime(Date creationTime)
    {
        this.creationTime = creationTime;
        return this;
    }

    Date getStartTime()
    {
        return startTime;
    }

    public TaskSequence setStartTime(Date startTime)
    {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime()
    {
        return endTime;
    }

    public TaskSequence setEndTime(Date endTime)
    {
        this.endTime = endTime;
        return this;
    }

    Date getPauseTime()
    {
        return pauseTime;
    }

    public TaskSequence setPauseTime(Date pauseTime)
    {
        this.pauseTime = pauseTime;
        return this;
    }

    public TaskStatus getStatus()
    {
        return status;
    }


    public TaskSequence setStatus(TaskStatus status)
    {
        this.status = status;
        return this;
    }

    public TaskSequence setStatus(String status)
    {
        this.status = TaskStatus.valueOf(status);
        return this;
    }

    Date getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    public boolean isLastHeartbeatBeforeMin(long numMin)
    {
        return (this.getLastHeartbeat() != null && this.getLastHeartbeat()
                                                       .before(DateTime.now().minusMinutes((int) numMin).toDate()));
    }

    public TaskSequence setLastHeartbeat(Date lastHeartbeat)
    {
        this.lastHeartbeat = lastHeartbeat;
        return this;
    }

    public boolean isLastHeartbeatBeforeMin(int numMin)
    {
        return this.getLastHeartbeat() != null
               && this.getLastHeartbeat().before(DateTime.now().minusMinutes(numMin).toDate());
    }

    public String getScheduleName()
    {
        return scheduleName;
    }

    public TaskSequence setScheduleName(String scheduleName)
    {
        this.scheduleName = scheduleName;
        return this;
    }

    public Map<String, String> getLastEvent()
    {
        return lastEvent;
    }

    public TaskSequence setLastEvent(Map<String, String> lastEvent)
    {
        this.lastEvent = lastEvent;
        return this;
    }


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof TaskSequence)) return false;

        TaskSequence that = (TaskSequence) o;

        if (getTaskId() != that.getTaskId()) return false;
        if (getClusterName() != null ? !getClusterName().equals(that.getClusterName()) : that.getClusterName() != null)
            return false;
        if (getSeq() != null ? !getSeq().equals(that.getSeq()) : that.getSeq() != null) return false;
        if (getNodeId() != null ? !getNodeId().equals(that.getNodeId()) : that.getNodeId() != null) return false;
        if (getCreationTime() != null ? !getCreationTime().equals(that.getCreationTime()) : that.getCreationTime() != null)
            return false;
        if (getStartTime() != null ? !getStartTime().equals(that.getStartTime()) : that.getStartTime() != null)
            return false;
        if (getEndTime() != null ? !getEndTime().equals(that.getEndTime()) : that.getEndTime() != null) return false;
        if (getPauseTime() != null ? !getPauseTime().equals(that.getPauseTime()) : that.getPauseTime() != null)
            return false;
        if (getStatus() != null ? !getStatus().equals(that.getStatus()) : that.getStatus() != null) return false;
        if (getLastHeartbeat() != null ? !getLastHeartbeat().equals(that.getLastHeartbeat()) : that.getLastHeartbeat() != null)
            return false;
        if (getScheduleName() != null ? !getScheduleName().equals(that.getScheduleName()) : that.getScheduleName() != null)
            return false;
        return getLastEvent() != null ? getLastEvent().equals(that.getLastEvent()) : that.getLastEvent() == null;
    }

    @Override
    public int hashCode()
    {
        int result = getClusterName() != null ? getClusterName().hashCode() : 0;
        result = 31 * result + getTaskId();
        result = 31 * result + (getSeq() != null ? getSeq().hashCode() : 0);
        result = 31 * result + (getNodeId() != null ? getNodeId().hashCode() : 0);
        result = 31 * result + (getCreationTime() != null ? getCreationTime().hashCode() : 0);
        result = 31 * result + (getStartTime() != null ? getStartTime().hashCode() : 0);
        result = 31 * result + (getEndTime() != null ? getEndTime().hashCode() : 0);
        result = 31 * result + (getPauseTime() != null ? getPauseTime().hashCode() : 0);
        result = 31 * result + (getStatus() != null ? getStatus().hashCode() : 0);
        result = 31 * result + (getLastHeartbeat() != null ? getLastHeartbeat().hashCode() : 0);
        result = 31 * result + (getLastEvent() != null ? getLastEvent().hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(TaskSequence o)
    {
        return this.getSeq().compareTo(o.getSeq());
    }

    @Override
    public String toString()
    {
        String sb = "TaskSequence{" + "clusterName='" + clusterName + '\'' +
                    ", taskId=" + taskId +
                    ", seq=" + seq +
                    ", nodeId='" + nodeId + '\'' +
                    ", creationTime=" + creationTime +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", pauseTime=" + pauseTime +
                    ", status='" + status + '\'' +
                    ", lastHeartbeat=" + lastHeartbeat +
                    ", lastEvent='" + lastEvent + '\'' +
                    ", scheduleName='" + scheduleName + '\'' +
                    '}';
        return sb;
    }
}
