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
import java.util.HashMap;
import java.util.Map;

public class TaskMetadata
{
    private final Map<String, String> lastEvent = new HashMap<>();
    public int taskId;
    private String clusterName, nodeId, keyspaceName, tableName, startToken, endToken;
    private TaskStatus status;
    private Date createdTime, startTime, endTime, pauseTime;
    private int repairNum;
    private TableTaskConfig repairConfig;

    public TaskMetadata()
    {

    }

    public TaskMetadata(String clusterName, String nodeId, String keyspaceName, String tableName)
    {
        this.clusterName = clusterName;
        this.nodeId = nodeId;
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public TaskMetadata setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }

    public String getKsTbName()
    {
        return keyspaceName + "." + tableName;
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public TaskMetadata setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public TaskMetadata setKeyspaceName(String keyspaceName)
    {
        this.keyspaceName = keyspaceName;
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    public TaskMetadata setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public String getStartToken()
    {
        return startToken;
    }

    public TaskMetadata setStartToken(String startToken)
    {
        this.startToken = startToken;
        return this;
    }

    public String getEndToken()
    {
        return endToken;
    }

    public TaskMetadata setEndToken(String endToken)
    {
        this.endToken = endToken;
        return this;
    }

    public TaskStatus getStatus()
    {
        return status;
    }

    public TaskMetadata setStatus(TaskStatus status)
    {
        this.status = status;
        return this;
    }

    public TaskMetadata setStatus(String status)
    {
        this.status = TaskStatus.valueOf(status);
        return this;
    }

    public Map<String, String> getLastEvent()
    {
        return lastEvent;
    }

    public TaskMetadata setLastEvent(String key, String value)
    {
        this.lastEvent.put(key, value);
        return this;
    }

    public TaskMetadata setEntireLastEvent(Map<String, String> data)
    {
        this.lastEvent.putAll(data);
        return this;
    }

    public Date getCreatedTime()
    {
        return createdTime;
    }

    public TaskMetadata setCreatedTime(Date createdTime)
    {
        this.createdTime = createdTime;
        return this;
    }

    public Date getStartTime()
    {
        return startTime;
    }

    public TaskMetadata setStartTime(Date startTime)
    {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime()
    {
        return endTime;
    }

    public TaskMetadata setEndTime(Date endTime)
    {
        this.endTime = endTime;
        return this;
    }

    public Date getPauseTime()
    {
        return pauseTime;
    }

    public TaskMetadata setPauseTime(Date pauseTime)
    {
        this.pauseTime = pauseTime;
        return this;
    }

    public int getRepairNum()
    {
        return repairNum;
    }

    public TaskMetadata setRepairNum(int repairNum)
    {
        this.repairNum = repairNum;
        return this;
    }

    public TableTaskConfig getRepairConfig()
    {
        return repairConfig;
    }

    public TaskMetadata setRepairConfig(TableTaskConfig repairConfig)
    {
        this.repairConfig = repairConfig;
        return this;
    }

    public int getTaskId()
    {
        return taskId;
    }

    public TaskMetadata setTaskId(int taskId)
    {
        this.taskId = taskId;
        return this;
    }

    @Override
    public String toString()
    {
        String sb = "TaskMetadata{" + "clusterName='" + clusterName + '\'' +
                    ", nodeId='" + nodeId + '\'' +
                    ", keyspaceName='" + keyspaceName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", startToken='" + startToken + '\'' +
                    ", endToken='" + endToken + '\'' +
                    ", status='" + status + '\'' +
                    ", lastEvent='" + lastEvent + '\'' +
                    ", createdTime=" + createdTime +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", pauseTime=" + pauseTime +
                    ", repairNum=" + repairNum +
                    ", taskId=" + taskId +
                    '}';
        return sb;
    }
}
