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

public class RepairMetadata
{
    private final Map<String, String> lastEvent = new HashMap<>();
    public int repairId;
    private String clusterName, nodeId, keyspaceName, tableName, startToken, endToken;
    private RepairStatus status;
    private Date createdTime, startTime, endTime, pauseTime;
    private int repairNum;
    private TableRepairConfig repairConfig;

    public RepairMetadata()
    {

    }

    public RepairMetadata(String clusterName, String nodeId, String keyspaceName, String tableName)
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

    public RepairMetadata setClusterName(String clusterName)
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

    public RepairMetadata setNodeId(String nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public RepairMetadata setKeyspaceName(String keyspaceName)
    {
        this.keyspaceName = keyspaceName;
        return this;
    }

    public String getTableName()
    {
        return tableName;
    }

    public RepairMetadata setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public String getStartToken()
    {
        return startToken;
    }

    public RepairMetadata setStartToken(String startToken)
    {
        this.startToken = startToken;
        return this;
    }

    public String getEndToken()
    {
        return endToken;
    }

    public RepairMetadata setEndToken(String endToken)
    {
        this.endToken = endToken;
        return this;
    }

    public RepairStatus getStatus()
    {
        return status;
    }

    public RepairMetadata setStatus(RepairStatus status)
    {
        this.status = status;
        return this;
    }

    public RepairMetadata setStatus(String status)
    {
        this.status = RepairStatus.valueOf(status);
        return this;
    }

    public Map<String, String> getLastEvent()
    {
        return lastEvent;
    }

    public RepairMetadata setLastEvent(String key, String value)
    {
        this.lastEvent.put(key, value);
        return this;
    }

    public RepairMetadata setEntireLastEvent(Map<String, String> data)
    {
        this.lastEvent.putAll(data);
        return this;
    }

    public Date getCreatedTime()
    {
        return createdTime;
    }

    public RepairMetadata setCreatedTime(Date createdTime)
    {
        this.createdTime = createdTime;
        return this;
    }

    public Date getStartTime()
    {
        return startTime;
    }

    public RepairMetadata setStartTime(Date startTime)
    {
        this.startTime = startTime;
        return this;
    }

    public Date getEndTime()
    {
        return endTime;
    }

    public RepairMetadata setEndTime(Date endTime)
    {
        this.endTime = endTime;
        return this;
    }

    public Date getPauseTime()
    {
        return pauseTime;
    }

    public RepairMetadata setPauseTime(Date pauseTime)
    {
        this.pauseTime = pauseTime;
        return this;
    }

    public int getRepairNum()
    {
        return repairNum;
    }

    public RepairMetadata setRepairNum(int repairNum)
    {
        this.repairNum = repairNum;
        return this;
    }

    public TableRepairConfig getRepairConfig()
    {
        return repairConfig;
    }

    public RepairMetadata setRepairConfig(TableRepairConfig repairConfig)
    {
        this.repairConfig = repairConfig;
        return this;
    }

    public int getRepairId()
    {
        return repairId;
    }

    public RepairMetadata setRepairId(int repairId)
    {
        this.repairId = repairId;
        return this;
    }

    @Override
    public String toString()
    {
        String sb = "RepairMetadata{" + "clusterName='" + clusterName + '\'' +
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
                    ", repairId=" + repairId +
                    '}';
        return sb;
    }
}
