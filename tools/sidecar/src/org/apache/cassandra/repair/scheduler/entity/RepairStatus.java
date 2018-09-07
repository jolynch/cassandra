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

import org.apache.commons.lang3.StringUtils;

public enum RepairStatus {
    // Transitional States
    NOT_STARTED("NOT_STARTED"), STARTED("STARTED"), PAUSED("PAUSED"), REPAIR_HOOK_RUNNING("REPAIR_HOOK_RUNNING"),
    // Complete failure
    NOTIFS_LOST("NOTIFS_LOST"), FAILED("FAILED"), CANCELLED("CANCELLED"),
    // Complete success
    FINISHED("FINISHED");

    private final String status;

    RepairStatus(String status)
    {
        this.status = status;
    }

    public boolean readyToStartNew()
    {
        return this == RepairStatus.FINISHED;
    }

    public boolean isStarted()
    {
        return this == STARTED;
    }

    public boolean isPaused()
    {
        return this == PAUSED;
    }

    public boolean isCancelled()
    {
        return this == CANCELLED;
    }

    public boolean isCompleted()
    {
        if (StringUtils.isBlank(status))
        {
            return false;
        }

        switch (this)
        {
            case CANCELLED:
            case FINISHED:
            case FAILED:
            case NOTIFS_LOST:
                return true;
            default:
                return false;
        }
    }

    public boolean isTerminal()
    {
        switch (this)
        {
            case CANCELLED:
            case FINISHED:
            case FAILED:
                return true;
            default:
                return false;
        }
    }

    public boolean isFailed()
    {
        return this == FAILED;
    }

    public boolean isRepairHookRunning()
    {
        return this == REPAIR_HOOK_RUNNING;
    }

}
