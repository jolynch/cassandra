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

package org.apache.cassandra.repair.scheduler.tasks.repair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public enum RepairType
{
    DISABLED,

    INCREMENTAL,

    FULL;

    private static final Logger logger = LoggerFactory.getLogger(RepairType.class);

    public static RepairType fromString(String text)
    {
        try
        {
            return RepairType.valueOf(text.toUpperCase());
        }
        catch (IllegalArgumentException | NullPointerException e)
        {
            logger.warn("RepairType of {} is not allowed, choose from {}. Falling back to {}",
                        text, RepairType.class.getEnumConstants(), DISABLED
            );

            return DISABLED;
        }
    }
}