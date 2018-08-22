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

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * How should the repair scheduler split full range repairs up. Split calculation is critical to ensure a good
 * balance of overstreaming vs generating too many small sstables for compaction to deal with.
 *
 * the _DRY_RUN options allow operators to experiment with this split strategy before turning it on if they want
 * to.
 */
public class RepairSplitStrategy
{
    public enum Strategy { DISABLED, PARTITION, PARTITION_DRY_RUN, SIZE, SIZE_DRY_RUN, ADAPTIVE, ADAPTIVE_DRY_RUN}

    private long value;
    private Strategy strategy;

    /**
     * Default constructor needed for Jackson JSON Deserialization
     */
    public RepairSplitStrategy()
    {

    }

    public RepairSplitStrategy(String input) {
        value = 0;
        try
        {
            strategy = Strategy.valueOf(input.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            try
            {
                String[] inp = input.split("_");
                if (inp.length == 1)
                {
                    strategy = Strategy.PARTITION;
                    value = Long.parseLong(inp[0]);
                }
                else if (inp.length == 2 && inp[1].equalsIgnoreCase("kb"))
                {
                    strategy = Strategy.SIZE;
                    value = Long.parseLong(inp[0]);
                }
                else if (inp.length == 3 && inp[1].equalsIgnoreCase("dry"))
                {
                    strategy = Strategy.PARTITION_DRY_RUN;
                    value = Long.parseLong(inp[0]);
                }
                else if (inp.length == 4 && inp[1].equalsIgnoreCase("kb"))
                {
                    strategy = Strategy.SIZE_DRY_RUN;
                    value = Long.parseLong(inp[0]);
                }
                else
                {
                    strategy = Strategy.DISABLED;
                }

            }
            catch (Exception ignored)
            {
                strategy = Strategy.DISABLED;
            }
        }
    }

    public long getValue()
    {
        return value;
    }

    public Strategy getStrategy()
    {
        return strategy;
    }

    @JsonIgnore
    public boolean isDryRun()
    {
        switch (this.strategy)
        {
            case ADAPTIVE_DRY_RUN:
            case SIZE_DRY_RUN:
            case PARTITION_DRY_RUN:
                return true;
            default:
                return false;
        }
    }

    public String toString() {
        switch (this.strategy)
        {
            case SIZE:
                return this.value + "_mb";
            case SIZE_DRY_RUN:
                return this.value + "_mb_dry_run";
            case PARTITION:
                return String.valueOf(value);
            case PARTITION_DRY_RUN:
                return value + "_dry_run";
            default:
                return this.strategy.toString();
        }
    }


}
