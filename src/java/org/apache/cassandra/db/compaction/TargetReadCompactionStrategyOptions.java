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
package org.apache.cassandra.db.compaction;

import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class TargetReadCompactionStrategyOptions
{
    protected static final long DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    protected static final long DEFAULT_TARGET_SSTABLE_SIZE = 1000L * 1024L * 1024L;
    protected static final long DEFAULT_MAX_COUNT = 10000;
    protected static final long DEFAULT_TARGET_OVERLAP = 4;
    protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    protected static final String TARGET_SSTABLE_SIZE = "target_sstable_size_in_mb";
    protected static final String TARGET_OVERLAP = "target_overlap";
    protected static final String SSTABLE_MAX_COUNT = "max_sstable_count";


    protected long minSSTableSize;
    protected long targetSSTableSize;
    protected long targetOverlap;
    protected long maxSSTableCount;

    public TargetReadCompactionStrategyOptions(Map<String, String> options)
    {
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
        optionValue = options.get(TARGET_SSTABLE_SIZE);
        targetSSTableSize = optionValue == null ? DEFAULT_TARGET_SSTABLE_SIZE : Long.parseLong(optionValue) * 1024L * 1024L;
        optionValue = options.get(TARGET_OVERLAP);
        targetOverlap = optionValue == null ? DEFAULT_TARGET_OVERLAP: Long.parseLong(optionValue);
        optionValue = options.get(SSTABLE_MAX_COUNT);
        maxSSTableCount = optionValue == null ? DEFAULT_MAX_COUNT : Long.parseLong(optionValue);
    }

    public TargetReadCompactionStrategyOptions()
    {
        minSSTableSize = DEFAULT_MIN_SSTABLE_SIZE;
        targetSSTableSize = DEFAULT_TARGET_SSTABLE_SIZE;
        maxSSTableCount = DEFAULT_MAX_COUNT;
        targetOverlap = DEFAULT_TARGET_OVERLAP;
    }

    private static long parseLong(Map<String, String> options, String key, long defaultValue) throws ConfigurationException
    {
        String optionValue = options.get(key);
        try
        {
            return optionValue == null ? defaultValue : Long.parseLong(optionValue);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable long for %s", optionValue, key), e);
        }
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        long minSSTableSize = parseLong(options, MIN_SSTABLE_SIZE_KEY, DEFAULT_MIN_SSTABLE_SIZE);
        if (minSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE_KEY, minSSTableSize));
        }

        long targetSSTableSize = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_MIN_SSTABLE_SIZE);
        if (targetSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_SSTABLE_SIZE, minSSTableSize));
        }

        if (targetSSTableSize < minSSTableSize)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than %s", TARGET_SSTABLE_SIZE, MIN_SSTABLE_SIZE_KEY));
        }

        long maxCount = parseLong(options, SSTABLE_MAX_COUNT, DEFAULT_MAX_COUNT);
        if (maxCount < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", SSTABLE_MAX_COUNT, minSSTableSize));
        }

        long targetOverlap = parseLong(options, TARGET_OVERLAP, DEFAULT_TARGET_OVERLAP);
        if (targetOverlap < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_OVERLAP, minSSTableSize));
        }

        uncheckedOptions.remove(MIN_SSTABLE_SIZE_KEY);
        uncheckedOptions.remove(SSTABLE_MAX_COUNT);
        uncheckedOptions.remove(TARGET_SSTABLE_SIZE);
        uncheckedOptions.remove(TARGET_OVERLAP);

        return uncheckedOptions;
    }
}