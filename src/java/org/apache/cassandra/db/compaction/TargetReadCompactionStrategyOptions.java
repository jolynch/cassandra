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

import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class TargetReadCompactionStrategyOptions
{
    protected static final long DEFAULT_MIN_SSTABLE_SIZE = 1L;
    protected static final long DEFAULT_SPLIT_RANGE = 16L;
    protected static final long DEFAULT_TARGET_SSTABLE_SIZE = 1024L;
    protected static final long DEFAULT_MAX_COUNT = 2000;
    protected static final long DEFAULT_TARGET_READ = 4;
    protected static final long DEFAULT_MAX_READ_PER_READ = 12;
    protected static final double DEFAULT_TIER_BUCKET_LOW = 0.25;
    protected static final double DEFAULT_TIER_BUCKET_HIGH = 1.75;

    protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    protected static final String SPLIT_RANGE_KEY = "split_range";
    protected static final String TARGET_SSTABLE_SIZE = "target_sstable_size_in_mb";
    protected static final String TARGET_READ_PER_READ = "target_read_per_read";
    protected static final String MAX_READ_PER_READ = "max_read_per_read";
    protected static final String MAX_SSTABLE_COUNT = "max_sstable_count";
    protected static final String TIER_BUCKET_LOW = "tier_bucket_low";
    protected static final String TIER_BUCKET_HIGH = "tier_bucket_high";

    protected final long minSSTableSize;
    protected final long splitRange;
    protected final long targetSSTableSize;
    protected final long targetReadPerRead;
    protected final long maxSSTableCount;
    protected final long maxReadPerRead;
    protected final double tierBucketLow;
    protected final double tierBucketHigh;

    public TargetReadCompactionStrategyOptions(Map<String, String> options)
    {
        minSSTableSize = parseLong(options, MIN_SSTABLE_SIZE_KEY, DEFAULT_MIN_SSTABLE_SIZE) * 1024L * 1024L;
        splitRange = parseLong(options, SPLIT_RANGE_KEY, DEFAULT_SPLIT_RANGE);
        targetSSTableSize = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_TARGET_SSTABLE_SIZE) * 1024L * 1024L;
        targetReadPerRead = parseLong(options, TARGET_READ_PER_READ, DEFAULT_TARGET_READ);
        maxSSTableCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        maxReadPerRead = parseLong(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        tierBucketLow = parseDouble(options, TIER_BUCKET_LOW, DEFAULT_TIER_BUCKET_LOW);
        tierBucketHigh = parseDouble(options, TIER_BUCKET_HIGH, DEFAULT_TIER_BUCKET_HIGH);
    }

    public TargetReadCompactionStrategyOptions()
    {
        this(Collections.emptyMap());
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

    private static double parseDouble(Map<String, String> options, String key, double defaultValue) throws ConfigurationException
    {
        String optionValue = options.get(key);
        try
        {
            return optionValue == null ? defaultValue : Double.parseDouble(optionValue);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable double for %s", optionValue, key), e);
        }
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        long minSSTableSize = parseLong(options, MIN_SSTABLE_SIZE_KEY, DEFAULT_MIN_SSTABLE_SIZE) * 1024 * 1024;
        if (minSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE_KEY, minSSTableSize));
        }

        long splitRange = parseLong(options, SPLIT_RANGE_KEY, DEFAULT_SPLIT_RANGE);
        if (splitRange < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", SPLIT_RANGE_KEY, splitRange));
        }

        long targetSSTableSize = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_MIN_SSTABLE_SIZE) * 1024 * 1024;
        if (targetSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_SSTABLE_SIZE, targetSSTableSize));
        }


        if (targetSSTableSize < minSSTableSize)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than %s, %s < %s",
                                                           TARGET_SSTABLE_SIZE, MIN_SSTABLE_SIZE_KEY,
                                                           targetSSTableSize, minSSTableSize));
        }

        long maxCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        if (maxCount < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MAX_SSTABLE_COUNT, maxCount));
        }

        long targetRead = parseLong(options, TARGET_READ_PER_READ, DEFAULT_TARGET_READ);
        if (targetRead < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_READ_PER_READ, targetRead));
        }

        long maxRead = parseLong(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        if (maxRead < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MAX_READ_PER_READ, maxRead));
        }

        if (maxRead < targetRead)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than %s, %s < %s",
                                                           MAX_READ_PER_READ, TARGET_READ_PER_READ,
                                                           maxRead, targetRead));
        }

        uncheckedOptions.remove(MIN_SSTABLE_SIZE_KEY);
        uncheckedOptions.remove(MAX_SSTABLE_COUNT);
        uncheckedOptions.remove(TARGET_SSTABLE_SIZE);
        uncheckedOptions.remove(TARGET_READ_PER_READ);
        uncheckedOptions.remove(MAX_READ_PER_READ);

        return uncheckedOptions;
    }
}