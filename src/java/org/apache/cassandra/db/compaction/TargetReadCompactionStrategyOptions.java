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
    protected static final int  DEFAULT_MIN_THRESHOLD_LEVELS = 2;
    protected static final int DEFAULT_TARGET_READ_PER_READ = 4;
    protected static final long DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS = 60 * 60;
    protected static final int DEFAULT_MAX_READ_PER_READ = 12;
    protected static final long DEFAULT_MAX_COUNT = 2000;
    protected static final double DEFAULT_TIER_BUCKET_LOW = 0.25;
    protected static final double DEFAULT_TIER_BUCKET_HIGH = 1.75;

    protected static final String SPLIT_RANGE_KEY = "split_range";
    protected static final String MIN_THRESHOLD_LEVELS = "min_threshold_levels";
    protected static final String TARGET_SSTABLE_SIZE = "target_sstable_size_in_mb";
    protected static final String TARGET_READ_PER_READ = "target_read_per_read";
    protected static final String TARGET_REWRITE_INTERVAL_SECS = "target_rewrite_interval_in_seconds";
    protected static final String MAX_READ_PER_READ = "max_read_per_read";
    protected static final String MAX_SSTABLE_COUNT = "max_sstable_count";
    protected static final String TIER_BUCKET_LOW = "tier_bucket_low";
    protected static final String TIER_BUCKET_HIGH = "tier_bucket_high";

    protected final long splitRange;
    protected final int minThresholdLevels;
    protected final long targetSSTableSizeBytes;
    protected final int targetReadPerRead;
    protected final long targetRewriteIntervalSeconds;
    protected final long maxSSTableCount;
    protected final int maxReadPerRead;
    protected final double tierBucketLow;
    protected final double tierBucketHigh;

    public TargetReadCompactionStrategyOptions(Map<String, String> options)
    {
        splitRange = parseLong(options, SPLIT_RANGE_KEY, DEFAULT_SPLIT_RANGE);
        minThresholdLevels = parseInt(options, MIN_THRESHOLD_LEVELS, DEFAULT_MIN_THRESHOLD_LEVELS);
        targetSSTableSizeBytes = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_TARGET_SSTABLE_SIZE) * 1024L * 1024L;
        targetReadPerRead = parseInt(options, TARGET_READ_PER_READ, DEFAULT_TARGET_READ_PER_READ);
        targetRewriteIntervalSeconds = parseLong(options, TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS);
        maxSSTableCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        maxReadPerRead = parseInt(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        tierBucketLow = parseDouble(options, TIER_BUCKET_LOW, DEFAULT_TIER_BUCKET_LOW);
        tierBucketHigh = parseDouble(options, TIER_BUCKET_HIGH, DEFAULT_TIER_BUCKET_HIGH);
    }

    public TargetReadCompactionStrategyOptions()
    {
        this(Collections.emptyMap());
    }

    private static int parseInt(Map<String, String> options, String key, int defaultValue) throws ConfigurationException
    {
        String optionValue = options.get(key);
        try
        {
            return optionValue == null ? defaultValue : Integer.parseInt(optionValue);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int for %s", optionValue, key), e);
        }
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
        long splitRange = parseLong(options, SPLIT_RANGE_KEY, DEFAULT_SPLIT_RANGE);
        if (splitRange <= 0)
        {
            throw new ConfigurationException(String.format("%s must be positive: %d", SPLIT_RANGE_KEY, splitRange));
        }

        long targetSSTableSize = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_MIN_SSTABLE_SIZE) * 1024 * 1024;
        if (targetSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_SSTABLE_SIZE, targetSSTableSize));
        }


        long targetRewrite = parseLong(options, TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS);
        if (targetRewrite < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", SPLIT_RANGE_KEY, splitRange));
        }

        long maxCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        if (maxCount < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MAX_SSTABLE_COUNT, maxCount));
        }

        int targetRead = parseInt(options, TARGET_READ_PER_READ, DEFAULT_TARGET_READ_PER_READ);
        if (targetRead < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_READ_PER_READ, targetRead));
        }

        int minThresholdLevels = parseInt(options, MIN_THRESHOLD_LEVELS, DEFAULT_MIN_THRESHOLD_LEVELS);
        if (minThresholdLevels > targetRead) {
            throw new ConfigurationException(String.format("%s should not be larger than %s, %s > %s." +
                                                           "Consider increasing %s",
                                                           MIN_THRESHOLD_LEVELS, TARGET_READ_PER_READ,
                                                           minThresholdLevels, targetRead, TARGET_READ_PER_READ));
        }

        int maxRead = parseInt(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
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

        uncheckedOptions.remove(SPLIT_RANGE_KEY);
        uncheckedOptions.remove(TARGET_SSTABLE_SIZE);
        uncheckedOptions.remove(TARGET_READ_PER_READ);
        uncheckedOptions.remove(TARGET_REWRITE_INTERVAL_SECS);
        uncheckedOptions.remove(MAX_SSTABLE_COUNT);
        uncheckedOptions.remove(MAX_READ_PER_READ);

        return uncheckedOptions;
    }
}