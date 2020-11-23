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
    protected static final long DEFAULT_WORK_UNIT_MULTIPLE = 16;
    protected static final long DEFAULT_TARGET_SSTABLE_SIZE = 1000L;
    protected static final int DEFAULT_TARGET_READ_PER_READ = 4;
    protected static final long DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS = 60 * 60; // 60 minutes
    protected static final int DEFAULT_MAX_READ_PER_READ = 12;
    protected static final long DEFAULT_MAX_COUNT = 2000;

    protected static final int DEFAULT_MIN_THRESHOLD_LEVELS = 2;
    protected static final int DEFAULT_MAX_LEVEL_AGE_SECONDS = 60 * 60 * 24 * 10;  // 10 days
    protected static final double DEFAULT_LEVEL_BUCKET_LOW = 0.75;
    protected static final double DEFAULT_LEVEL_BUCKET_HIGH = 1.25;


    protected static final String TARGET_WORK_UNIT = "target_work_unit_in_mb";
    protected static final String TARGET_SSTABLE_SIZE = "target_sstable_size_in_mb";
    protected static final String TARGET_READ_PER_READ = "target_read_per_read";
    protected static final String TARGET_REWRITE_INTERVAL_SECS = "target_rewrite_interval_in_seconds";
    protected static final String MAX_READ_PER_READ = "max_read_per_read";
    protected static final String MAX_SSTABLE_COUNT = "max_sstable_count";

    protected static final String MIN_THRESHOLD_LEVELS = "min_threshold_levels";
    protected static final String MAX_LEVEL_AGE_SECS = "max_level_age_in_seconds";
    protected static final String LEVEL_BUCKET_LOW = "level_bucket_low";
    protected static final String LEVEL_BUCKET_HIGH = "level_bucket_high";

    protected final long targetWorkSizeInBytes;
    protected final long targetSSTableSizeBytes;
    protected final int targetReadPerRead;
    protected final long targetRewriteIntervalSeconds;
    protected final long maxSSTableCount;
    protected final int maxReadPerRead;

    protected final int minThresholdLevels;
    protected final int maxLevelAgeSeconds;
    protected final double levelBucketLow;
    protected final double levelBucketHigh;

    public TargetReadCompactionStrategyOptions(Map<String, String> options)
    {
        long targetSSTableSizeInMb = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_TARGET_SSTABLE_SIZE);
        targetSSTableSizeBytes =  targetSSTableSizeInMb * 1024L * 1024L;
        // Default to some relatively large multiple of the SSTable size,
        targetWorkSizeInBytes = parseLong(options,
                                          TARGET_WORK_UNIT,
                                          targetSSTableSizeInMb * DEFAULT_WORK_UNIT_MULTIPLE) * 1024L * 1024L;

        targetReadPerRead = parseInt(options, TARGET_READ_PER_READ, DEFAULT_TARGET_READ_PER_READ);
        targetRewriteIntervalSeconds = parseLong(options, TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS);
        maxSSTableCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        maxReadPerRead = parseInt(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        minThresholdLevels = parseInt(options, MIN_THRESHOLD_LEVELS, DEFAULT_MIN_THRESHOLD_LEVELS);
        maxLevelAgeSeconds = parseInt(options, MAX_LEVEL_AGE_SECS, DEFAULT_MAX_LEVEL_AGE_SECONDS);
        levelBucketLow = parseDouble(options, LEVEL_BUCKET_LOW, DEFAULT_LEVEL_BUCKET_LOW);
        levelBucketHigh = parseDouble(options, LEVEL_BUCKET_HIGH, DEFAULT_LEVEL_BUCKET_HIGH);
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
        long targetSSTableSizeMb = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_TARGET_SSTABLE_SIZE);

        long targetSSTableSize = targetSSTableSizeMb * 1024 * 1024;
        if (targetSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_SSTABLE_SIZE, targetSSTableSize));
        }

        long targetWorkUnit = parseLong(options, TARGET_WORK_UNIT, DEFAULT_WORK_UNIT_MULTIPLE * targetSSTableSizeMb) * 1024 * 1024;
        if (targetWorkUnit <= 0)
        {
            throw new ConfigurationException(String.format("%s must be positive: %d", TARGET_WORK_UNIT, targetWorkUnit));
        }
        if (targetSSTableSize > targetWorkUnit)
        {
            throw new ConfigurationException(String.format("%s should not be larger than %s, %s > %s." +
                                                           "Consider increasing %s",
                                                           TARGET_SSTABLE_SIZE, TARGET_WORK_UNIT,
                                                           targetSSTableSize, targetWorkUnit, TARGET_WORK_UNIT));
        }

        long targetRewrite = parseLong(options, TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS);
        if (targetRewrite < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS));
        }

        long maxCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        if (maxCount < 50)
        {
            throw new ConfigurationException(String.format("%s must be larger than 10: %d", MAX_SSTABLE_COUNT, maxCount));
        }

        int targetRead = parseInt(options, TARGET_READ_PER_READ, DEFAULT_TARGET_READ_PER_READ);
        if (targetRead < 2)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than 2: %d", TARGET_READ_PER_READ, targetRead));
        }

        int minThresholdLevels = parseInt(options, MIN_THRESHOLD_LEVELS, DEFAULT_MIN_THRESHOLD_LEVELS);
        if (minThresholdLevels > targetRead) {
            throw new ConfigurationException(String.format("%s should not be larger than %s, %s > %s." +
                                                           "Consider increasing %s",
                                                           MIN_THRESHOLD_LEVELS, TARGET_READ_PER_READ,
                                                           minThresholdLevels, targetRead, TARGET_READ_PER_READ));
        }

        int maxRead = parseInt(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        if (maxRead < 3)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than 3: %d", MAX_READ_PER_READ, maxRead));
        }

        if (maxRead < targetRead)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than %s, %s < %s",
                                                           MAX_READ_PER_READ, TARGET_READ_PER_READ,
                                                           maxRead, targetRead));
        }

        int maxLevelAgeSeconds = parseInt(options, MAX_LEVEL_AGE_SECS, DEFAULT_MAX_LEVEL_AGE_SECONDS);
        if (maxLevelAgeSeconds < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MAX_LEVEL_AGE_SECS, maxLevelAgeSeconds));
        }

        uncheckedOptions.remove(TARGET_WORK_UNIT);
        uncheckedOptions.remove(TARGET_SSTABLE_SIZE);
        uncheckedOptions.remove(TARGET_READ_PER_READ);
        uncheckedOptions.remove(TARGET_REWRITE_INTERVAL_SECS);
        uncheckedOptions.remove(MAX_SSTABLE_COUNT);
        uncheckedOptions.remove(MAX_READ_PER_READ);
        uncheckedOptions.remove(MAX_LEVEL_AGE_SECS);

        return uncheckedOptions;
    }
}