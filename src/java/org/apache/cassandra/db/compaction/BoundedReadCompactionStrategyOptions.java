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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class BoundedReadCompactionStrategyOptions
{
    protected static final long DEFAULT_MIN_SSTABLE_SIZE    = 64L;                       // 64 MiB
    protected static final long DEFAULT_TARGET_SSTABLE_SIZE = 1024L;                     // 1 GiB
    protected static final long DEFAULT_CONSOLIDATE_SIZE    = 8192L;                     // 8 GiB
    protected static final long DEFAULT_CONSOLIDATE_INTERVAL_SECONDS    = 60 * 60;       // 1 hour
    protected static final long DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS = 60 * 60 * 24;  // 24 hours
    protected static final double DEFAULT_MAX_WORK_UNIT = 0.125;                         // 12.5%
    protected static final int DEFAULT_MAX_READ_PER_READ = 8;
    protected static final long DEFAULT_MAX_COUNT = 2000;

    protected static final int DEFAULT_MIN_THRESHOLD_LEVELS = 4;
    protected static final int DEFAULT_MAX_LEVEL_AGE_SECONDS = 60 * 60 * 24 * 10;  // 10 days
    protected static final double DEFAULT_LEVEL_BUCKET_LOW = 0.75;
    protected static final double DEFAULT_LEVEL_BUCKET_HIGH = 1.25;

    protected static final String MIN_SSTABLE_SIZE = "min_sstable_size_in_mib";
    protected static final String TARGET_SSTABLE_SIZE = "target_sstable_size_in_mib";
    protected static final String TARGET_CONSOLIDATE_SIZE = "target_consolidate_size_in_mib";
    protected static final String TARGET_CONSOLIDATE_INTERVAL_SECS = "target_consolidate_interval_in_seconds";
    protected static final String TARGET_REWRITE_INTERVAL_SECS = "target_rewrite_interval_in_seconds";
    protected static final String MAX_WORK_UNIT = "max_work_unit_in_percent";
    protected static final String MAX_READ_PER_READ = "max_read_per_read";
    protected static final String MAX_SSTABLE_COUNT = "max_sstable_count";

    protected static final String MIN_THRESHOLD_LEVELS = "min_threshold_levels";
    protected static final String MAX_LEVEL_AGE_SECS = "max_level_age_in_seconds";
    protected static final String LEVEL_BUCKET_LOW = "level_bucket_low";
    protected static final String LEVEL_BUCKET_HIGH = "level_bucket_high";

    protected final long minSSTableSizeBytes;
    protected final long targetSSTableSizeBytes;
    protected final long targetConsolidateSizeInBytes;
    protected final long targetConsolidateIntervalSeconds;
    protected final long targetRewriteIntervalSeconds;
    protected final double maxWorkUnitPercent;
    protected final long maxSSTableCount;
    protected final int maxReadPerRead;

    protected final int minThresholdLevels;
    protected final int maxLevelAgeSeconds;
    protected final double levelBucketLow;
    protected final double levelBucketHigh;

    public BoundedReadCompactionStrategyOptions(Map<String, String> options)
    {
        minSSTableSizeBytes = parseLong(options, MIN_SSTABLE_SIZE, DEFAULT_MIN_SSTABLE_SIZE) * 1024 * 1024;
        targetSSTableSizeBytes =  parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_TARGET_SSTABLE_SIZE) * 1024L * 1024L;
        targetConsolidateSizeInBytes = parseLong(options, TARGET_CONSOLIDATE_SIZE, DEFAULT_CONSOLIDATE_SIZE) * 1024L * 1024L;
        targetConsolidateIntervalSeconds = parseLong(options, TARGET_CONSOLIDATE_INTERVAL_SECS, DEFAULT_CONSOLIDATE_INTERVAL_SECONDS);
        targetRewriteIntervalSeconds = parseLong(options, TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS);
        maxWorkUnitPercent = parseDouble(options, MAX_WORK_UNIT, DEFAULT_MAX_WORK_UNIT);
        maxSSTableCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        maxReadPerRead = parseInt(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        minThresholdLevels = parseInt(options, MIN_THRESHOLD_LEVELS, DEFAULT_MIN_THRESHOLD_LEVELS);
        maxLevelAgeSeconds = parseInt(options, MAX_LEVEL_AGE_SECS, DEFAULT_MAX_LEVEL_AGE_SECONDS);
        levelBucketLow = parseDouble(options, LEVEL_BUCKET_LOW, DEFAULT_LEVEL_BUCKET_LOW);
        levelBucketHigh = parseDouble(options, LEVEL_BUCKET_HIGH, DEFAULT_LEVEL_BUCKET_HIGH);
    }

    public BoundedReadCompactionStrategyOptions()
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
        long minSSTableSizeBytes = parseLong(options, MIN_SSTABLE_SIZE, DEFAULT_MIN_SSTABLE_SIZE) * 1024 * 1024;
        if (minSSTableSizeBytes < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE, minSSTableSizeBytes));
        }

        long targetSSTableSizeMb = parseLong(options, TARGET_SSTABLE_SIZE, DEFAULT_TARGET_SSTABLE_SIZE);
        long targetSSTableSize = targetSSTableSizeMb * 1024 * 1024;
        if (targetSSTableSize < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_SSTABLE_SIZE, targetSSTableSize));
        }
        if (minSSTableSizeBytes > targetSSTableSize)
        {
            throw new ConfigurationException(String.format("%s should not be larger than %s, %s > %s." +
                                                           "Consider increasing %s",
                                                           MIN_SSTABLE_SIZE, TARGET_SSTABLE_SIZE,
                                                           minSSTableSizeBytes, minSSTableSizeBytes, TARGET_SSTABLE_SIZE));
        }

        long targetConsolidateSize = parseLong(options, TARGET_CONSOLIDATE_SIZE, DEFAULT_CONSOLIDATE_SIZE) * 1024 * 1024;
        if (targetConsolidateSize <= 0)
        {
            throw new ConfigurationException(String.format("%s must be positive: %d", TARGET_CONSOLIDATE_SIZE, targetConsolidateSize));
        }
        if (targetSSTableSize > targetConsolidateSize)
        {
            throw new ConfigurationException(String.format("%s should not be larger than %s, %s > %s." +
                                                           "Consider increasing %s",
                                                           TARGET_SSTABLE_SIZE, TARGET_CONSOLIDATE_SIZE,
                                                           targetSSTableSize, targetConsolidateSize, TARGET_CONSOLIDATE_SIZE));
        }

        long targetConsolidate = parseLong(options, TARGET_CONSOLIDATE_INTERVAL_SECS, DEFAULT_CONSOLIDATE_INTERVAL_SECONDS);
        if (targetConsolidate < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_CONSOLIDATE_INTERVAL_SECS, targetConsolidate));
        }

        long targetRewrite = parseLong(options, TARGET_REWRITE_INTERVAL_SECS, DEFAULT_TARGET_REWRITE_INTERVAL_SECONDS);
        if (targetRewrite < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", TARGET_REWRITE_INTERVAL_SECS, targetRewrite));
        }

        double maxWorkUnit = parseDouble(options, MAX_WORK_UNIT, DEFAULT_MAX_WORK_UNIT);
        if (maxWorkUnit >= 1 || maxWorkUnit < 0.01)
        {
            throw new ConfigurationException(String.format("%s must be between [0.01, 1.0): %f", MAX_WORK_UNIT, maxWorkUnit));
        }

        long maxCount = parseLong(options, MAX_SSTABLE_COUNT, DEFAULT_MAX_COUNT);
        if (maxCount < 50)
        {
            throw new ConfigurationException(String.format("%s must be larger than 50: %d", MAX_SSTABLE_COUNT, maxCount));
        }

        int maxRead = parseInt(options, MAX_READ_PER_READ, DEFAULT_MAX_READ_PER_READ);
        if (maxRead < 2)
        {
            throw new ConfigurationException(String.format("%s cannot be smaller than 2: %d", MAX_READ_PER_READ, maxRead));
        }

        int minThresholdLevels = parseInt(options, MIN_THRESHOLD_LEVELS, DEFAULT_MIN_THRESHOLD_LEVELS);
        if (minThresholdLevels > maxRead) {
            throw new ConfigurationException(String.format("%s should not be larger than %s, %s > %s." +
                                                           "Consider increasing %s or set %s=0 to disable" +
                                                           "density tiering all together",
                                                           MIN_THRESHOLD_LEVELS, MAX_READ_PER_READ,
                                                           minThresholdLevels, maxRead, MAX_READ_PER_READ,
                                                           MIN_THRESHOLD_LEVELS));
        }

        int maxLevelAgeSeconds = parseInt(options, MAX_LEVEL_AGE_SECS, DEFAULT_MAX_LEVEL_AGE_SECONDS);
        if (maxLevelAgeSeconds < 0)
        {
            throw new ConfigurationException(String.format("%s must be non negative: %d", MAX_LEVEL_AGE_SECS, maxLevelAgeSeconds));
        }

        uncheckedOptions = new HashMap<>(options);

        uncheckedOptions.remove(MIN_THRESHOLD_LEVELS);
        uncheckedOptions.remove(MIN_SSTABLE_SIZE);
        uncheckedOptions.remove(TARGET_SSTABLE_SIZE);
        uncheckedOptions.remove(TARGET_CONSOLIDATE_SIZE);
        uncheckedOptions.remove(TARGET_CONSOLIDATE_INTERVAL_SECS);
        uncheckedOptions.remove(TARGET_REWRITE_INTERVAL_SECS);
        uncheckedOptions.remove(MAX_WORK_UNIT);
        uncheckedOptions.remove(MAX_SSTABLE_COUNT);
        uncheckedOptions.remove(MAX_READ_PER_READ);
        uncheckedOptions.remove(MAX_LEVEL_AGE_SECS);
        uncheckedOptions.remove(LEVEL_BUCKET_LOW);
        uncheckedOptions.remove(LEVEL_BUCKET_HIGH);

        return uncheckedOptions;
    }
}