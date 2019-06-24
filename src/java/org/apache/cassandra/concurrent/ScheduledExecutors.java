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
package org.apache.cassandra.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.ExecutorUtils;

/**
 * Centralized location for shared executors
 */
public class ScheduledExecutors
{
    /**
     * Holds all shared executors keyed by name and how long we should wait for them on shutdown
     */
    private static Map<String, DebuggableScheduledThreadPoolExecutor> executors = new HashMap<>();

    /**
     * Either retrieves an existing named DebuggabledScheduledThreadPoolExecutor (DSTPE), or creates a new one if it
     * does not exist. Note that this method is thread safe, and will create a single unique executor per unique
     * name passed.
     *
     * Use this instead of manually constructing DSTPE instances so that when Cassandra needs to properly shutdown
     * threadpools it can from this central location. These pools will be waited on for one minute by default to
     * shutdown. If you need more fine graind control over construction, use
     * {@link ScheduledExecutors#getOrCreateSharedExecutor(String, Function)}
     *
     * @param name The name of the DSTPE to get or create
     * @return Either a freshly constructed or previously cached DebuggableScheduledThreadPoolExecutor
     */
    public static DebuggableScheduledThreadPoolExecutor getOrCreateSharedExecutor(String name)
    {
        return getOrCreateSharedExecutor(name, DebuggableScheduledThreadPoolExecutor::new);
    }

    /**
     * Either retrieves an existing named DebuggabledScheduledThreadPoolExecutor (DSTPE), or creates a new one if it
     * does not exist. Note that this method is thread safe, and will create a single unique executor per unique
     * name passed.
     *
     * Use this instead of manually constructing DSTPE instances so that when Cassandra needs to properly shutdown
     * threadpools it can from this central location.
     *
     * @param name The name of the DSTPE to get or create
     * @param create The constructor you want to use to construct the DSTPE
     * @return Either a freshly constructed or previously cached DebuggableScheduledThreadPoolExecutor
     */
    public static synchronized DebuggableScheduledThreadPoolExecutor getOrCreateSharedExecutor(String name,
                                                                                               Function<String, DebuggableScheduledThreadPoolExecutor> create)
    {
        executors.putIfAbsent(name, create.apply(name));
        return executors.get(name);
    }

    /**
     * This pool is used for periodic fast (sub-microsecond) tasks.
     */
    public static final DebuggableScheduledThreadPoolExecutor scheduledFastTasks = getOrCreateSharedExecutor("ScheduledFastTasks");

    /**
     * This pool is used for periodic short (sub-second) tasks.
     */
    public static final DebuggableScheduledThreadPoolExecutor scheduledTasks = getOrCreateSharedExecutor("ScheduledTasks");

    /**
     * This executor is used for tasks that can have longer execution times, and usually are non periodic.
     */
    public static final DebuggableScheduledThreadPoolExecutor nonPeriodicTasks = getOrCreateSharedExecutor("NonPeriodicTasks");

    /**
     * This executor is used for tasks that do not need to be waited for on shutdown/drain.
     */
    public static final DebuggableScheduledThreadPoolExecutor optionalTasks = getOrCreateSharedExecutor("OptionalTasks");

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executors.values());
    }
}
