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

package org.apache.cassandra.repair.scheduler.hooks;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;


public class TaskHookManagerTest
{
    @Test
    public void getRepairHook()
    {
        Assert.assertFalse(TaskHookManager.getRepairHook("non").isPresent());
        Assert.assertTrue(TaskHookManager.getRepairHook("CompactSTCSTaskHook").get() instanceof CompactSTCSTaskHook);
        Assert.assertTrue(TaskHookManager.getRepairHook("CompactAllTaskHook").get() instanceof CompactAllTaskHook);
        Assert.assertTrue(TaskHookManager.getRepairHook("CleanupTaskHook").get() instanceof CleanupTaskHook);
    }

    @Test
    public void getRepairHooks()
    {
        Assert.assertEquals(0, TaskHookManager.getRepairHooks(Arrays.asList("none", "foo", "bar")).size());
        Assert.assertEquals(1, TaskHookManager.getRepairHooks(Arrays.asList("none", "CompactSTCSTaskHook", "bar")).size());
        Assert.assertEquals(3, TaskHookManager.getRepairHooks(Arrays.asList("CleanupTaskHook", "CompactSTCSTaskHook", "CleanupTaskHook")).size());
    }
}