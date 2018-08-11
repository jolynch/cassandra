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


public class RepairHookManagerTest
{
    @Test
    public void getRepairHook()
    {
        Assert.assertFalse(RepairHookManager.getRepairHook("non").isPresent());
        Assert.assertTrue(RepairHookManager.getRepairHook("CompactSTCSRepairHook").get() instanceof CompactSTCSRepairHook);
        Assert.assertTrue(RepairHookManager.getRepairHook("CompactAllRepairHook").get() instanceof CompactAllRepairHook);
        Assert.assertTrue(RepairHookManager.getRepairHook("CleanupRepairHook").get() instanceof CleanupRepairHook);
    }

    @Test
    public void getRepairHooks()
    {
        Assert.assertEquals(0, RepairHookManager.getRepairHooks(Arrays.asList("none", "foo", "bar")).size());
        Assert.assertEquals(1, RepairHookManager.getRepairHooks(Arrays.asList("none", "CompactSTCSRepairHook", "bar")).size());
        Assert.assertEquals(3, RepairHookManager.getRepairHooks(Arrays.asList("CleanupRepairHook", "CompactSTCSRepairHook", "CleanupRepairHook")).size());
    }
}