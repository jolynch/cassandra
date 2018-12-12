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

package org.apache.cassandra.distributed;

import java.lang.ref.Reference;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.sun.jna.Native;

public class DistributedTestBase
{
    static String KEYSPACE = "distributed_test_keyspace";

    @BeforeClass
    public static void setup()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    TestCluster createCluster(int nodeCount) throws Throwable
    {
        TestCluster cluster = TestCluster.create(nodeCount);
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + nodeCount + "};");

        return cluster;
    }

    @After
    public void cleanup()
    {
        // ThreadLocals appear to leak our class loaders, see
        // https://stackoverflow.com/questions/3869026/how-to-clean-up-threadlocals
        try {
            // Get a reference to the thread locals table of the current thread
            Thread thread = Thread.currentThread();
            Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
            threadLocalsField.setAccessible(true);
            Object threadLocalTable = threadLocalsField.get(thread);

            // Get a reference to the array holding the thread local variables inside the
            // ThreadLocalMap of the current thread
            Class threadLocalMapClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
            Field tableField = threadLocalMapClass.getDeclaredField("table");
            tableField.setAccessible(true);
            Object table = tableField.get(threadLocalTable);

            // The key to the ThreadLocalMap is a WeakReference object. The referent field of this object
            // is a reference to the actual ThreadLocal variable
            Field referentField = Reference.class.getDeclaredField("referent");
            referentField.setAccessible(true);

            for (int i = 0; i < Array.getLength(table); i++) {
                // Each entry in the table array of ThreadLocalMap is an Entry object
                // representing the thread local reference and its value
                Object entry = Array.get(table, i);
                if (entry != null) {
                    // Get a reference to the thread local object and remove it from the table
                    ThreadLocal threadLocal = (ThreadLocal)referentField.get(entry);
                    threadLocal.remove();
                }
            }

            // TODO: Find a better way to not leak the class loaders via the options
            // static field in Native ... This is royally bad.
            Field libraries = Native.class.getDeclaredField("libraries");
            libraries.setAccessible(true);
            Field options = Native.class.getDeclaredField("options");
            options.setAccessible(true);
            Map<Class, ?> foo = (Map<Class, ?>) options.get(null);
            List<Class> toRemove = new ArrayList<>();
            for (Class klass : foo.keySet())
            {
                Native.unregister(klass);
                if (klass.getName().startsWith("org.apache.cassandra"))
                    toRemove.add(klass);
            }
            synchronized (libraries.get(null))
            {
                for (Class klass : toRemove)
                    foo.remove(klass);
            }
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void assertRows(Object[][] actual, Object[]... expected)
    {
        Assert.assertEquals(rowsNotEqualErrorMessage(expected, actual),
                            expected.length, actual.length);

        for (int i = 0; i < expected.length; i++)
        {
            Object[] expectedRow = expected[i];
            Object[] actualRow = actual[i];
            Assert.assertTrue(rowsNotEqualErrorMessage(actual, expected),
                              Arrays.equals(expectedRow, actualRow));
        }
    }

    public static String rowsNotEqualErrorMessage(Object[][] actual, Object[][] expected)
    {
        return String.format("Expected: %s\nActual:%s\n",
                             rowsToString(expected),
                             rowsToString(actual));
    }

    public static String rowsToString(Object[][] rows)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        boolean isFirst = true;
        for (Object[] row : rows)
        {
            if (isFirst)
                isFirst = false;
            else
                builder.append(",");
            builder.append(Arrays.toString(row));
        }
        builder.append("]");
        return builder.toString();
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }
}
