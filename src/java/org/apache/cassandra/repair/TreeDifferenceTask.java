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
package org.apache.cassandra.repair;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.utils.MerkleTrees;

/**
 * Calculate ranges that differ between two provided trees
 */
public class TreeDifferenceTask extends AbstractFuture<TreeDifference> implements Runnable
{
    private final TreeResponse tr1;
    private final TreeResponse tr2;

    public TreeDifferenceTask(TreeResponse r1, TreeResponse r2)
    {
        this.tr1 = r1;
        this.tr2 = r2;
    }

    @Override
    public void run()
    {
        set(new TreeDifference(tr1.endpoint, tr2.endpoint, MerkleTrees.difference(tr1.trees, tr2.trees)));
    }
}
