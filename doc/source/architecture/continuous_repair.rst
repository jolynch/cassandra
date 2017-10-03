.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Continuous Repair Design Document
---------------------------------
This document proposes a new way to repair data that does not involve manually
scheduled jobs. I call it "continuous repair" because your cluster is constantly
doing incremental work based on mutations to keep it self consistent.

Problem:
~~~~~~~~
Manual, scheduled repair, is difficult for users of cassandra. Many
don’t do it because it’s difficult to schedule properly (single node at
a time, never overlapping), or because it causes heap or fd pressure
(often leading to a "partially failed" state that is particularly hard
to debug). This is particularly bad for large datasets where repair may
never finish, and even if it does finish it has to run for long enough
to *read the entire dataset* off disk; this naturally negatively impacts cache
and therefore read performance.

Background:
~~~~~~~~~~~
Cassandra has a number of ways to try to reduce entropy in the dataset:

1. Hinted Handoff:
   The mutation coordinator saves a "hint" to a hints file indicating that a
   particular node was unavailable during the operation; when that node comes
   back writes are replayed. The main issue with this is that the hints are
   expensive (lots of data on nodes that shouldn’t have that data), and if you
   go past the hint window or lose the coordinator before the hint can be
   replayed, you are forever inconsistent.

2. Read Repair:
   During a normal read, if the digests returned during a read differ, forcibly
   repair that partition. This involves basically no work if digests match, but
   if they differ you have to read the entire row, calculate the mutations
   required to fix it, and fix it. This can be particularly problematic for
   latency sensitive reads because it blocks

3. Manual Repair:
   Since #1 and #2 do not **guarantee** that mutations will be eventually
   consistent, you need manual repair which literally *reads all the data,
   calculates a diff and streams data to nodes*. This is possibly the most
   heavyweight way, but also it works. Incremental repair is a significant step
   forward where we only repair data that has been changed in new mutations
   (but a lot of that work is potentially wasted as well)

Proposal: Record rows needing repair at mutation time, pro-actively read-repair them
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

3.x already got a long way to making hints all you need (moving them off
your heap, making them durable on disk, etc ..), but they still have the
problem that when the hint window expires, or that coordinator dies
before it can replay hints, you have inconsistency that will only get
repaired through manual repair. In practice, we can't have an infinite hint
window because coordinators run out of disk space (especially for clusters
with many overwrites).

Coordinator write hints therefore do not guarantee eventual consistency because
they may not even happen at the coordinator (crash) and because it adds a lot
of load on the cluster due to storing the full mutation. I propose that in
addition to mutation hints, we introduce **read repair hints**, which are
recorded on replicas during the mutation path and indicate that a row is dirty
(might be inconsistent). A potential implementation
is:

1. During a mutation, each node that commits the write also writes to a system
   local table (or file) indicating which partition key is dirty (i.e. might
   need repair). This write includes the mutated primary key (pk + cc) write
   time of the mutation, and the replicas.

2. On each replica, a scheduled job (e.g. every 30s) checks if there are any
   potentially inconsistent rows whose replica sets are entirely available. If
   so, initiate read repair on those rows (rate limiting as needed); if not,
   continue on.

I believe eventual consistency is guaranteed as long as the full replica
set of just that key is live at some point in the future.

**Definitions:**

-  \_{<expression>} number of nodes that must participate

-  RF = Replication Factor \* number of Datacenters (effective RF)

-  D = number of down nodes

-  R = Read

-  W = Write

-  RR = Read Repair

**Performance:**

Unfortunately this simple protocol turns every mutation from a W\_{RF}
operation into a fairly expensive double write (regular + read repair)
and later read + write (read repair):

2\*W\_{RF} + (R\_{RF^2})

Optimization #1: Offload second write and amortize them
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can offload the RR hint to an asynchronous thread or buffer, which
only needs to be flushed to disk when a column family itself is flushed.
For heap control reasons this would need to be kept small to prevent
buffer bloat (similar to hint buffering). This means that we can very
efficiently write these RR hints to disk and hopefully not slow down
writes. This should hopefully get us to (amortized):

~1 W\_{RF} + (R\_{RF^2})

Optimization #2: Coordinator cleans read repair hints on successful write 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After a coordinator finishes the operation, if all nodes succeeded, it
sends deletes of the RR hints. Since these are a performance improvement
we don’t need to wait on the futures to complete, freeing coordinator
resources asap.

This improves the typical performance to:

~1 W\_{RF} + 1 cheap W\_{RF}

When nodes are down for prolonged periods, the same worst case perf
occurs.

~1 W\_{RF} + (R\_{RF^2})

Optimization #3: Hinting coordinators remove read repair hints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to original coordinators removing RR hints after a
successful write, they can remove hints for successful replicas and
coordinators replaying hints can clear RR hints from appropriate
replicas as well.

This sets the typical performance to:

~1 W\_{RF} + 1 cheap W\_{RF-D} write (async)

If nodes are down longer than the hint window, same worst performance
occurs, but if the node comes back during the window we get

~1 W\_{RF} + 1 cheap W\_{RF} write (async)

Optimization #4: Repairing replica removes read repair hints from other replicas
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When a node executes a successful read repair for a particular mutation,
it can remove the read repair hints from the other replicas as well.
I believe this improves the worst case in all situations to:

<= ~1 W\_{RF} + ~1 R\_{RF} + 1 cheap W\_{RF} write

Typical case (nodes are available) is
<= ~1 W\_{RF} + + 1 cheap W\_{RF} write

Concerns:
~~~~~~~~~
- How much does this slow down the write path
- How much load does this generate on data nodes reading data
- Wide rows, reading large large rows is expensive, perhaps storing mutations
  is better in the case of a particular large partition or column
- Range movements, what has to happen
