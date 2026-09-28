# Handout for TASK_INSERT

The process of distributing a key-ordered batch among tasklets and giving each tasklet the information
it needs to update the tree on its own is called the **handout**.

## 1. Output

When the handout finishes, `partitions[t]` (`InsertPartition`) is ready for each tasklet t.

Properties of the output: each tasklet's **assignment** is a contiguous run of children of some node (the
node may differ between tasklets); listing the assignments in tasklet-number order gives key order; every
leaf of the tree lies under exactly one assignment; and the number of queries is close to even across
tasklets.

These properties serve different purposes. **"Exactly one" and the ordering guarantee correctness.**
Reconstruction concatenates the trees built by the tasklets in number order and rebuilds the upper levels,
so a subtree that belongs to no assignment becomes unreachable from the new root and is lost, and a subtree
that belongs to two assignments appears twice. Evenness, on the other hand, matters for performance: the
time of a batch is determined by the heaviest tasklet.

The output is a parent node and a range of its children. The edge leaves and local trees used by the update
and the reconstruction can be derived from it, and all tasklets perform the derivation in parallel, so it
adds no barriers.

Once the handout is decided, each element of `partitions` is read only by the tasklet it belongs to.

## 2. Storage for groups

One element of `partitionings` is handed out each time a group is created. **The element count
`TASK_INSERT_MAX_NR_PARTITIONINGS`, the number of tasklets − 1, is enough because the handout touches at most
that many nodes.** Touching one node draws one or more assignment boundaries inside the interval of that
group's tasklets. If there are two or more allotments, a boundary is drawn between them. If there is only one,
that allotment does not descend (an allotment that descends contains only one child, but an internal node has
two or more children), so it takes only one tasklet, and a boundary is drawn between it and the remaining
tasklets. Boundaries are drawn only inside nested intervals, so they do not coincide across nodes, and there
are only tasklets − 1 positions where they can be drawn.

## 3. Invariants of one round

*   **The tasklets taking part in the handout (those numbered below `TASK_INSERT_NR_TASKLETS`) pass barriers
    the same number of times.** Tasklets whose assignment is already settled, and tasklets that belong to no
    group in that round, still pass the barriers, and an early return happens only for all of them at once
*   **The groups created in one round occupy a contiguous range of `partitionings`** (because element indices
    are taken from the monotonically increasing `nr_partitionings`). The next round handles that range, and if
    the range is empty the handout ends there. All tasklets read the `nr_partitionings` used for this decision
    after a barrier, so they all end the same way

Group leaders may advance their plans in parallel because they write only to the elements of their own
group's tasklets and to the `partitionings` elements they took (byte writes to WRAM are native, so they do
not collide with adjacent elements). The exception, `nr_partitionings`, is incremented under a lock.

## 4. Search range for bucket boundaries

A boundary is the position of a parent's separator key in the query sequence, found with
`INSERT_lower_bound`. The search range is narrowed beforehand using two pieces of information.

*   The boundary that this tasklet found just before, in the same group. The boundaries a tasklet is
    responsible for increase in key order
*   The **piece of the first split** of the sort. When there are at most `TASK_INSERT_SORT_RUN` queries, the
    first split is not performed at all, so this narrowing is unavailable. The split was made on "the most
    significant digit at which the minimum and maximum keys differ" (the least significant digit if all
    digits match), so among keys that share the digits above it, digit order = key order, and the boundary
    always lies within the piece for the separator key's digit

**Pieces can narrow the search only for separator keys greater than the batch's minimum key and at most its
maximum key.** Other separator keys do not necessarily share the digits above that digit with the batch. In
that case, however, the group's queries are either all at least that separator key or all less than it, so
the boundary is at the start or the end of the query interval and no search is needed. This is expressed by
passing an empty search range. On the start side, what is actually returned is the boundary this tasklet
found just before; but if that were after the start, a query smaller than the minimum key would exist, which
is a contradiction, so it is the start.
