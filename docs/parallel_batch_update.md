Tasklet parallelization of batch updates
===

The design for running TASK_INSERT on all tasklets of one DPU. The batch is sorted by key,
distributed to the tasklets along the shape of the tree, and each tasklet updates only its own share.

The externally visible behavior is the same as when running on a single tasklet. Both the
input/output format with the host (`docs/dpu_task_signature.md`) and the rule that "the last
one remains" when the same key is inserted more than once in a batch are unchanged.

TASK_DELETE also uses sorting and the handout ([parallel_delete.md](parallel_delete.md)).

## Terms

Listed in an order in which each term can be defined using only the ones before it.

*   **Share** — a contiguous range of the children of a node that one tasklet may update
*   **Depth** — the distance of a node from the root. The root has depth 0
*   **Group** — a contiguous interval of tasklets that jointly take charge of one node at a given depth
*   **Bucket** — one child of the node a group takes charge of. The smallest unit of assignment
*   **Local tree** — a tasklet's share, held as a self-contained B+ tree

## Sorting

The batch is sorted by key. The sort is **stable**, i.e., among equal keys it keeps the order in
which the host sent them. Stability is preserved in three places.

*   The write positions of the first partition (the first partition, which splits the whole
    batch) are assigned "by digit, and within a digit in tasklet-number order". Each tasklet
    reads its own section in the order the host sent it, and the sections themselves are
    arranged in that order
*   In partitions within a piece (a contiguous range of queries whose upper digits of the key
    agree), a single tasklet scans the piece from the beginning
*   The insertion sort that sorts small pieces does not swap the order of equal keys

**The query sequence moves back and forth between two regions (the region the host wrote and the
destination), but the queries `[begin, end)` occupy the same `[begin, end)` in either region.**
This is because the write destination of a partition never goes outside the range of the piece.
Hence neither pieces handed to different tasklets nor pieces within one tasklet overlap. At the
end of processing a piece (sorting it in WRAM, or moving a piece whose digits are used up), it is
**always written back to the region the host wrote**. This way there is no need to count whether
the number of partitions the piece has gone through is odd or even.

The number of elements of the MRAM stack that holds pieces (a tasklet's stack is not large
enough for nested C recursion) is bounded by "the number of values the most significant digit can
take + (the number of pieces pushed by one partition − 1) × the number of digits below the most
significant one". Each time we descend one digit, we pop one piece before pushing, so only its
siblings remain at that digit. The most significant digit is narrower, by the amount by which the
key width is not a multiple of the digit width, and can take fewer values. When the digit of the
first partition is lower than this, the number of pieces created there increases, but the number
of digits remaining below decreases by more, so this bound is not exceeded.

Defining `TASK_INSERT_SORT_CHECK` checks that the keys are non-decreasing right after sorting. On
real hardware the host reads the DPU log only on a fault, so a violation is written to the log
and then a fault is raised. This check is meaningful only right after sorting, so unlike the tree
structure check (`TASK_INSERT_CHECK`) it cannot be moved to the dedicated slot
(`docs/dpu_iram_overlay.md`).

## Handout

A standalone explanation of the handout is in [insert_handout.md](insert_handout.md).

**No two tasklets read or write the same node.** This is because a share contains whole subtrees,
and the boundaries of its key range coincide with node boundaries. Because of this property, a
share does not need to be represented as a sequence of subtrees; **the node itself and a range of
its children** suffice.

Since the batch is sorted by key, the positions of the queries falling into the key range of a
bucket form a contiguous interval. Its boundaries are found by binary-searching the batch for the
separator keys of the node (`INSERT_lower_bound`), at a cost of log2(width of the search range)
8-byte reads per boundary. **The query sequence is never scanned.** This is why no upper limit on
the descent depth is needed.

### Choosing the target load

The assignment is determined by a single **target load L** (the upper bound we aim for on the
number of queries any tasklet takes charge of). The number of tasklets `need(L)` required for a
given L is obtained by a greedy method that scans the buckets once in key order.

*   Under the contiguity constraint, this greedy method uses the minimum number of tasklets (by an
    exchange argument). Hence `need` is monotonically decreasing in L, and the smallest L that
    fits in the number of tasklets of the group can be found by binary search
*   The lower end of the search range is the total number of queries of the group divided by the
    number of tasklets of the group (rounded up). This is the load of a perfectly even split, and
    since one tasklet takes charge of at most L queries, any L below it always runs out of
    tasklets. The upper end is the load when one tasklet takes charge of everything
*   When a bucket that alone exceeds L is a leaf, it cannot be descended into, and that L is
    infeasible. The implementation's `need(L)` returns a value greater than the number of tasklets
    of the group, and the binary search raises L
*   Counting `ceil(q[b] / L)` rests on the assumption that "the queries of that bucket can be
    split into pieces of L each". Whether they really can is not known until we descend one level
    and look at the children of the bucket

`need(L)` does not necessarily equal the number of tasklets of the group. The leftover tasklets
are added one at a time to **the descending bucket with the largest number of queries per
tasklet**. Adding them to a non-descending bucket does not lower the maximum load, whereas in a
descending bucket it raises the parallelism of the handout inside it (the skew within a bucket is
not visible from outside, so adding tasklets never makes things worse). Repeating this until the
leftovers are used up makes the maximum number of queries per tasklet exactly minimal (by an
exchange argument).

## Updates and reconstruction

### Path cache

The current path is kept across queries. If a key exceeds the upper bound of the cached leaf, we
climb the cached path instead of starting from the root, and descend from the deepest node the key
falls within. **Since only the upper bound is kept for each node, a batch that is not in key
order must not be passed to this procedure.**

The cache is read and written only by its own tasklet, and no other tasklet accesses the local
tree, so invalidation need not be considered. However, **it must always be written back before
finishing the processing of the share**, because the subsequent steps (stitching leaves and
joining trees) read MRAM.

The roots of the whole trees (`cold_root` / `hot_root`) are used by every task, so they stay in
WRAM. The update procedure handles only trees in MRAM, so when there is no handout and in the
upsert of TASK_MOVE_HOT, the root of the whole tree is copied to MRAM and used only for the
duration of the batch.

### Stitching leaves

When `INSERT_execute` splits a leaf or increases its number of pairs, it rewrites `right` /
`left` of the neighboring leaf. At the edge of a share, the neighboring leaf belongs to another
tasklet, so doing this as is would conflict.

Therefore, before starting insertion, each tasklet sets `left` of its leftmost leaf and `right`
of its rightmost leaf to a null link. `INSERT_execute` skips the rewrite if the link is null, so
it never writes outside the share. After insertion, each tasklet finds its new leftmost and
rightmost leaves again, and **connects one boundary at each join of trees**. There is no need to
search for the partner to connect to: the two trees being joined are exactly the partners, and the
rightmost leaf of the receiving side meets the leftmost leaf of the donor side. Thus

> at any point in time, the tree a tasklet holds is a complete B+ tree, its leaf chain is
> closed, and no other tasklet touches it

holds to the end, and the fact that operations touching leaves do not spill outside the share can
be argued from the owner of the tree alone.

### Joining trees

**Joining** is the operation that turns two B+ trees covering adjacent key ranges, together with
the one key at their boundary, into a single B+ tree.

The rules are dictated by the **occupancy** constraint. In a B+ tree only the root may have just 2
children; a non-root node needs at least `MIN_NR_CHILDREN` children (at least `MIN_NR_PAIRS`
pairs for a leaf). The root of a local tree may have 2 children. So when joining, **the root of
the lower tree is dismantled, and its children are inserted into the node of the same height in
the higher tree**. The children obtained by dismantling are non-root nodes of the lower tree, so
they satisfy the lower bound as they are. Only the number of children of the receiving side
increases, so only the upper bound can be violated, and that is fixed by an ordinary split.

The insertion is done at **the edge of the higher tree that touches the boundary**. Since we
insert at the edge, none of the separator keys of the ancestors on the path need to be rewritten.

When the lower tree is a single leaf, it cannot be dismantled, so the leaf itself is inserted as a
child into the node of height 1. The root of a height-0 tree is still the leaf received in the
handout, and insertion only adds pairs, so it satisfies the lower bound on the number of pairs
even when placed as a non-root. Only when both are single leaves is a new root created with the
two as children.

If insertion makes the number of children exceed `MAX_NR_CHILDREN`, the node is split in half.
Both the children of the receiving side and the inserted children number at most
`MAX_NR_CHILDREN` each, so together at most twice that, and since twice `MIN_NR_CHILDREN` is
statically guaranteed to be at most `MAX_NR_CHILDREN`, halves do not fall below the lower bound.
This split propagates up the path, and if it reaches the root the height increases by 1, but not
exceeding `MAX_HEIGHT` follows from the lower bound on occupancy. **The premise for this is that
no join ever goes below the lower bound on occupancy.**

## Execution framework

### Phase boundaries

The boundaries use barriers built from the chains in `sync.h` (a scheme that passes readiness to
the neighboring tasklet in turn). A waiting tasklet is stopped, so it consumes no issue slots,
and unlike the SDK barriers it can cover only a subset of the tasklets. One barrier is a pair of
"wait for those behind" and "wait for those ahead"; **the order must be kept consistent, because
repeating the same direction deadlocks**. Work for tasklet 0 alone can be placed between the two
halves of this pair.

The sort is joined by tasklets with numbers below `TASK_INSERT_SORT_NR_TASKLETS`, and everything
after it by tasklets below `TASK_INSERT_NR_TASKLETS` (the former is at least the latter). The
invariant to keep is that **the number of barriers is the same for every tasklet that joins that
barrier**. An early return is allowed only when all joining tasklets leave at the same time.
There is also a barrier between batches, so that tasklets that join only the sort do not start
sorting the hot batch while the cold tree is being updated (they write to the same union
storage).

**A shared value published through a barrier must not be overwritten by its writer before
everyone has finished reading it.**

### MRAM work area

The work area is taken right after the result area. Its position is determined on the DPU side
alone from `qrys.result_offset`, which the host passes, and the size of the result area. It holds
the sort destination (the same size as the whole batch) and the piece stack. **That this area
fits before the end of MRAM is a precondition of the caller** (the same precondition is needed
when the host writes the query sequence).

### Without a handout

When the tree has only one leaf, or there are at most `TASK_INSERT_SORT_RUN` queries, there is no
handout and a single tasklet does the processing. That update can increase the height of the
tree, so **each tasklet reads the height before the barrier preceding the handout**. A tasklet
that is late would see the new height, and the branches would diverge.
