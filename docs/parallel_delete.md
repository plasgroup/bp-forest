# Parallel deletion

The design for running TASK_DELETE on all tasklets of one DPU. For each query it returns "whether
the key was originally present", and removes present keys from the tree.

## Semantics

When the same batch has several delete queries for the same key, **only the one nearest the front
returns "present"**. As a RESP server, it must appear as if the queries nearer the front of the
batch were processed first.

## Phases

One DPU launch goes through two phases. **The positions of the return values are determined by
the order the host sent, whereas we want to hand out the tree editing in key order**, so a phase
that builds the result array by rewriting only values comes first. Once it is done, the order
information has moved into the result array, so the second phase is free to sort the query
sequence. Sorting and the handout are as described in
[parallel_batch_update.md](parallel_batch_update.md) and [insert_handout.md](insert_handout.md).

The code of the two phases does not fit in the IRAM window at the same time, so two overlay slots
(`docs/dpu_iram_overlay.md`) are used. **Sorting the query sequence is for the second phase, but
it does not fit in the latter slot, so it is placed in the former.**

### How the tasklets are divided

The sort is joined by tasklets with numbers below `TASK_DELETE_SORT_NR_TASKLETS`, and building
the result array and editing the tree by tasklets below `TASK_DELETE_NR_TASKLETS` (the former is
at least the latter).

These two are for tuning speed. The upper bound of the workspace union is set by the sort branch
of insert (measured at 51,008 B); the delete branch is 28,560 B even with the default 16/16.

The barriers of building the result array run inside, between two barriers of the sort. The state
of a chain barrier (`sync.h`) exists per pair of adjacent tasklets, so the tasklets waiting outside
(numbers `TASK_DELETE_NR_TASKLETS` and above) touch only the pairs from
(`TASK_DELETE_NR_TASKLETS` - 1, `TASK_DELETE_NR_TASKLETS`) onward, and do not collide with the
inside.

## Value representation

Values are `int64_t`. Users can store values in the **63-bit signed range**, and the values
outside it are reserved.

| Value | Meaning |
|:-|:-|
| `INT64_MIN` | tombstone |
| `INT64_MIN + 1 + t` | tasklet `t` intends to return "deleted" |

Below, a "younger" tasklet or mark means one with a smaller tasklet number. Whether a mark younger
than one's own is already there is determined by a single comparison, `v < DELETE_CLAIM(me())`.
A tombstone falls on the side younger than anyone, so this expression also serves as "do not
touch a pair that has already been deleted".

Keys are shifted to unsigned with `key_int64_to_uint64` as before. This is because the digits of
the radix sort must be monotonic in the key, which is the standard way to radix-sort signed
integers. The order of values is used only by `TASK_RANGE_MAX`, so values are not shifted and are
compared as signed.

## Phase 1: result array

The query sequence is split into intervals among the tasklets. It is not sorted.

In the **first pass**, each tasklet descends the tree with the keys of its own queries, and if no
mark younger than its own is there, rewrites the value to its own mark and records the address of
that slot. In the **second pass**, it reads the recorded addresses, and if the mark is still its
own, returns "present" and rewrites the value to a tombstone.

The structure of the tree does not change in this phase, so the recorded addresses remain valid
until the second pass. They are recorded in MRAM taken after the area returned to the host, which
the host does not read.

**The second pass is needed because, at the time of the first pass, it is not yet decided whether
a younger tasklet will later take over one's mark.** Under interval partitioning, the rule that the
younger one wins coincides with "making it appear as if the front of the batch were processed
first". The result of the rewrites does not depend on arrival order, so the mark converges to that
of the youngest tasklet.

**Locking.** The reads and writes of the first pass are protected by a mutex pool. Only the first
pass needs locks. Only one mark survives, so a slot is written in the second pass by exactly one
tasklet; the others only read it, and whichever value they read, doing nothing is the correct
behavior.

Even in the first pass, the lock is skipped when the value read into the per-leaf cache is already
a mark younger than one's own. Marks only move toward the younger side, so a tasklet that has lost
once never turns into the winner later. If the cached value is an ordinary value, a mark may have
been placed after it was read, so it is re-read inside the lock.

For a query for which it did not write its own mark, a tasklet records "none" instead of an
address. The second pass can then answer "not present" without even reading the slot.

When the same tasklet has several delete queries for the same key, the value is written back to
`INT64_MIN` while the second pass processes the tasklet's queries in order, so the second and later
queries get "not present".

## Phase 2: physical deletion

Since a bucket is the smallest unit of assignment, **a run of equal keys made adjacent by the sort
never spans tasklets**. Pairs marked in the first phase have always become tombstones in the second
pass, so the second phase only needs to perform "remove the key if present".

### Within a local tree

A node that may fall below the lower bound either has a sibling inside the local tree (borrowing
and merging stay within the local tree) or is the root of the local tree. The root is exempt from
the lower bound.

Leaves are rebalanced **all at once, when leaving that leaf**. Since the batch is in key order,
deletions from one leaf are consecutive, so once per leaf suffices. Rebalancing every time a pair
is removed would remix the same leaf with its siblings many times.

**The leaf chain also stays within the local tree.** When a local tree is built, `right` of its
rightmost leaf and `left` of its leftmost leaf are cut to `NULL`. Both leaves being cut belong to
the tasklet itself, so this does not conflict with the neighboring share. As a result, merging
leaves never writes outside the chain.

### Joining local trees

As in `JOIN_trees`, the shorter side is chosen as the donor, its root is dismantled, and its
children are moved to the edge node of the receiving side. The leaf chain is also connected here
(parallel_batch_update.md, "Stitching leaves").

**Leaves below the lower bound are mixed at the join.** A leaf below the lower bound arises only
when a local tree has shrunk to a single leaf. That leaf is the root of the local tree, so it is
exempt from the lower bound, but it becomes a violation once the join makes it an ordinary leaf.
So when the donor is a single leaf below the lower bound, the join mixes it with the leaf of the
receiving side that it meets. If the pairs fit in one leaf, the donor disappears and there is
nothing to attach; if not, they are redistributed so that both satisfy the lower bound, and the
donor is attached as before (only the boundary key is updated). When both trees have height 0 and
the mixed result still has height 0, it is the root after the join, so it may be below the lower
bound.

The join already holds, as its path, the parent of the receiving side's leaf that is met (if the
donor has height 0, `jp.path[0]` is the node of height 1). The parent never loses a child, so
nothing propagates upward either.

When mixing makes one of the leaves disappear, the ends of the joined tree also change. The caller
cannot tell which one disappeared, so the join resets `leftmost_leaf` / `rightmost_leaf`.

**Mixing is allowed because the leaf chain is connected inside the join.** The leaves touched are
the two at the just-connected boundary and their neighbors, all of which are inside the tree being
assembled.

## Lifetime of tombstones

Since the second phase performs physical deletion every time, **tombstones never survive across
DPU launches**. Tombstones are visible only from the second pass of the first phase until the
second phase, and no other task runs in between. Hence readers of the tree need not consider
tombstones.

## Tasklet stacks

The call chain of deletion is deep (phase entry → driving the handout → batch → descent to a leaf
→ leaf rebalancing → upward propagation). There are only 640 bytes per tasklet, so the parts not
on the deepest path (building local trees, joining) are made `noinline` to separate their frames.
MRAM allocation is placed in the workspace.
