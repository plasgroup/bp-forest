Input and Output of DPU Tasks
===

The DPU program (`dpu/src/dpumain.c`) runs one kind of task per launch.
The task to run and its arguments are specified by the `InputHeader` placed at the start of the MRAM heap
(`common/inc/input_header.h`, fixed at 16 bytes), and the results are also written back to MRAM.
This document describes each task's "arguments → return values" and their layout in MRAM.

## Notation and common conventions

*   Tasks are written in the form `(arguments, ...) -> (return values, ...)`.
    *   The leading scalar arguments are passed as members of the `InputHeader` union.
    *   The arrays that follow are placed in the **payload**, i.e., the region starting at the heap start +
        `sizeof(InputHeader)` (= 16).
    *   The layout of return values differs by task (see each entry below).
*   `Key` = `key_uint64_t`, `Value` = `value_uint64_t` (`common/inc/workload_types.h`).
    `KVPair` / `KeyRange` / `RangeCountQuery` are in `common/inc/common.h`.
*   Most tasks target both the cold tree and the hot tree. Arrays of queries or pairs are passed
    **contiguously as `[cold | hot]`**, and the header gives the count of each.
*   The list of task IDs is `enum TaskID` (`common/inc/common.h`).

Correspondence between `InputHeader` union members and tasks:

| union member | Contents | Used by |
| --- | --- | --- |
| `init` | `uint32_t nr_cold_pairs, nr_hot_pairs` | TASK_INIT |
| `qrys` | `uint32_t nr_cold_qrys, nr_hot_qrys, result_offset` | TASK_GET / PRED / RANGE_COUNT / RANGE_MAX / INSERT / DELETE |
| `move_hot` | `uint32_t nr_cold_pairs, nr_hot_pairs; bool renew_cold, renew_hot` | TASK_MOVE_HOT |
| `serialize` | `uint32_t nr_delims, max_nr_delims; bool do_cold, do_hot` | TASK_SERIALIZE |

`qrys.result_offset` is the position where return values are written, as a byte offset from the heap start.
Using the maximum query count over all DPUs, `max_nqrys`, the host sets it to
`sizeof(InputHeader) + sizeof(Query) * max_nqrys` plus the maximum length of whatever follows the query sequence
(the min-refresh requests of TASK_DELETE)
(`BPForest::execute_in_dpus`, `bpforest/inc/bpforest.ipp`). Using the same value on all DPUs lets the results
be transferred in bulk from a single offset.

Per-query return values are placed in `[cold | hot]` order, and **each section is rounded up to an 8-byte
boundary** (this matters only for TASK_DELETE, which returns 1-byte values). This way the results for the two
trees do not share an 8-byte DMA word, so tasklets can write them out without regard to each other.
Return values that do not correspond one-to-one to queries, such as live pair counts, are placed after them.

The most readable reference implementation of the current protocol is `bpforest/inc/fake_dpu.ipp` (the DPU
emulator for the fake_dpu build). The actual DPU side is `dpu/src/bplustree.c`.

## Implemented tasks

These are the tasks that have a branch in the `switch` of `dpu/src/dpumain.c`. Tasks marked with a `SUPPORT_*`
macro are included in the DPU binary only in builds that define that macro (pass `-DSUPPORT_GET` etc. in
CMake's `CMAKE_C_FLAGS`).

### TASK_INIT

```
(uint32_t nr_cold_pairs, uint32_t nr_hot_pairs, KVPair[nr_cold_pairs + nr_hot_pairs]) -> ()
```

Builds the cold tree and the hot tree bottom-up from a `[cold | hot]` pair sequence sorted by key in ascending
order. The construction algorithm is in `docs/tree_initialization.md`.

### TASK_GET (`SUPPORT_GET`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 Key[nr_cold_qrys + nr_hot_qrys]) -> Value[nr_cold_qrys + nr_hot_qrys]
```

Returns `NOT_FOUND_VALUE` if the key does not exist.

### TASK_PRED (`SUPPORT_PRED`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 Key[nr_cold_qrys + nr_hot_qrys]) -> KVPair[nr_cold_qrys + nr_hot_qrys]
```

Returns the pair with the largest key **less than** the query key (strict predecessor). If the tree has no
pair with a key less than the query key, returns the sentinel `{KEY_MIN, NOT_FOUND_VALUE}`. The host keeps each
partition starting at its smallest live key, so a correctly routed query never receives this sentinel (see the
comment on `BPForest::locate_pred_partition`).
The DPU returns a `KVPair` because the return value of `BPForest::batch_pred` is a key-value pair.

### TASK_RANGE_COUNT (`SUPPORT_RANGE_COUNT`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 RangeCountQuery[nr_cold_qrys + nr_hot_qrys]) -> uint64_t[nr_cold_qrys + nr_hot_qrys]
```

`RangeCountQuery = {KeyRange range; Value needle}`. Returns the number of pairs in the key range (both ends
inclusive) whose value equals `needle`.

### TASK_RANGE_MAX (`SUPPORT_RANGE_MAX`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 KeyRange[nr_cold_qrys + nr_hot_qrys]) -> Value[nr_cold_qrys + nr_hot_qrys]
```

Returns the maximum value in the key range (both ends inclusive). `NOT_FOUND_VALUE` if the range has no pairs.

### TASK_INSERT (`SUPPORT_INSERT`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 KVPair[nr_cold_qrys + nr_hot_qrys]) -> uint32_t[2]
```

Overwrites the value if the key exists; otherwise inserts it.

Only the DPU knows how many of the insert queries had new keys, so the host receives the return value
(live pair count of the cold tree, live pair count of the hot tree) and reflects it in `nr_pairs`.
This task has no per-query return values, so this comes at the start of `result_offset`
(only this task and TASK_DELETE return live pair counts; after other tasks the host can compute the correct
values on its own).

### TASK_DELETE (`SUPPORT_DELETE`)

```
(uint32_t nr_cold_qrys, uint32_t nr_hot_qrys, uint32_t result_offset,
 Key[nr_cold_qrys + nr_hot_qrys],
 uint32_t nr_cold_refreshes, uint32_t nr_hot_refreshes,
 KeyRange[nr_cold_refreshes + nr_hot_refreshes])
-> (uint8_t[nr_cold_qrys] (+pad), uint8_t[nr_hot_qrys] (+pad), uint32_t[2],
    KVPair[nr_cold_refreshes + nr_hot_refreshes])
```

Removes keys from the tree. For each query returns 0/1 for "whether the pair existed just before the deletion";
if the same key appears more than once in a batch, only the first one returns 1. The design is in
`docs/parallel_delete.md`.

*   The `KeyRange[]` argument (min-refresh requests) is placed right after the key sequence. Each request asks
    for "the smallest live key in the range (both ends inclusive)", and the host uses it to keep each
    partition's start key at its smallest live key.
    It is processed by a single tasklet after all tasklets have finished deleting.
*   Layout of return values (relative to `result_offset`, each section on an 8-byte boundary):
    1.  `uint8_t[nr_cold_qrys]`: 0/1 flags of the cold queries
    2.  `uint8_t[nr_hot_qrys]`: 0/1 flags of the hot queries
    3.  `uint32_t[2]`: (live pair count of the cold tree, live pair count of the hot tree)
    4.  `KVPair[]`: min-refresh responses. `{key, 1}` if found, `{0, 0}` if the range has no live
        key
*   Live pair counts are returned for the same reason as in TASK_INSERT (only the DPU knows how many pairs
    were actually removed). A single tasklet writes them after all tasklets have finished deleting, so they
    are finalized at the same time as the min-refresh responses.
*   So that writing 1-byte flags does not make tasklets share an 8-byte DMA word, queries are distributed with
    a quota rounded up to a multiple of 8 for every tasklet except the last.
*   The area after the min-refresh responses is a DPU-only work area (the address of the value slot for each
    query, a copy of the key sequence, and a sort stack per tasklet). The host does not read it, but the MRAM
    heap needs room for it.

### TASK_SERIALIZE

```
(uint32_t nr_delims, uint32_t max_nr_delims, bool do_cold, bool do_hot,
 Key[nr_delims]) -> (uint32_t[nr_delims], KVPair[])
```

Writes out the contents of the trees as a sequence of KV pairs. Used to move pairs between DPUs during
rebalancing.

*   The `Key[nr_delims]` argument lists the cut points of the cold region in ascending order. The host places
    them at the boundaries between consecutive cold partitions (= the start key of the next cold partition).
*   `do_cold` / `do_hot` select what to write out (three forms: cold+hot / cold only / hot only).
*   The return value `uint32_t[nr_delims]` (incisions) gives the index in the written cold pair array that
    each cut point corresponds to. `incisions[i]` = "the position of the first pair whose key is **at least**
    `delims[i]`" (= the number of pairs before the cut point). It is written at the start of the payload,
    **overwriting the delim area of the argument**.
*   The return value `KVPair[]` is contiguous as `[cold | hot]` and starts at the heap start +
    `sizeof(InputHeader) + sizeof(Key) * max_nr_delims`. Using `max_nr_delims`, which is common across DPUs,
    instead of `nr_delims` aligns the start of the pair array on all DPUs.
    When `do_cold=false`, there are 0 cold pairs, so hot starts at the beginning.

### TASK_MOVE_HOT

```
(uint32_t nr_cold_pairs, uint32_t nr_hot_pairs, bool renew_cold, bool renew_hot,
 KVPair[nr_cold_pairs + nr_hot_pairs]) -> ()
```

Loads the pairs extracted by TASK_SERIALIZE into the destination DPU. If `renew_*` is true, that tree is rebuilt
from the given pairs (the same construction as TASK_INIT); if false, the pairs are upserted into the existing
tree.

### TASK_NONE

```
() -> ()
```

Does nothing. Because DPUs are launched per rank, this is assigned to DPUs that have no work in the batch.

## Unimplemented tasks

TASK_RANGE_MIN (minimum value in a key range) and TASK_RANGE_SUM (sum) in `enum TaskID` are implemented only by
the CPU baseline (`cpudb/`); the DPU side and `host_app` do not support them.
TASK_RANGE_MIN once had a DPU implementation (`SUPPORT_RANGE_MIN`), but it was not updated for changes to
`InputHeader` and could no longer be enabled, so it was removed. TASK_SCAN / TASK_SUMMARIZE / TASK_EXTRACT /
TASK_FLATTEN_HOT / TASK_RESTORE, which existed only as IDs, were also removed, since they had neither an
implementation nor any use.

## Parallelizability with tasklets

The number of tasklets for each current task is set by `TASK_*_NR_TASKLETS` in `dpu/inc/dpu_params.h`.
By default, TASK_GET / TASK_PRED / TASK_INSERT / TASK_DELETE / TASK_RANGE_COUNT /
TASK_RANGE_MAX and tree construction (`TREE_CONSTRUCT_NR_TASKLETS`) run with `NR_TASKLETS` in parallel, and
TASK_SERIALIZE is fixed at 1 tasklet
(`_Static_assert(TASK_SERIALIZE_NR_TASKLETS == 1)`).

Tasks whose number of return values is not one per query are hard to parallelize,
because tasklets other than the first do not know at which offset of the return value array to start writing
their results.
Three remedies are conceivable:

1.  Do not parallelize.
2.  Write to some arbitrary place and compact afterwards.
3.  Write to some arbitrary place and tell the host the resulting order, so that the results are compacted
    when transferred from the DPU.

TASK_SERIALIZE has this problem and currently takes remedy 1 (a single tasklet).

TASK_INSERT does not have this problem (its return value is just one pair of live pair counts) and is
parallelized over all tasklets. The design is in `docs/parallel_batch_update.md`.
