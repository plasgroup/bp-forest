Rebalancing Algorithm
===

How BPForest's rebalancing (extracting hot ranges and relocating them to
smooth the query load) works, organized as **semantics → implementation**.

Code source of truth: `bpforest/inc/bpforest.ipp`
Theory: §"Query Density-Driven Partitioning" and the appendix of the paper
Related documents:

- `docs/pairs-range.md` — types and capabilities

---

## 1. Semantics

### 1.1 What rebalancing is responsible for

Each DPU holds one base partition. When a subrange with a high query load
(= a hot range) is found in it, rebalancing **extracts it and moves it to
another DPU**, evening out the query load across DPUs.

- Input: the queries of the current batch (keys / range queries) and the current partition of each DPU
- Output: the updated partition table `parts` and `hot_ranges`, and the `TASK_MOVE_HOT` instructions sent to the DPUs
- Property: one rebalancing creates at most $P$ (= the number of DPUs) new hot ranges (§3)

Call sites:

- `BPForest::batch_get` / `batch_pred` / `batch_insert` / `batch_delete` /
  `batch_range_count` / `batch_range_max` call the `repartition()` wrapper
  right after `route_queries`
- `repartition()` runs only when `param.enable_dynamic_repartition` (CLI
  `--dynamic-repartition`, on by default) is set. It first tries
  `incremental_repartition`, and escalates to `full_repartition` if that
  returns `Balanced::No`
- The initial construction paths (`partition_with_get_batch` etc.) call `full_repartition` directly

### 1.2 Two paths

| Path | Meaning | When it runs |
|---|---|---|
| `full_repartition` | Heavy path: pulls all KV pairs to the CPU and rebuilds the partitions | At initial construction, and when incremental returns `Balanced::No` |
| `incremental_repartition` | Light path: reuses the state of the previous batch and touches only DPUs with a skewed load | Steady-state batch processing. Returns `Balanced::No` if the result does not fit in `new_hots` (§3) |

### 1.3 Terms and symbols

The queries in one batch are either all point queries or all range queries; they are never mixed.
The load of a range query is **approximated as point queries to its two
endpoints, begin and end** (one range query counts as two, $W = 2$). The
formulas below assume this approximation.

Correspondence with the paper's symbols (`more_hotness = 1`, $W = 1$, no existing hot ranges):

| Paper | Code | Meaning |
|---|---|---|
| $P$ | `nr_base_parts` | Number of DPUs (= number of base partitions) |
| $Q$ (paper) / $Q \cdot W$ (code) | `nr_queries * W`, $W \in \{1, 2\}$ | Total query load |
| $D$ | total number of KV pairs | Bytes in the paper, pair count in the code |
| $Q/P$ / $\lceil Q/P \rceil$ | `hot_load` | Target load per hot range. The unit for the number of split pieces (§4) |
| $\alpha$ | `param.balancing` | balancing factor |
| $\dfrac{1}{\alpha}\dfrac{D}{P}$ | `hot_npairs` | Target number of pairs of one hot range (= the block width; §4) |
| — | `param.more_hotness` | Multiplier applied to `hot_load` and `cold_endpoint_cnt_goal` (CLI `--more-hot`, default 1) |

Representative formulas (at the top of `full_repartition_worker` /
`incremental_repartition_worker_cold`; identical in both):

```cpp
hot_load               = (more_hotness * nr_queries * W + nr_base_parts - 1) / nr_base_parts;
cold_endpoint_cnt_goal = more_hotness * nr_queries * W * (balancing + 1) * (balancing + 1) / balancing / 4 / nr_base_parts;  // cold_load_goal()

// full_repartition_worker: from the number of cold pairs of the target base
hot_npairs       = (cold_npairs + param.balancing - 1) / param.balancing;
// incremental_repartition_worker_cold: from the base pair count, cold plus existing hot
hot_npairs       = (base_npairs + param.balancing - 1) / param.balancing;
```

The coefficient $\dfrac{(\alpha+1)^2}{4\alpha}$ of `cold_endpoint_cnt_goal` is the
smallest coefficient for which, with the selection rule of §4, lowering the cold
load to the goal never carves out more than $P$ ranges (§3).

**Phase 1 and Phase 3 (§6) compare against different units** (incremental path):

- The `cold_cnt_goal` of Phase 1 (the pre-filter at the top of
  `incremental_repartition`) is compared with **the number of range query
  fragments cut at partition boundaries**
  (`routed.cold[d].nr_qrys`; no `W` factor)
- The `cold_endpoint_cnt_goal` of Phase 3 (`incremental_repartition_worker_cold`)
  is compared with **the number of begin/end endpoints of the original range
  queries that fall in a cold subrange** (`W=2`)

The two differ in both unit and coefficient, so they cannot be compared directly. For point queries,
both the number of fragments and the number of endpoints equal the number of queries, and the two coincide.

**Approximation in load estimation**: the actual number of chunks a range query
intersects is ignored; a chunk's `load()` is incremented only when the begin or
end endpoint falls in the target range. The full side (the load estimation block
of `full_repartition_worker`) checks against `[base_min, base_max]`; the
incremental side (`incremental_repartition_worker_cold`) counts only "endpoints
that fall in a remaining cold subrange", using the `cold_range_bounds` sequence
built from `cold_key_ranges`, and deduplicates by `orig_idx` when the same query
is routed to several fragments on the same DPU.

---

## 2. Data structures

See `docs/pairs-range.md` for details. Only their roles in rebalancing are listed here:

- `PairsRange` / `ChunkedPairsRange` — non-owning views representing a cold subrange
- `DataChunkIterator` — chunk iterator over units of `KVPairsChunkSize` pairs. The granularity of blocks (§4) and splits (§6.2)
- `BPForest::parts` — the partition table in key order. This is the output of rebalancing itself
- `NewHotRange = {PairsRange, KeyRange, load, origin}` — the slot for an extracted hot range. `origin` is the base partition it was carved from, needed to rebuild `parts`
- `LinkedList<ChunkedPairsRange>` — the set of cold ranges of each DPU. The splitting caused by hot extraction is handled by erase/insert (iterators stay stable)
- `BPForest::new_hots` — a **fixed-size buffer** `ExtendableBuffer<NewHotRange>{nr_base_parts + 1}` (§3). The +1 is for the extra piece that `carve_new_hots` merges away
- `BPForest::kept_hot` / `BPForest::hot_split_plans` — intermediate data of hot partition split (§6.2). Used to pass piece[0], which stays on the source DPU of the split, and pieces[1..], which are relocated

---

## 3. Invariants

**Proposition (full path):** when one run of `full_repartition` completes, the total number
`hot_count` of newly carved hot ranges does not exceed `nr_base_parts` (= $P$).

**Proof sketch** (`more_hotness = 1`, rounding ignored): let $L_b$ be the cold load of
base partition $b$, $x_b = L_b / (Q/P)$, $c = (\alpha+1)^2/(4\alpha)$, and $m$ the number of blocks.
There is one cold range, and every block except the last has at least `hot_npairs` $= \lceil n/\alpha \rceil$
pairs ($n$ is the number of cold pairs), so $m \le \alpha$. The $k$-th block in descending load order is taken
only when the remainder after taking $k - 1$ blocks exceeds the goal $c \cdot Q/P$, and that remainder is at
most $L_b (m-k+1)/m$, so $k - 1 < \alpha (1 - c/x_b)$. By the AM-GM inequality $x_b + \alpha c / x_b \ge \alpha + 1$,
$k < x_b$. The same holds when counting split pieces: a block of load $l$ yields
$\max(1, \lfloor l / \text{hot\_load} \rfloor)$ pieces, which is at most $l/(Q/P)$ if $l \ge Q/P$.
Blocks with $l < Q/P$ come last in load order, so the same argument can be repeated on them alone,
with $L_b$ minus the load of the former blocks. Hence base partition $b$ yields at most $x_b$.
Since $\sum_b L_b \le Q$, the total is at most $P$.

**It does not hold on the incremental path:** when existing hot ranges fragment the cold
data, there are several cold ranges, and the number of blocks grows up to
$\alpha + (\text{the number of existing hot ranges originating from that base})$.
There is also no guarantee that the total including existing hot ranges is at most $P$.

**How the code enforces it:**

- `new_hots` is fixed-size. The hooks of both workers pass `carve_new_hots` the remaining capacity
  (`P − (number of existing hot ranges) − hot_count` for incremental, `P − hot_count` for full),
  and if the pieces of the next block do not fit, `carve_new_hots` writes nothing and returns 0
- On receiving 0, the incremental hook sets `overflow` and stops selecting (that block has already been
  removed from the cold list, but the whole list is discarded because the path falls back to full).
  `incremental_repartition` returns `Balanced::No` if `overflow` is set right after the cold pass, and,
  right after the hot pass, also counting the new pieces from splits, if
  `nr_existing_hots + hot_count + nr_new_pieces > nr_base_parts`. The pieces[1..] of a hot split
  are written to `new_hots` only after passing the second check
- By the proposition above, the full hook never receives 0 (`assert`)

---

## 4. `find_relatively_hot_ranges` and placing new hot ranges

### Semantics

Divides the set of cold ranges of one base partition into blocks of `hot_npairs` pairs,
and turns blocks into hot ranges in descending order of load.

- **Blocks**: each cold range is divided from its start. A block is `hot_npairs` pairs
  rounded up to whole chunks, and the leftover at the end of a range becomes one block as is.
  A block never spans two cold ranges
- **Selection**: blocks are passed to `hot_hook` in descending order of load, stopping when the hook
  returns `false`. The hooks of both workers stop when "the cold load has dropped to
  `cold_endpoint_cnt_goal` or below" or when "the pieces do not fit in the remaining capacity of `new_hots`"
  (§3)
- **Remaining cold data**: the passed blocks are removed from the list. The rest of a range stays in
  its original node; when the removal breaks a range apart, the pieces go into the node returned by
  `new_cold_hook`. The list does not change while the hook is running

Callback convention: `bool hot_hook(range, begin_chunk, end_chunk, load)`,
`LinkedChunkedPairsRange& new_cold_hook()`.

The selected hot ranges are not necessarily adjacent. Blocks are scanned with `DataChunkIterator`. It
holds `part_begin/part_end/cursor/p_load` by value and does not reference the range's node live,
so rewriting nodes while rebuilding the remaining cold data does not break the recorded block boundaries.

Tested by `bpforest/test/hot_range_finding.cpp` (`hot_range_finding_test_<target>`). For randomly
generated sets of cold ranges, it checks the blocks passed, their order, and the remaining cold ranges
against a reference implementation that directly transcribes the definitions of this section.

### Placing new hot ranges (`carve_new_hots`)

The `hot_hook` of both workers passes the selected block to `carve_new_hots`. It turns
the block into one or more hot partitions (`NewHotRange`), writes them to `new_hots`,
and returns their count. When `param.enable_hot_split` is set and the block's load exceeds the
hot split goal (`hot_load_goal()`; the `hot_cnt_goal` of §6 Phase 1 computed in units of endpoints),
it divides the block into `load / hot_load` pieces with `split_hot_range_equal_load` (§6.2). If the
number of pieces exceeds the remaining capacity of `new_hots` (§3), it writes nothing and returns 0.

### Hot ranges still too heavy after splitting (`relieve_overloaded_hots`)

After the new hot ranges are assigned to DPUs and the queries are re-routed, for each hot range placed
by this rebalancing (those carved from cold data, pieces[1..] of hot splits, and piece[0] kept on
the split source) whose routed query count `routed.hot[dpu].nr_qrys` exceeds `hot_cnt_goal`,
the following are tried in order (when `param.enable_hot_split` is set; the processing of one hot
range is `relieve_overloaded_hot`). This is the same condition under which, if the hot range is
larger than one chunk, it becomes a hot split target (`do_hot`; §6 Phase 1) in the next batch.

1. **Adoption into the host-side cache** ("Adoption" in docs/host_hot_cache.md): subtract the
   estimated count of the adopted keys from the query count
2. **`hot_split_failed`**: if it still exceeds `hot_cnt_goal`, set `hot_split_failed` of that DPU

If at least one pair was put into the cache, the queries are routed once more.

---

## 5. `full_repartition`

### Semantics

A **bulk rebuild** path: pulls all KV pairs to the CPU, extracts hot ranges (§4) from
base partitions that divide the data equally among the DPUs, assigns them to lightly loaded
DPUs with `partial_sort`, and then rebuilds the trees on all DPUs.
The skeleton of Algorithm 1 "Construction of hot/cold partitions" in the paper.

### Implementation (flow)

The workers of `full_repartition_worker` synchronize with each other through the mutex and
shared counters (`idx_base` / `cold_count` / `hot_count`) of `TmpDataForFullRepartition`.

`full_repartition` itself:

1. Discard the `hot_cache` of all DPUs ("Eviction" in docs/host_hot_cache.md) and drop `kept_hot`
2. Pull the KV pairs from all DPUs with `retrieve_all_data(data_buf)`
3. Divide `data_buf` equally into `nr_base_parts`, put each base partition into
   `chunked_cold_ranges[]`, and rebuild `parts` from the base partitions only
4. `rebuild_part_indices()` → `route_queries()` to re-route to the new partitions
5. **find_hot stage**: `parallel_run(&full_repartition_worker)` (below)
6. Only when `hot_count > 0`, assign `new_hots[0..hot_count)` to lightly loaded
   DPUs:
   - `partial_sort` the first `hot_count` of `cold_loads` in ascending load order
   - sort `new_hots` in descending load order
   - pair the least loaded DPUs with the most loaded hot ranges, and record this in `hot_entries[]`
   - `rebuild_parts()` → `route_queries()` to re-route to the updated partitions
   - `relieve_overloaded_hots` (§4)
7. Rebuild the DPU-side trees with `initialize_in_dpu(...)`

`full_repartition_worker` (each thread takes one base at a time with
`get_next_idx_base`):

- **Base acquisition** (`get_next_idx_base`, under the mutex): for point queries,
  bases with `routed.cold[idx].nr_qrys <= cold_endpoint_cnt_goal` are skipped
  here, recording only `nr_pairs` / `cold_loads`. For range queries, the
  fragment count and the endpoint count are different units (§1.3), so they are
  not skipped here; the decision is made after counting endpoints
- Fix `cold_npairs` and `hot_npairs`
- **Load estimation** (outside the mutex, in parallel): the approximation of §1.3. The chunk of a
  point query is looked up with `upper_bound` (`lower_bound` for the predecessor family)
- **Hot selection** (under the mutex; only when `cold_endpoint_cnt > cold_endpoint_cnt_goal`):
  `find_relatively_hot_ranges` (§4). `carve_new_hots` puts the selected blocks
  into `new_hots`
- Update `nr_pairs[idx_base]` and `cold_loads[idx_base]`

---

## 6. `incremental_repartition`

### Semantics

A **lightweight path that keeps the state of the previous batch and reassigns hot
ranges only when a load skew beyond a statistical threshold is observed**. It returns a
`Balanced`: `Balanced::No` if the result does not fit in `new_hots` (§3), or if
a trigger fires while `param.enable_incremental == false`.

### Implementation (flow)

1. **Phase 1: trigger decision and selection of serialize targets**
   - There are two goals:
     - `cold_cnt_goal` — the same coefficient as `cold_endpoint_cnt_goal` of §1.3, but
       for the count of routed fragments (no `W` factor)
     - `hot_cnt_goal = hot_load_goal(nr_queries)` = `ceil(more_hotness * 2 * nr_queries / P)` —
       the lower bound for hot split. The coefficient is fixed at 2 regardless of the query type
   - The threshold is derived by
     `overload_threshold.threshold_for(nr_queries, goal, family)`
     (`util/inc/overload_threshold.hpp`). `OverloadThresholdSpec` is either
     `HighWatermarkRatio{r}` (threshold = goal × r; CLI `--high-watermark`) or
     `FalsePositiveRate` (Bernstein threshold, CLI `--fp-rate`). The default of the
     library's `BPForest::Param` is `HighWatermarkRatio` with r=1.05; the command-line
     drivers (`host_app`, `resp_server`) default to `--fp-rate 0.001`. Only the latter
     receives a Bonferroni correction with
     `family = nr_base_parts + number of existing hot ranges`
   - **Separation of trigger and target selection**: exceeding the threshold (`trigger_cold` /
     `trigger_hot`) decides "whether to start rebalancing in this batch", and
     rebalancing starts if it holds for even one DPU. The targets actually serialized are
     selected by exceeding the goal (`do_cold` / `do_hot`). Since this condition is looser than
     the trigger, once rebalancing starts, DPUs that merely exceed the goal are processed together
   - Conditions on the hot side: `trigger_hot` = `param.enable_hot_split` ∧ the hot range is larger
     than one chunk ∧ `!hot_split_failed[dpu]` ∧ `nr_qrys > hot_cnt_threshold`.
     `do_hot` is the same, with the threshold replaced by the goal and without checking `hot_split_failed`.
     That is, a hot range with `hot_split_failed` (§4, §6.2) set does not start rebalancing by itself,
     but is re-examined in a batch in which another DPU starts it
   - A `do_cold` DPU scans its own cold partitions with `cut_points_of_cold_tree()`,
     records their boundaries in `incision_keys[]` and the key range of each partition in
     `cold_key_ranges[]`, and sets `TASK_SERIALIZE`.
     `do_cold` / `do_hot` are recorded in the header, and subsequent host-side decisions look only
     at these two flags (`task_no` is the instruction to the DPU)
   - If a trigger fires and `param.enable_incremental == false`, return
     `Balanced::No`
   - After scanning all DPUs, if no trigger fired, return early with `Balanced::Yes`
2. **Phase 2: serialize execution / retrieval / cold range reconstruction**
   - gather + execute `TASK_SERIALIZE` on each target DPU
   - `incision_indices[]` is retrieved at the same time
   - `do_cold` DPUs: repack the KV pairs into `chunked_cold_ranges[]` and
     relink them as `chunked_cold_ranges_lists[idx_dpu]`
   - `do_hot` DPUs: retrieve the hot pair sequence into `hot_ranges[idx_dpu]`
3. **Phase 3: two parallel passes**
   - **cold pass**: `parallel_run(&incremental_repartition_worker_cold)`
     (§6.1). Check #1 of §3 right after it
   - **hot pass**: `parallel_run(&incremental_repartition_worker_hot)`
     (§6.2). Check #2 of §3 right after it. The hot pass does not touch `new_hots`
4. **Finalize hot split plans**: for DPUs whose `hot_split_plans[idx_dpu]` is non-empty,
   reserve piece[0] to stay in `kept_hot[idx_dpu]` and push pieces[1..] to
   `new_hots[hot_count++]`
5. If `hot_count == 0`, return `Balanced::Yes`
6. Assign `new_hots` to lightly loaded DPUs. DPUs that already have a hot range (including DPUs
   that split and keep piece[0]) are excluded from the candidates before `partial_sort`. After assignment, kept piece[0] is
   reinserted into its original DPU. That DPU already holds the leftmost data, so the hot tree
   can be rebuilt without data movement
7. Collect the unchanged hot ranges, the kept piece[0]s, and the newly assigned hot ranges in
   `hot_entries[]`, sort them in key order, and rebuild `parts` with `rebuild_parts()`. Set each DPU's
   `input_header` for `TASK_MOVE_HOT`
8. Send the updated partitions to the DPUs and execute
9. Re-route with `route_queries()`, and after `relieve_overloaded_hots` (§4), return `Balanced::Yes`

### 6.1 cold pass (`incremental_repartition_worker_cold`)

Targets are the `do_cold` DPUs. For the others, it only sets
`cold_npairs_list = 0` and records the current load in `cold_loads`.
`get_next_idx_dpu` does not take a new DPU once `overflow` (§3) is set. For each target
DPU:

- **Load estimation** (outside the mutex, in parallel): see "Approximation in load
  estimation" at the end of §1.3. Attribution to remaining cold subranges and `orig_idx` dedup
- `hot_npairs` follows the formula of §1.3 (`base_npairs` = current number of cold pairs + the number
  of pairs of the existing hot ranges originating from that base)
- If `cold_endpoint_cnt <= cold_endpoint_cnt_goal`, clear `do_cold` and leave the cold data
  as is (the only direction in which the endpoint-unit decision overrides the fragment-unit decision of Phase 1; §1.3).
  This does not happen for point queries, where the two units coincide. If the DPU is also `do_hot`, it
  remains a target of the hot pass (§6.2) (test: `bpforest/test/hot_pass_targets.cpp`)
- **Hot selection** (under the mutex): same flow as the full side (§5)
- Update `cold_npairs_list[idx_dpu]` and `cold_loads[idx_dpu]`

### 6.2 hot pass — the hot partition split mechanism (`incremental_repartition_worker_hot`)

A mechanism, enabled by `param.enable_hot_split` (on by default), that **splits an overheated existing
hot partition into several pieces and redistributes them**. Targets are the `do_hot` DPUs.

- Load estimation on the retrieved hot pair sequence (`hot_ranges[idx_dpu]`).
  It is a single contiguous range, so the dedup of the cold pass is unnecessary. For range queries,
  only endpoints within `[hot_begin_key, hot_max_key]` are counted
- The number of pieces is `nr_target_pieces = measured / hot_load`. If it is less than 2, no split
  is done: `hot_ranges[idx_dpu]` is set back to null and the hot range is kept (the DPU-side
  hot tree is left as is)
- `split_hot_range_equal_load` (`bpforest/inc/split_hot_range.hpp`) cuts at
  chunk granularity into pieces of nearly equal load (`ceil(measured / nr_target_pieces)`
  each). The number emitted is one more than the target if load-free chunks remain after the last cut,
  and fewer than the target if there is a single huge chunk that cannot be subdivided. If it is 1,
  the split is abandoned and the hot range is kept, "Adoption" in docs/host_hot_cache.md is tried, and if
  nothing is adopted, `hot_split_failed` is set
- The pieces are recorded in `hot_split_plans[idx_dpu]`, and `emit_count - 1` is added to
  `nr_new_pieces`

### Execution order within a batch (batch_get etc.)

```
BPForest::batch_get(nr_queries, keys, results)
  ├─ route_queries(...)
  ├─ repartition(...)                          // when param.enable_dynamic_repartition
  │    ├─ incremental_repartition(...)
  │    │    ├─ Phase 1: trigger decision + serialize target selection (Balanced::Yes if no trigger)
  │    │    ├─ Phase 2: pull KV to the CPU with TASK_SERIALIZE
  │    │    ├─ Phase 3: cold pass → hot pass
  │    │    │           └─ Balanced::No if it does not fit in new_hots
  │    │    ├─ assign new_hots to lightly loaded DPUs (partial_sort), reinsert kept piece[0]
  │    │    ├─ send TASK_MOVE_HOT → build trees on the DPU side
  │    │    ├─ re-route with route_queries()
  │    │    └─ relieve_overloaded_hots() (route_queries() once more if anything was adopted)
  │    └─ full_repartition(...) if Balanced::No
  ├─ execute_in_dpus()                         // query execution
  └─ postprocess_of_get()                      // result collection
```

The other batch operations (§1.1) follow the same flow (only the routed data and the postprocessing differ).

---

## 7. Rough costs (body.tex §"Expense of Full Rebalancing")

- Most of full rebalancing is KV pair movement (~3/4). The partitioning computation itself is <0.8%
- One full rebalancing ≈ 100 batched range queries
- Incremental is far cheaper than full, but when it bails out to full it
  incurs that cost

---

## 8. Safety checkpoints for reimplementation

When reimplementing rebalancing outside bpforest.ipp, preserve the invariants of §3 and the
remaining-capacity checks, and the snapshot design of §4 (`DataChunkIterator` does not reference
the range's node live).
