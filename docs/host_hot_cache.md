# Host-side cache

A mechanism that copies a KV pair to the host and serves most point queries on that key at the host (all gets;
all inserts, deletes, and preds except one per routing thread), used when the load of a hot partition is
concentrated on that one pair and the hot split fails, and when a hot partition just placed exceeds the goal.
It is an experimental implementation, switched by `BPForestParameter::enable_hot_cache` (`--hot-cache` in
host_app, on by default).

## Semantics

* The cache is used by get, insert, delete, and pred batches. Range operations (range_count,
  range_max, scan) do not use it and go to the DPUs as usual.
* At the end of each batch, the trees on the DPUs hold the same pairs as the cache. Hence range queries
  never see stale values.
* Each DPU holds at most 1 pair, and the key of that pair lies in the range of that DPU's hot partition
  (maintained by "Eviction"). Hence the size of the cache is at most the number of hot partitions, i.e., at most the number of DPUs.
* A query hits the cache only when its routing destination is the hot partition of that DPU and
  its key equals the key cached for that DPU.

## Adoption

A batch proceeds through routing, repartitioning, and execution on the DPUs, in that order; when repartitioning
changes the partition table, routing is redone before execution. Putting a pair into the cache (adoption)
happens during repartitioning.

The search looks for a key that at least half of the queries assigned to a hot partition (excluding those served
by the cache) go to, and there are two occasions for the search: when the hot split of incremental repartitioning
(`incremental_repartition_worker_hot`) fails, and when a hot partition placed by repartitioning
exceeds the hot split goal (`hot_cnt_goal`) (see "Adoption at a placed hot partition" below).
The former is described first. A hot split (docs/rebalancing-algorithm.md §6.2) fails when
`split_hot_range_equal_load` cannot make even one cut, i.e., when the last chunk carries
more than half of the load (not splitting because there are fewer than 2 pieces is not a failure).
If the load is concentrated on a chunk in the middle, the cut is made right after that chunk, and the first
piece, which ends with that chunk, stays on that DPU. Right after it is placed, this piece becomes subject to
"Adoption at a placed hot partition" (below). A hot partition of 1 chunk (at most `KVPairsChunkSize`
pairs) is never tried for a split, so it is subject to this mechanism only right after it is placed.

The search happens only when the hot split fails in a get, insert, or pred batch. It does not happen in a delete batch,
because the pair disappears after the batch.

`find_hot_cache_candidate` searches with the randomized method of [hot_key_finding.md](hot_key_finding.md):
it draws 108 samples with replacement from the assigned queries, picks at most 2 promising keys, runs each of
them through a truncated sequential probability ratio test with at most 1337 samples drawn with replacement, and
considers only the first key that passes (it does not proceed to the second even if the first key has no pair).
With failure probability δ = 0.01 and width w = 0.1, if a key accounts for at least half, it is found with
probability at least 1 − δ, and a key that is found accounts for more than 1/2 − w = 0.4 with probability at least 1 − δ. In addition,
the fraction of samples seen in the test that matched the key, multiplied by the number of assigned queries,
is taken as the estimated count for the key.

In get and pred batches, the pair to cache is taken from the hot tree. For the hot split, the
hot tree has been serialized to the host (`data_buf`), so no extra DPU round trip is needed.
If the found key is not in the hot tree, there is no pair to cache, so it is not adopted (`NOT_FOUND_VALUE`
marks an empty slot, so "does not exist" cannot be cached). In insert batches, without looking at the tree,
the pair of the last query in the batch that inserts that key is cached. The inserts of that batch create the
same pair on the DPU as well.

When that DPU's slot already holds a pair, the number of queries that pair served in this batch's most recent
routing is compared with the estimated count of the found key, and the pair is replaced only when the latter is
larger. The load seen by the search excludes what the cache served, so replacing without comparing would bring
the evicted key's load back in the next batch, and re-adoption would repeat every batch.

When a hot split fails and does not lead to adoption (a delete batch, no key accounting for at least half,
no pair for that key, or the existing pair is larger), `hot_split_failed` is set.
While it is set, that hot partition does not trigger repartitioning (in batches triggered by other DPUs,
it is rechecked if it exceeds the goal). It is cleared when the hot partition is re-placed (newly created,
full rebuild, or a piece placed by a hot split). Hence, for example, a concentration of gets on a deleted key
sets it, and even if that key is reinserted, the partition does not trigger repartitioning on its own until
the hot partition is re-placed.

The queries of the batch in which adoption happens were routed before the adoption, so the cache takes effect
from the next batch (only when the same repartitioning newly creates or splits another hot partition and
routing is redone does it take effect from that batch).

### Adoption at a placed hot partition

After hot partitions placed by repartitioning (those cut out of cold, the second and later pieces of a hot split,
and the first piece left at the split source) are placed on the DPUs and the queries are rerouted, the same
search is done for those whose number of assigned queries exceeds `hot_cnt_goal` (those that, if larger than
1 chunk, would be subject to a hot split in the next batch) (`relieve_overloaded_hots` in
docs/rebalancing-algorithm.md). The comparison with an existing pair is the same as for adoption at a hot split.
The differences are as follows.

* `hot_split_failed` is set when the query count minus the estimated count of the adopted key still exceeds
  `hot_cnt_goal`. When nothing is adopted (including range query and delete batches), the count is compared
  without subtracting.
* If even one key is adopted, routing is redone once more, so the cache takes effect from that batch.

## Handling in each batch

Queries that hit the cache in routing (`route_single_point_query`) are handled by kind as follows, and of those,
at most 1 per routing thread is sent to the DPU.
A pred returns the pair with the largest key less than key, so it cannot be answered from the cached pair.
Only the fact that preds on the same key have the same answer is used.

| Query | At the host | Sent to the DPU |
|:-|:-|:-|
| get | Writes the value on the spot | Nothing |
| insert | Sets the cached value to that of the last one in batch order | The last one of each thread |
| delete | `existed = 0` for those not sent | The first one of each thread. `existed` is received from the DPU |
| pred | Copies the result of the one sent | The first one of each thread |

One per thread suffices because of the DPU-side rules: if a batch has several inserts of the same key, the value
of the last one remains (docs/parallel_batch_update.md), and if it has several deletes of the same key, only the
first one returns `existed = 1` (docs/parallel_delete.md). Both are determined by the order in which the
host sends them. Each thread handles a contiguous range in batch order, and the query sequence sent to the DPU
is arranged in thread order, so the order the DPU sees is batch order, and the last (insert) or first (delete)
one in the whole batch takes effect.

## Eviction

A pair is discarded on the following 3 occasions.

* When the key is removed by a delete batch (after the batch).
* When another pair is adopted on the same DPU.
* When the key is not in the first piece of a hot split, and on a full rebuild. Once the key leaves the DPU's
  hot partition, writes to that key no longer go through the cache, so the pair is discarded to avoid
  leaving a stale value.

## Logs (part-log)

* `nosplit hot <dpu> reason new_hot load <m>`: `hot_split_failed` was set on a placed hot partition.
  `m` is the number of assigned queries minus the estimated count of the adopted key.
* `cache hot <dpu> key <k> nqrys <n> load <m>`: adoption. `n` is the estimated count, and `m` is the load of
  that hot partition (excluding what the cache served).
* `cache keep <dpu> key <k> served <h> over <k'> nqrys <n>`: the existing pair `k` (which served `h` queries in
  this batch, counting those sent to the DPU) was kept in preference to the found key `k'` (estimated `n`
  queries).
* `cache evict <dpu> key <k> reason replaced|deleted|moved`: eviction. `moved` means the key left the DPU's hot
  partition through a hot split or a full rebuild.
* `cache hits <n>`: the number of queries that hit the cache in that batch (counting those sent to the DPU).
  Not printed when 0.

## Implementation

* The state is `hot_cache`, whose elements (slots), indexed by DPU number, hold that DPU's pair. An empty
  slot has the value `NOT_FOUND_VALUE` (user values never take it). Eviction just resets the value to it
  (`drop_hot_cache_pair`). Pairs are discarded where the first piece is placed in a hot split, and at the
  beginning of a full rebuild.
* Per-thread routing record `HotCacheHits`: for each slot, the number of queries served, the in-batch position
  of the query sent to the tree, and the positions of preds that hit the same slot as the second or later one
  in the same thread. Cleared by `route_clear_impl`.
* Deletes and preds are sent with `route_point_query_to_tree` (routing that ignores the cache) at the moment
  a thread first hits. For inserts, each thread sends the last one after finishing its range, and
  `route_queries` overwrites the cached value in thread order so that it becomes the last one in
  batch order.
* The copying for pred is at the end of `postprocess_of_pred_impl`, and the eviction for delete is
  `evict_deleted_hot_cache_pairs` (at the end of `batch_delete`).
* The test is `bpforest/test/hot_cache.cpp` (`hot_cache_test_<target>`). It checks, with the cache both on and
  off, that a concentration of gets on one key puts the pair into the cache, and that subsequent get, insert,
  pred, range_count, delete, reinsertion, and reads after a full rebuild match the reference implementation.
