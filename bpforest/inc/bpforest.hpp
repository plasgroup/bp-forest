#pragma once

#include "cacheline_aligned.hpp"
#include "common.h"
#include "const_cap_vector.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "input_header.h"
#include "overload_threshold.hpp"
#include "pairs_range.hpp"
#include "parallel.hpp"
#include "partition.hpp"
#include "workload_types.h"

#include <any>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <ostream>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>


struct ScanRange {
    uint16_t outer_begin, outer_end;
    uint32_t inner_begin, inner_end;
};
struct BatchScanResult {
    ExtendableBuffer<value_uint64_t> values;
    ConstantCapacityVector<size_t, MAX_NR_DPUS * 2> outer_offset;
    ExtendableBuffer<ScanRange> ranges;

    std::pair<const value_uint64_t*, const value_uint64_t*> get_nth_result(size_t n) const
    {
        return std::make_pair(
            &values[outer_offset[ranges[n].outer_begin] + ranges[n].inner_begin],
            &values[outer_offset[ranges[n].outer_end] + ranges[n].inner_end]);
    }
};

template <typename Query, typename Result>
struct QueryDataPerRange {
    // qrys[idx_host_thread][idx_qry]
    std::vector<std::vector<Query>> qrys;
    // orig_idxs[idx_host_thread][idx_qry]
    std::vector<std::vector<uint32_t>> orig_idxs;
    // results[idx_host_thread][idx_qry]
    std::vector<ExtendableBuffer<Result>> results;
    uint32_t nr_qrys;

    explicit QueryDataPerRange(unsigned nr_threads) : qrys(nr_threads), orig_idxs(nr_threads), results(nr_threads) {}
    void clear();
    void clear_for_thread(unsigned tid);
};
template <typename QandR>
struct QueryDataPerRange<QandR, QandR> {
    std::vector<std::vector<QandR>> qrys;
    std::vector<std::vector<uint32_t>> orig_idxs;
    uint32_t nr_qrys;

    explicit QueryDataPerRange(unsigned nr_threads) : qrys(nr_threads), orig_idxs(nr_threads) {}
    void clear();
    void clear_for_thread(unsigned tid);
};
template <typename Query>
struct QueryDataPerRange<Query, void> {
    std::vector<std::vector<Query>> qrys;
    uint32_t nr_qrys;

    explicit QueryDataPerRange(unsigned nr_threads) : qrys(nr_threads) {}
    void clear();
    void clear_for_thread(unsigned tid);
};
//! delete: routing reports which of this tree's partitions have their
//! smallest live key deleted, besides where each query goes.
template <>
struct QueryDataPerRange<key_uint64_t, uint8_t> {
    std::vector<std::vector<key_uint64_t>> qrys;
    std::vector<std::vector<uint32_t>> orig_idxs;
    std::vector<ExtendableBuffer<uint8_t>> results;
    uint32_t nr_qrys;
    //! Those partitions, per host thread, named once per query that deleted a
    //! begin.  Such a delete moves the begin, so the DPU is asked for the new
    //! smallest key in the same launch.
    std::vector<std::vector<uint32_t>> min_hits;
    //! Landing area for the bytes that pad the 1-byte flags up to the 8-byte
    //! granularity the result layout keeps.  One per tree, and the trees are
    //! far enough apart in memory not to share a cache line.
    std::array<uint8_t, 8> result_pad;

    explicit QueryDataPerRange(unsigned nr_threads)
        : qrys(nr_threads), orig_idxs(nr_threads), results(nr_threads), min_hits(nr_threads) {}
    void clear();
    void clear_for_thread(unsigned tid);
};
template <typename Query, typename Result>
struct QueryData {
    std::vector<QueryDataPerRange<Query, Result>> cold, hot;

    explicit QueryData(dpu_id_t nr_dpus, unsigned nr_threads);
    void clear();
    void clear_for_thread(unsigned tid);
};

//! @brief An upper bound on the number of partitions: every base partition
//! contributes one, and each of the at most `nr_base_parts` hot partitions
//! adds itself and the cold partition resuming after it.
constexpr size_t max_nr_parts(dpu_id_t nr_base_parts) { return size_t{nr_base_parts} * 3; }

struct BPForestParameter {
    unsigned balancing = 1;
    unsigned more_hotness = 1;
    bool greedy_only = false;
    bool enable_dynamic_repartition = true;
    bool enable_incremental = true;
    bool enable_hot_split = true;
    // BPForest resolves this spec to a concrete policy at construction using ndpus.
    OverloadThresholdSpec overload_threshold_spec = HighWatermarkRatio{1.05};
    unsigned nr_host_threads = 0;
};
struct BPForest : ParallelManager<BPForest> {
    using Param = BPForestParameter;

    BPForest(const KVPair sorted_pairs[], size_t nr_pairs, const Param& = {});
    BPForest(const KVPair sorted_pairs[], size_t nr_pairs, const std::vector<Partition>& partitioning, const Param& = {});
    ~BPForest();

    void batch_get(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    //! result[i].value == NOT_FOUND_VALUE means keys[i] has no live (i.e. not
    //! deleted) strict predecessor; otherwise result[i] is the live pair with
    //! the largest key < keys[i].
    void batch_pred(uint32_t nr_queries, const key_uint64_t keys[], KVPair result[]);
    void batch_insert(uint32_t nr_queries, const KVPair pairs[]);
    //! existed[i] != 0 iff keys[i] was a live (i.e. not deleted) pair just
    //! before its deletion; duplicates within a batch as Database::batch_delete.
    void batch_delete(uint32_t nr_queries, const key_uint64_t keys[], uint8_t existed[]);
    void batch_range_count(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[]);
    void batch_range_max(uint32_t nr_queries, const KeyRange queries[], value_uint64_t result[]);
    void batch_scan(size_t nr_queries, const KeyRange ranges[], BatchScanResult& result);
    std::vector<std::array<uint32_t, 2>> get_nr_pairs() const;
    std::vector<std::array<uint32_t, 2>> last_query_dist() const;

    void partition_with_get_batch(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    void partition_with_pred_batch(uint32_t nr_queries, const key_uint64_t keys[], KVPair result[]);
    void partition_with_insert_batch(uint32_t nr_queries, const KVPair pairs[]);
    void partition_with_delete_batch(uint32_t nr_queries, const key_uint64_t keys[]);
    void partition_with_range_count_batch(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[]);
    void partition_with_range_max_batch(uint32_t nr_queries, const KeyRange queries[], value_uint64_t result[]);

    void print_params(std::ostream&) const;
    std::vector<Partition> dump_partitions() const;

private:
    BPForest(const Param& = {});

    const dpu_id_t nr_base_parts;

    //! The tree a partition's queries go to.
    struct QueryDest {
        dpu_id_t dpu;
        bool is_hot;

        friend bool operator==(const QueryDest& lhs, const QueryDest& rhs) { return lhs.dpu == rhs.dpu && lhs.is_hot == rhs.is_hot; }
    };
    //! @brief How the key space is divided among the trees the DPUs hold.
    //!
    //! Entry `i` gives tree `dests[i]` every key in `[begins[i], begins[i + 1])`,
    //! the last entry reaching up to `KEY_MAX`.  `origins[i]` names the base
    //! partition out of whose key range the entry was carved, which for a cold
    //! entry is `dests[i].dpu` itself.
    //!
    //! `begins` is strictly increasing and no two adjacent entries share a
    //! destination, which is what keeps the table canonical; `origins` is
    //! therefore non-decreasing, base partitions being numbered in key order.
    //! That `begins[i]` is partition `i`'s smallest live key is a further
    //! invariant, maintained by delete and rebalancing rather than by this
    //! type.
    struct PartitionTable {
        std::vector<key_uint64_t> begins;
        std::vector<QueryDest> dests;
        std::vector<dpu_id_t> origins;

        explicit PartitionTable(size_t capacity)
        {
            begins.reserve(capacity);
            dests.reserve(capacity);
            origins.reserve(capacity);
        }

        size_t size() const { return begins.size(); }
        void clear() { resize(0); }
        void resize(size_t n)
        {
            begins.resize(n);
            dests.resize(n);
            origins.resize(n);
        }
        //! The largest key the partition holds.
        key_uint64_t end_of(size_t i) const { return i + 1 < size() ? begins[i + 1] - 1 : KEY_MAX; }

        //! @brief Gives `dest` every key from `begin` on, dropping whatever
        //! that leaves holding none.
        //!
        //! Callers build the table from the smallest key up, so this also
        //! closes the partition before it.
        void assign_from(key_uint64_t begin, QueryDest dest, dpu_id_t origin)
        {
            while (!begins.empty() && begins.back() == begin) {
                begins.pop_back();
                dests.pop_back();
                origins.pop_back();
            }
            if (!begins.empty() && dests.back() == dest) {
                return;
            }
            begins.push_back(begin);
            dests.push_back(dest);
            origins.push_back(origin);
        }
        void assign_from(key_uint64_t begin, const PartitionTable& src, size_t i) { assign_from(begin, src.dests[i], src.origins[i]); }

        void swap(PartitionTable& other)
        {
            begins.swap(other.begins);
            dests.swap(other.dests);
            origins.swap(other.origins);
        }
    };
    PartitionTable parts{max_nr_parts(nr_base_parts)},
        //! Scratch for the rewrites that cannot be done in place.
        rebuilt_parts{max_nr_parts(nr_base_parts)};

    //! @name Where each DPU's partitions sit in `parts`
    //! Derived by rebuild_part_indices().  Base partition `d` owns the entries
    //! `[base_part[d], base_part[d + 1])`, an empty range when it holds
    //! nothing; `hot_part[d]` is `INVALID_DPU_ID` when DPU `d` hosts no hot
    //! partition.
    //! @{
    const ExtendableBuffer<dpu_id_t> base_part{nr_base_parts + 1};
    const ExtendableBuffer<dpu_id_t> hot_part{nr_base_parts};
    dpu_id_t nr_hot_parts = 0;
    //! @}

    //! A hot partition rebuild_parts() is to install.
    struct HotEntry {
        key_uint64_t begin;
        dpu_id_t host;   //!< the DPU whose hot tree holds it
        dpu_id_t origin;
    };
    //! Its input, sorted by key; scratch for the cold begins it merges them with.
    const ExtendableBuffer<HotEntry> hot_entries{nr_base_parts};
    const ExtendableBuffer<key_uint64_t> cold_begins{nr_base_parts + 1};

    //! Live (i.e. not deleted) pairs of each DPU's (cold, hot) trees, always
    //! equal to the DPUs' own counters.  Construction and rebalancing set it
    //! from the pairs they stage; insert and delete adopt what the DPUs
    //! publish, since only the DPU knows how many of the keys it was handed
    //! were new, or were still there to remove.
    const ExtendableBuffer<CachelineAligned<std::array<uint32_t, 2>>> nr_pairs{nr_base_parts};

    const Param param;

    const OverloadThreshold overload_threshold{param.overload_threshold_spec};
    ExtendableBuffer<bool> hot_stage1_fired{nr_base_parts};
    // The split piece kept on the original host.  A separate buffer rather
    // than InputHeader fields: InputHeader's union is clobbered by the
    // TASK_MOVE_HOT rewrite before this is consumed.  `active` is reset every
    // batch in the hot pre-filter loop.
    struct KeptHotPiece {
        bool active = false;
        PairsRange pairs_range;
        KeyRange key_range;
        uint32_t load = 0;
        dpu_id_t origin = 0;
    };
    ExtendableBuffer<KeptHotPiece> kept_hot{nr_base_parts};
    // Pieces[1..] emitted by the hot-split pass 1, indexed by source DPU.
    // A member rather than a per-batch local so the inner vectors keep their
    // capacity across batches; entries are cleared at the top of pass 1.
    std::vector<std::vector<NewHotRange>> hot_split_plans = std::vector<std::vector<NewHotRange>>(nr_base_parts);

    TaskID last_qry_type = TASK_NONE;
    QueryData<key_uint64_t, value_uint64_t> get_queries{nr_base_parts, get_parallelism()};
    QueryData<key_uint64_t, KVPair> pred_queries{nr_base_parts, get_parallelism()};
    QueryData<KVPair, void> insert_queries{nr_base_parts, get_parallelism()};
    QueryData<key_uint64_t, uint8_t> delete_queries{nr_base_parts, get_parallelism()};
    QueryData<RangeCountQuery, uint64_t> rcqs{nr_base_parts, get_parallelism()};
    QueryData<KeyRange, value_uint64_t> rmaxqs{nr_base_parts, get_parallelism()};

    const ExtendableBuffer<InputHeader> input_headers{nr_base_parts};

    // for insert queries
    const ExtendableBuffer<CachelineAligned<key_uint64_t>> new_min_keys{get_parallelism()};

    //! @name min-refresh questions of one delete batch
    //! Grouped by DPU: DPU d owns `[refresh_begin[d], refresh_begin[d + 1])` of
    //! the flat arrays, its cold questions first, as TASK_DELETE expects
    //! (docs/dpu_task_signature.md).  A partition asks at most one question.
    //! @{
    const ExtendableBuffer<uint32_t> refresh_begin{nr_base_parts + 1};
    const ExtendableBuffer<std::array<uint32_t, 2>> refresh_nr_requests{nr_base_parts}, refresh_cursor{nr_base_parts};
    const ExtendableBuffer<KeyRange> refresh_ranges{max_nr_parts(nr_base_parts)};
    const ExtendableBuffer<KVPair> refresh_responses{max_nr_parts(nr_base_parts)};
    //! whether a partition asked, and which slot above it got
    const ExtendableBuffer<uint8_t> refresh_asked{max_nr_parts(nr_base_parts)};
    const ExtendableBuffer<uint32_t> refresh_slot{max_nr_parts(nr_base_parts)};
    //! @}

    //! @name What TASK_INSERT and TASK_DELETE send and receive on top of the
    //! per-query payload, at the tail of the buffer.
    //! @{
    struct NrPairsTail;
    struct RefreshRequestTail;
    struct RefreshResponseTail;
    //! @}

    // retrieved data from DPU
    ExtendableBuffer<KVPair> data_buf;

    // for commanding serialization of data
    const ExtendableBuffer<dpu_id_t> base_to_nr_incisions_psum{nr_base_parts + 1};
    ExtendableBuffer<key_uint64_t> incision_keys{nr_base_parts};
    // for receiving nr. of pairs for each range
    const ExtendableBuffer<uint32_t> incision_indices{nr_base_parts};

    // for communicating serialized data
    const ExtendableBuffer<LinkedList<PairsRange>> cold_ranges_lists{nr_base_parts};
    const ExtendableBuffer<LinkedPairsRange> cold_ranges{nr_base_parts * 2};
    const ExtendableBuffer<PairsRange> hot_ranges{nr_base_parts};

    // used in rebalancing
    const ExtendableBuffer<LinkedList<ChunkedPairsRange>> chunked_cold_ranges_lists{nr_base_parts};
    const ExtendableBuffer<LinkedChunkedPairsRange> chunked_cold_ranges{nr_base_parts * 2};
    const ExtendableBuffer<KeyRange> cold_key_ranges{nr_base_parts * 2};
    const ExtendableBuffer<std::pair<dpu_id_t, uint32_t /* load */>> cold_loads{nr_base_parts};
    const ExtendableBuffer<uint32_t /* npairs */> cold_npairs_list{nr_base_parts};
    const ExtendableBuffer<NewHotRange> new_hots{nr_base_parts};
    inline static thread_local ExtendableBuffer<uint32_t> chunk2load;

    // pass intermediate data to parallel workers
    std::any any_tmp_data;

    inline static thread_local unsigned numa_id = 0;
    void set_numa_affinity(unsigned tid);

    void distribute_equal_data(const KVPair sorted_pairs[], size_t nr_pairs);
    void load_partitioning(const std::vector<Partition>& partitioning);
    void distribute_data_based_on_partitions(const KVPair sorted_pairs[], size_t nr_pairs);
    template <typename PairsRangeLike>
    void initialize_in_dpu(const CachelineAligned<std::array<uint32_t, 2>> nr_pairs[], const LinkedList<PairsRangeLike> colds[], const PairsRange hots[]);
    template <typename PairsRangeLike>
    void initialize_in_dpu(const InputHeader input_headers[], const LinkedList<PairsRangeLike> colds[], const PairsRange hots[]);
    //! @brief Re-derives `base_part`, `hot_part` and `nr_hot_parts` from `parts`.
    void rebuild_part_indices();
    //! @brief Rebuilds `parts` from what the trees now hold.
    //!
    //! `hot_entries[0, nr_hot_entries)` names every hot partition there is to
    //! be, in key order.  A base partition whose cold tree this round rebuilt,
    //! which `renewed` decides, takes its cold partitions from the pieces
    //! staged in `chunked_cold_ranges_lists`; the others keep the ones they
    //! have.  Pieces holding no pair contribute nothing, so the partition
    //! before them takes their keys over.
    template <typename Renewed /* bool(dpu_id_t) */>
    void rebuild_parts(dpu_id_t nr_hot_entries, Renewed&& renewed);
    //! @brief Tells a DPU where to cut the cold tree it is about to serialize,
    //! and what key range each of the resulting pieces covers.
    //!
    //! The cuts go between consecutive cold partitions of base `idx_base`,
    //! each named by the key the piece after it begins at.  Returns how many
    //! pieces there are, one more than the number of cuts, and none at all
    //! when the base holds no cold partition.
    dpu_id_t cut_points_of_cold_tree(dpu_id_t idx_base, key_uint64_t incisions[], KeyRange key_ranges[]) const;
    ptrdiff_t locate_pred_partition(key_uint64_t key) const;

    template <typename Query, typename Result>
    void route_queries(
        uint32_t nr_queries, const Query queries[], Result* results,
        QueryData<Query, Result>& routed);
    template <typename Query, typename Result>
    void route_queries_impl(unsigned tid);
    template <typename Query, typename Result>
    void route_clear_impl(unsigned tid);
    template <typename Query, typename Result>
    void route_accumulate_impl(unsigned tid);
    template <typename Query, typename Result>
    using TmpDataForRouteQueries = const std::tuple<uint32_t, const Query*, QueryData<Query, Result>*, Result*>;

    template <typename Query, typename Result>
    void route_single_point_query(
        uint32_t idx_qry, const Query& qry, Result* result,
        QueryData<Query, Result>& routed,
        unsigned tid);
    template <typename Query, typename Result>
    void not_found_in_point_query(
        uint32_t idx_qry, const Query& qry, Result* result,
        QueryData<Query, Result>& routed,
        unsigned tid);

    template <typename Query, typename Result>
    void route_single_range_query(
        uint32_t idx_qry, const Query& qry, Result* result,
        QueryData<Query, Result>& routed,
        unsigned tid);

    template <typename Query, typename Result>
    void execute_in_dpus(TaskID task_no, QueryData<Query, Result>&);
    //! @brief Runs one task, appending `send_tail` to what the queries send
    //! and `recv_tail` to what their results take back.
    template <typename Query, typename Result, typename SendTail, typename RecvTail>
    void execute_in_dpus(TaskID task_no, QueryData<Query, Result>&, const SendTail&, const RecvTail&);

    void postprocess_of_get(value_uint64_t result[]);
    void postprocess_of_get_impl(unsigned tid);

    void postprocess_of_pred(KVPair result[]);
    void postprocess_of_pred_impl(unsigned tid);

    //! @brief How many min-refresh questions a DPU is being asked.
    uint32_t nr_refreshes_of(dpu_id_t dpu) const { return refresh_begin[dpu + 1] - refresh_begin[dpu]; }
    void build_delete_refresh_requests();
    void postprocess_of_delete(uint8_t existed[]);
    void postprocess_of_delete_impl(unsigned tid);
    void apply_delete_refresh_responses();

    void postprocess_of_rcq(uint32_t nr_queries, uint64_t result[]);
    void postprocess_of_rcq_impl(unsigned tid);
    using TmpDataForPostprocessOfRCQ = std::tuple<uint32_t, uint64_t*>;

    void postprocess_of_rmaxq(uint32_t nr_queries, value_uint64_t result[]);
    void postprocess_of_rmaxq_impl(unsigned tid);
    using TmpDataForPostprocessOfRMaxQ = std::tuple<uint32_t, value_uint64_t*>;

    size_t retrieve_all_data(ExtendableBuffer<KVPair>& buf);
    template <typename Query, typename Result>
    void repartition(uint32_t nr_queries, const Query queries[], Result results[], QueryData<Query, Result>& routed);
    enum class Balanced {
        Yes,
        No
    };
    template <typename Query, typename Result>
    Balanced incremental_repartition(uint32_t nr_queries, const Query queries[], Result results[], QueryData<Query, Result>& routed);
    template <typename Query, typename Result>
    void full_repartition(uint32_t nr_queries, const Query queries[], Result* results, QueryData<Query, Result>& routed);

    template <typename Query, typename Result>
    void incremental_repartition_worker_cold(unsigned tid);
    template <typename Query, typename Result>
    void incremental_repartition_worker_hot(unsigned tid);
    template <typename Query, typename Result>
    void full_repartition_worker(unsigned tid);
};


#include "bpforest.ipp"
