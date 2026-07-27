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
#include <set>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
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
template <typename Query, typename Result>
struct QueryData {
    std::vector<QueryDataPerRange<Query, Result>> cold, hot;

    explicit QueryData(dpu_id_t nr_dpus, unsigned nr_threads);
    void clear();
    void clear_for_thread(unsigned tid);
};

struct BasePartitionDelim {
    dpu_id_t dpu;
    friend bool operator<(const BasePartitionDelim& lhs, const BasePartitionDelim& rhs) { return lhs.dpu < rhs.dpu; }
};
struct HotPartitionDelim {
    key_uint64_t max_key;
    dpu_id_t dpu;

    // Two hot partitions cannot start with the same key.  No need for comparablility.
    friend bool operator<(const HotPartitionDelim&, const HotPartitionDelim&) { return false; }
};
using PartitionDelim = std::pair<key_uint64_t, std::variant<BasePartitionDelim, HotPartitionDelim>>;

struct BPForestParameter {
    unsigned balancing = 1;
    unsigned more_hotness = 1;
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
    void batch_pred(uint32_t nr_queries, const key_uint64_t keys[], KVPair result[]);
    void batch_insert(uint32_t nr_queries, const KVPair pairs[]);
    void batch_delete(uint32_t nr_queries, const key_uint64_t pairs[]);
    void batch_range_count(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[]);
    void batch_range_max(uint32_t nr_queries, const KeyRange queries[], value_uint64_t result[]);
    void batch_scan(size_t nr_queries, const KeyRange ranges[], BatchScanResult& result);
    std::vector<std::array<uint32_t, 2>> get_nr_pairs() const;
    std::vector<std::array<uint32_t, 2>> last_query_dist() const;

    void partition_with_get_batch(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    void partition_with_pred_batch(uint32_t nr_queries, const key_uint64_t keys[], KVPair result[]);
    void partition_with_insert_batch(uint32_t nr_queries, const KVPair pairs[]);
    void partition_with_delete_batch(uint32_t nr_queries, const key_uint64_t pairs[]);
    void partition_with_range_count_batch(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[]);
    void partition_with_range_max_batch(uint32_t nr_queries, const KeyRange queries[], value_uint64_t result[]);

    void print_params(std::ostream&) const;
    std::vector<Partition> dump_partitions() const;

private:
    BPForest(const Param& = {});

    const dpu_id_t nr_base_parts;

    std::set<PartitionDelim> delims;
    using DelimIter = std::set<PartitionDelim>::iterator;
    std::vector<DelimIter> cold_delims = [this] {
        std::vector<DelimIter> result(nr_base_parts + 1);
        result.back() = delims.end();
        return result;
    }(),
                           hot_delims = std::vector<DelimIter>(nr_base_parts);
    const ExtendableBuffer<CachelineAligned<std::array<uint32_t, 2>>> nr_pairs{nr_base_parts};

    std::vector<key_uint64_t> combined_delims = [this] {
        std::vector<key_uint64_t> result;
        result.reserve(nr_base_parts * 3);
        return result;
    }();
    struct QueryDest {
        dpu_id_t dpu;
        bool is_hot;
    };
    std::vector<QueryDest> combined_delims_dest = [this] {
        std::vector<QueryDest> result;
        result.reserve(nr_base_parts * 3);
        return result;
    }();

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
    QueryData<key_uint64_t, void> delete_queries{nr_base_parts, get_parallelism()};
    QueryData<RangeCountQuery, uint64_t> rcqs{nr_base_parts, get_parallelism()};
    QueryData<KeyRange, value_uint64_t> rmaxqs{nr_base_parts, get_parallelism()};

    const ExtendableBuffer<InputHeader> input_headers{nr_base_parts};

    // for insert queries
    const ExtendableBuffer<CachelineAligned<key_uint64_t>> new_min_keys{get_parallelism()};

    // retrieved data from DPU
    ExtendableBuffer<KVPair> data_buf;

    // for commanding serialization of data
    const ExtendableBuffer<dpu_id_t> base_to_nr_hot_psum{nr_base_parts + 1};
    const ExtendableBuffer<dpu_id_t> nr_extracted_hots{nr_base_parts};
    ExtendableBuffer<key_uint64_t> hot_delim_keys{nr_base_parts};
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

    void combine_delims();

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
    template <typename Query, typename Result, typename Func>
    void execute_in_dpus(TaskID task_no, QueryData<Query, Result>&, Func&&);

    void postprocess_of_get(value_uint64_t result[]);
    void postprocess_of_get_impl(unsigned tid);

    void postprocess_of_pred(KVPair result[]);
    void postprocess_of_pred_impl(unsigned tid);

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
