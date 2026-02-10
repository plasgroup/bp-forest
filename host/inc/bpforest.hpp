#pragma once

#include "common.h"
#include "const_cap_vector.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "parallel.hpp"
#include "partition.hpp"
#include "workload_types.h"

#include <any>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
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

struct BPForestParameter {
    unsigned balancing = 1;
    bool one_scan = false;
    bool naive_init = false;
    unsigned nr_host_threads = 0;
};
struct BPForest : ParallelManager<BPForest> {
    using Param = BPForestParameter;

    BPForest(const KVPair sorted_pairs[], size_t nr_pairs, const Param& = {});
    BPForest(const KVPair sorted_pairs[], size_t nr_pairs, const std::vector<Partition>& partitioning, const Param& = {});
    ~BPForest();

    void batch_get(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    void batch_insert(uint32_t nr_queries, const KVPair pairs[]);
    void batch_delete(uint32_t nr_queries, const key_uint64_t pairs[]);
    void batch_range_minimum(size_t nr_queries, const KeyRange ranges[], value_uint64_t result[]);
    void batch_range_count(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[]);
    std::vector<size_t> get_nr_rcqs() const;
    void batch_scan(size_t nr_queries, const KeyRange ranges[], BatchScanResult& result);

    template <typename Query>
    std::vector<std::pair<size_t /* nr pairs in cold */, size_t /* nr pairs in hot */>>
    partition_data_with_reference_point_queries(size_t nr_queries, const Query queries[]);
    template <typename Query>
    std::vector<std::pair<size_t /* nr pairs in cold */, size_t /* nr pairs in hot */>>
    partition_data_with_reference_range_queries(size_t nr_queries, const Query queries[]);

    void print_params(std::ostream&) const;
    std::vector<Partition> dump_partitions() const;

private:
    const dpu_id_t nr_cold_ranges;
    dpu_id_t nr_hot_ranges = 0;

    std::array<key_uint64_t, MAX_NR_DPUS> cold_delims;

    std::array<key_uint64_t, MAX_NR_DPUS> hot_delims, hot_max_key;
    std::array<dpu_id_t, MAX_NR_DPUS> dpu_to_hot_range;

    //! @brief i-th cold range -> [cold_to_hot[i]-th, cold_to_hot[i+1]-th) hot range(s)
    std::array<dpu_id_t, MAX_NR_DPUS + 1> cold_to_hot{};


    std::vector<key_uint64_t> combined_delims;
    struct QueryDest {
        dpu_id_t dpu;
        bool is_hot;
    };
    std::vector<QueryDest> combined_delims_dest;

    const Param param;
    double threshold_nr_queries_to_hot = 0;

    ExtendableBuffer<uint64_t> rg_qry_data;
    ExtendableBuffer<std::array<size_t, 2>> rg_qry_to_minirg;
    std::vector<size_t> rg_lump_end_indices;

    template <typename Query, typename Result>
    struct QueryDataPerRange {
        // qrys[idx_host_thread][idx_qry]
        std::vector<std::vector<Query>> qrys;
        // orig_idxs[idx_host_thread][idx_qry]
        std::vector<std::vector<uint32_t>> orig_idxs;
        // results[idx_host_thread][idx_qry]
        std::vector<ExtendableBuffer<Result>> results;
        uint16_t nr_qrys;
    };
    template <typename QandR>
    struct QueryDataPerRange<QandR, QandR> {
        std::vector<std::vector<QandR>> qrys;
        std::vector<std::vector<uint32_t>> orig_idxs;
        uint16_t nr_qrys;
    };
    template <typename Query>
    struct QueryDataPerRange<Query, void> {
        std::vector<std::vector<Query>> qrys;
        uint16_t nr_qrys;
    };
    template <typename Query, typename Result>
    struct QueryData {
        std::array<QueryDataPerRange<Query, Result>, MAX_NR_DPUS> cold, hot;
    };

    QueryData<key_uint64_t, value_uint64_t> get_queries;
    QueryData<KVPair, void> insert_queries;
    QueryData<key_uint64_t, void> delete_queries;
    QueryData<RangeCountQuery, uint64_t> rcqs;

    std::vector<key_uint64_t> new_min_keys;

public:  // TODO: privatize
    struct Summary {
        uint32_t nr_pairs;
        uint32_t nr_blocks;
        ExtendableBuffer<SummaryBlock> blocks;

        key_uint64_t head_key(uint32_t i) const { return blocks[i / 4].head_keys[i % 4]; }
        uint16_t nr_keys(uint32_t i) const { return blocks[i / 4].nr_keys[i % 4]; }
    };

private:
    std::array<Summary, MAX_NR_DPUS> summaries;

    // used in rebalancing
    ExtendableBuffer<size_t> load_idxs;
    std::array<ExtendableBuffer<KVPair>, MAX_NR_DPUS> hot_kvpairs;

    void distribute_initial_data(const KVPair sorted_pairs[], size_t nr_pairs);
    void apply_partitioning_of_initial_data(const KVPair sorted_pairs[], size_t nr_pairs, const std::vector<Partition>& partitioning);

    void combine_delims();

    template <bool HasHotRanges>
    void route_get_queries(size_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    bool check_if_get_queries_balance(size_t nr_queries);
    void execute_get_in_dpus();
    void postprocess_of_get(value_uint64_t result[]);
    struct GetQuerySender;
    struct GetResultReceiver;

    template <bool HasHotRanges>
    void route_get_queries_impl(unsigned tid);
    void postprocess_of_get_impl(unsigned tid);

    void execute_insert_in_dpus();
    struct InsertQuerySender;

    void execute_delete_in_dpus();
    struct DeleteQuerySender;

    bool check_if_rcq_balance();
    void execute_rcq_in_dpus();
    void postprocess_of_rcq(uint32_t nr_queries, uint64_t result[]);
    void postprocess_of_rcq_impl(unsigned tid);
    using TmpDataForPostprocessOfRCQ = std::tuple<uint32_t, uint64_t*>;
    struct RCQSender;
    struct RCQResultReceiver;

    template <typename Query, typename Result>
    using TmpDataForRouteQueries = std::tuple<uint32_t, const Query*, QueryData<Query, Result>*, Result*>;
    std::any any_tmp_data;

    union TmpData {
        TmpData() {}
        ~TmpData() {}

        template <typename T, typename F>
        void with(T TmpData::*member, const T& value, F&& func)
        {
            T* place = &(this->*member);
            new (place) T{value};
            std::forward<F>(func)();
            place->~T();
        }

        std::tuple<size_t, const key_uint64_t*, value_uint64_t*> route_get_queries;
        value_uint64_t* postprocess_of_get;
    } tmp_data;


    size_t /* nr_delim_keys */ preprocess_rmq(const size_t nr_queries, const KeyRange ranges[]);
    template <bool HasHotRanges>
    void route_rmq(
        size_t nr_delim_keys,
        std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
        std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
        std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
        std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
        std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle,
        std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_lump_idx,
        std::array<std::array<size_t, 2>, MAX_NR_DPUS + 1>& hot_range_to_lump_idx) const;
    bool check_if_rmq_balance(size_t nr_delims,
        const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
        const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
        const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle) const;
    size_t nr_rmq_to_cold(dpu_id_t idx_cold,
        const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
        const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
        const std::array<bool, MAX_NR_DPUS + 1>& if2_cold_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle) const;
    size_t nr_rmq_to_hot(dpu_id_t idx_hot,
        const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle) const;
    void execute_rmq_in_dpus(
        const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
        const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
        const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle,
        const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_lump_idx,
        const std::array<std::array<size_t, 2>, MAX_NR_DPUS + 1>& hot_range_to_lump_idx);
    struct RMQSender;
    struct RMQResultReceiver;

public:
    template <typename Query, typename Result>
    using RouteSingleQueryFunc = void (BPForest::*)(uint32_t idx_qry, const Query& qry, Result*, QueryData<Query, Result>& routed, unsigned tid);

private:
    template <auto /* RouteSingleQueryFunc<Query, Result> */ Func, typename Query, typename Result, std::enable_if_t<std::is_same_v<decltype(Func), RouteSingleQueryFunc<Query, Result>>, std::nullptr_t> = nullptr>
    void route_queries(
        uint32_t nr_queries, const Query queries[], Result* results,
        QueryData<Query, Result>& routed);
    template <typename Query, typename Result, RouteSingleQueryFunc<Query, Result> Func>
    void route_queries_impl(unsigned tid);

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
    struct QuerySender;
    template <typename Query, typename Result>
    struct ResultReceiver;
    template <typename Query, typename Result>
    void execute_in_dpus(uint32_t task_no, QueryData<Query, Result>&);

    template <typename Query, typename Result>
    bool check_if_queries_balance(size_t nr_queries, const QueryData<Query, Result>& routed);
    std::vector<std::pair<size_t /* nr pairs in cold */, size_t /* nr pairs in hot */>>
    repartition(const std::vector<key_uint64_t>& sorted_queries);
    size_t retrieve_all_data(ExtendableBuffer<KVPair>& buf) const;
    struct SerializationCommander;
    struct SerializaionNrPairsReceiver;
    struct SerializedKVPairReceiver;

    void restore_hot_ranges();
    struct HotKVPairsFlattenedCollecter;
    struct HotKVPairsRestorer;

    void take_summary(const std::array<bool, MAX_NR_DPUS>& cold_range_rebalanced);
    struct SummaryHeadReceiver;
    struct SummaryChunkInfoReceiver;
    struct SummaryReceiver;

    void extract_and_distribute_hot_ranges();
#ifdef EXTRACT_BY_INITIALIZATION
    ExtendableBuffer<KVPair> initial_data;
    size_t data_size;
    struct RebalancedColdKVPairsSender;
    struct HotKVPairsSender;
#else
    struct HotKVPairsExtracter;
    struct NrHotKVPairsCollecter;
    struct HotKVPairsExtractedCollecter;
    struct HotRangeConstructor;
#endif
};


#include "bpforest.ipp"
