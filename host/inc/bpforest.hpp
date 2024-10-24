#pragma once

#include "common.h"
#include "const_cap_vector.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "workload_types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <tuple>
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
};
struct BPForest {
    using Param = BPForestParameter;

    BPForest(size_t nr_pairs, const KVPair sorted_pairs[], const Param& = {});
    ~BPForest() {}

    void batch_get(size_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    void batch_range_minimum(size_t nr_queries, const KeyRange ranges[], value_uint64_t result[]);
    void batch_scan(size_t nr_queries, const KeyRange ranges[], BatchScanResult& result);

private:
    const dpu_id_t nr_cold_ranges;
    dpu_id_t nr_hot_ranges = 0;

    std::array<key_uint64_t, MAX_NR_DPUS> cold_delims;

    std::array<key_uint64_t, MAX_NR_DPUS> hot_delims, hot_max_key;
    std::array<dpu_id_t, MAX_NR_DPUS> dpu_to_hot_range;

    //! @brief i-th cold range -> [cold_to_hot[i]-th, cold_to_hot[i+1]-th) hot range(s)
    std::array<dpu_id_t, MAX_NR_DPUS + 1> cold_to_hot{};

    const Param param;
    double threshold_nr_queries_to_hot = 0;

    struct {
        std::array<std::vector<uint64_t>, MAX_NR_DPUS> cold, hot;
    } pt_qrys;
    ExtendableBuffer<uint64_t> rg_qry_data;
    ExtendableBuffer<std::array<size_t, 2>> rg_qry_to_minirg;
    std::vector<size_t> rg_lump_end_indices;
    struct {
        std::array<std::vector<size_t>, MAX_NR_DPUS> cold, hot;
    } orig_idxs;

public:  // TODO: privatize
    struct Summary {
        uint32_t nr_pairs;
        uint32_t nr_entries;
        ExtendableBuffer<SummaryBlock> blocks;

        key_uint64_t head_key(uint32_t i) const { return blocks[i / 4].head_keys[i % 4]; }
        uint16_t nr_keys(uint32_t i) const { return blocks[i / 4].nr_keys[i % 4]; }
    };

private:
    std::array<Summary, MAX_NR_DPUS> summaries;

    // used in rebalancing
    ExtendableBuffer<size_t> query_idxs;
    std::array<ExtendableBuffer<KVPair>, MAX_NR_DPUS> hot_kvpairs;

    void ditribute_initial_data(size_t nr_pairs, const KVPair sorted_pairs[]);

    template <bool HasHotRanges>
    void route_get_queries(size_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    bool check_if_get_queries_balance(size_t nr_queries);
    void execute_get_in_dpus();
    struct GetQuerySender;
    struct GetResultReceiver;

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
        const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
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

    // //! @return whether any hot range is to be restored
    // bool propagete_rebalancing(
    //     std::array<bool, MAX_NR_DPUS>& cold_range_rebalanced,
    //     std::array<bool, MAX_NR_DPUS>& hot_range_rebalanced) const;

    void restore_hot_ranges();
    struct HotKVPairsFlattenedCollecter;
    struct HotKVPairsRestorer;

    void take_summary(const std::array<bool, MAX_NR_DPUS>& cold_range_rebalanced);
    struct SummaryMetadataReceiver;
    struct SummaryReceiver;

    void extract_and_distribute_hot_ranges();
    struct HotKVPairsExtracter;
    struct NrHotKVPairsCollecter;
    struct HotKVPairsExtractedCollecter;
    struct HotRangeConstructor;
};


#include "bpforest.ipp"
