#pragma once

#include "common.h"
#include "workload_types.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <new>
#include <utility>
#include <vector>


class DPUEmulator
{
    static constexpr size_t MRAMSize = 1ul << 25;
    std::unique_ptr<std::byte[]> mram{new (std::align_val_t{alignof(uint64_t)}) std::byte[MRAMSize]};
    std::unique_ptr<std::byte[]> mram_2nd{new (std::align_val_t{alignof(uint64_t)}) std::byte[MRAMSize]};

    using Tree = std::map<key_uint64_t, value_uint64_t>;
    Tree cold_tree, hot_tree;
    std::vector<key_uint64_t> cold_incisions;

public:
    std::byte* get_comm_buffer() { return &mram[0]; }
    void execute();

private:
    void task_init(unsigned nr_pairs, const KVPair pairs[]);
    void task_get(const Tree& tree, unsigned nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    void task_pred(const Tree& tree, unsigned nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    uint32_t /* nr_values */ task_scan_cold(unsigned nr_queries, const KeyRange ranges[],
        uint32_t incision_pos[], IndexRange result_ranges[], value_uint64_t values[]);
    uint32_t /* nr_values */ task_scan_hot(unsigned nr_queries, const KeyRange ranges[],
        IndexRange result_ranges[], value_uint64_t values[]);
    void task_insert(Tree& tree, unsigned nr_queries, const KVPair pairs[]);
    key_uint64_t /* min_key */ task_delete(Tree& tree, unsigned nr_queries, const key_uint64_t keys[]);

    std::pair<uint32_t /* nr_pairs */, uint32_t /* nr_entries */> task_summarize(SummaryBlock summary_blocks[]);
    void task_extract(unsigned nr_ranges, const KeyRange ranges[],
        uint32_t nr_pairs[], KVPair pairs[]);
    void task_construct_hot(unsigned nr_pairs, const KVPair pairs[]);
    uint32_t /* nr_pairs */ task_flatten_hot(KVPair pairs[]);
    void task_restore(unsigned nr_ranges, const uint32_t nr_pairs[], const KVPair pairs[]);
};


#include "dpu_emulator.ipp"
