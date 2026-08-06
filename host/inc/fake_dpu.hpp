#pragma once

#include "common.h"
#include "workload_types.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>


//! @brief Emulates a single DPU running the B+-Forest DPU program.
//!
//! The communication buffer is laid out exactly like the MRAM heap of a real
//! DPU: the host writes an InputHeader followed by the task payload at offset
//! 0, and execute() leaves each task's results where the DPU program would
//! (e.g. at InputHeader::qrys::result_offset for query tasks), so the host can
//! read them back through the same offsets as with real DPUs.  The cold/hot
//! trees are emulated with std::map; deletion erases pairs instead of leaving
//! the tombstones the DPU program uses, which yields the same observable
//! contents for every task.
class FakeDPU
{
public:
    // as large as the whole MRAM of a real DPU
    static constexpr size_t MRAMSize = 1ul << 26;

private:
    struct alignas(uint64_t) MRAMImage {
        std::byte impl[MRAMSize];
    };
    const std::unique_ptr<MRAMImage> mram_impl{new MRAMImage};
    std::byte* mram{&mram_impl->impl[0]};

    using Tree = std::map<key_uint64_t, value_uint64_t>;
    Tree cold_tree, hot_tree;

public:
    std::byte* get_comm_buffer() { return &mram[0]; }
    void execute();

private:
    static void construct_tree(Tree& tree, uint32_t nr_pairs, const KVPair pairs[]);
    static void task_get(const Tree& tree, uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[]);
    static void task_pred(const Tree& tree, uint32_t nr_queries, const key_uint64_t keys[], KVPair result[]);
    static void task_range_count(const Tree& tree, uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[]);
    static void task_range_max(const Tree& tree, uint32_t nr_queries, const KeyRange queries[], value_uint64_t result[]);
    static void task_insert(Tree& tree, uint32_t nr_queries, const KVPair pairs[]);
    static void task_delete(Tree& tree, uint32_t nr_queries, const key_uint64_t keys[]);
};


#include "fake_dpu.ipp"
