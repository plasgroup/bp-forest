#pragma once

//! @file
//! UPMEM module implementation shared by the emulated-DPU builds (fake_dpu
//! and dpu_on_cpu).  Transfers assert the constraints the SDK's transfer
//! functions enforce, and keep the rank-wise total_xfer_bytes accounting.
//!
//! The including file must define, before including this one:
//! - constexpr dpu_id_t NrDPUs, NrDPUsInRank
//! - inline std::optional<Backend> emulated_dpus, where Backend provides
//!   get_comm_buffer(dpu_id_t), launch_dpu(dpu_id_t), wait_all() and
//!   mram_heap_bytes()
//! as well as upmem_init_impl()/upmem_release_impl() managing emulated_dpus.

#include "batch_transfer_buffer.hpp"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "sg_block_info.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <type_traits>
#include <utility>


inline UPMEM_AsyncDuration::~UPMEM_AsyncDuration()
{
}


inline dpu_id_t nr_dpus_in_set(const DPUSet& set)
{
    return static_cast<dpu_id_t>(set.count());
}
inline dpu_id_t upmem_get_nr_dpus()
{
    return NrDPUs;
}
inline std::pair<dpu_id_t, dpu_id_t> upmem_get_dpu_range_in_rank(dpu_id_t idx_rank)
{
    return {NrDPUsInRank * idx_rank, NrDPUsInRank * (idx_rank + 1)};
}

inline DPUSet select_dpu(dpu_id_t index)
{
    assert(index < upmem_get_nr_dpus());
    DPUSet result;
    result.set(index);
    return result;
}
inline DPUSet select_rank(dpu_id_t index)
{
    assert(index < NR_RANKS);
    DPUSet result;
    for (dpu_id_t i = 0; i < NrDPUsInRank; i++) {
        result.set(index * NrDPUsInRank + i);
    }
    return result;
}


template <bool ToDPU, class BatchTransferBuffer>
inline void xfer_with_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration&)
{
    assert(offset % 8 == 0 && "MRAM offset of a transfer must be 8-byte aligned");
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (dpu_id_t i = 0; i < NrDPUs;) {
        size_t max_xfer_bytes = 0;
        for (dpu_id_t j = 0; i < NrDPUs && j < NrDPUsInRank; i++, j++) {
            if (set[i]) {
                auto* const ptr = buf.for_dpu(i);
                const auto size = buf.bytes_for_dpu(i);
                static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                    "non-trivial copying cannot be performed between CPU and DPU");
                assert(size % 8 == 0 && "length of a transfer must be 8-byte aligned");
                assert(offset + size <= emulated_dpus->mram_heap_bytes() && "transfer beyond the end of MRAM");

                void* const mram_addr = emulated_dpus->get_comm_buffer(i) + offset;
                if constexpr (ToDPU)
                    std::memcpy(mram_addr, ptr, size);
                else
                    std::memcpy(ptr, mram_addr, size);
                total_effective_bytes += size;
                max_xfer_bytes = std::max<size_t>(max_xfer_bytes, size);
            }
        }
        total_xfer_bytes += max_xfer_bytes * NrDPUsInRank;
    }
}

template <typename T>
inline void broadcast_to_dpu(const DPUSet& set, uint32_t offset, const Single<T>& datum, UPMEM_AsyncDuration& async)
{
    xfer_with_dpu<true>(set, offset, datum, async);
}

template <bool ToDPU, class ScatteredBatchTransferBuffer>
inline void scatter_gather_with_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration&)
{
    // per-DPU lengths need no alignment here: the upmem implementation
    // rounds them up to 8 bytes when issuing the scatter-gather transfer
    assert(offset % 8 == 0 && "MRAM offset of a transfer must be 8-byte aligned");
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (dpu_id_t i = 0; i < NrDPUs;) {
        size_t max_xfer_bytes = 0;
        for (dpu_id_t j = 0; i < NrDPUs && j < NrDPUsInRank; i++, j++) {
            if (set[i]) {
                const size_t xfer_bytes = buf.bytes_for_dpu(i);
                assert(offset + xfer_bytes <= emulated_dpus->mram_heap_bytes() && "transfer beyond the end of MRAM");
                std::byte* mram_addr = emulated_dpus->get_comm_buffer(i) + offset;
                size_t left_xfer_bytes = xfer_bytes;

                sg_block_info block;
                for (block_id_t idx_block = 0; buf(&block, i, idx_block); idx_block++) {
                    assert(left_xfer_bytes >= block.length);
                    if constexpr (ToDPU)
                        std::memcpy(mram_addr, block.addr, block.length);
                    else
                        std::memcpy(block.addr, mram_addr, block.length);
                    mram_addr += block.length;
                    left_xfer_bytes -= block.length;
                    total_effective_bytes += block.length;
                }
                max_xfer_bytes = std::max(max_xfer_bytes, xfer_bytes);
            }
        }
        total_xfer_bytes += max_xfer_bytes * NrDPUsInRank;
    }
}

inline void execute(const DPUSet& set, UPMEM_AsyncDuration&)
{
    for (dpu_id_t i = 0; i < NrDPUs; i++)
        if (set[i])
            emulated_dpus->launch_dpu(i);
    emulated_dpus->wait_all();
}

template <class Func>
inline void then_call(const DPUSet& set, Func& func, UPMEM_AsyncDuration& async)
{
    if (set == all_dpu) {
        for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
            func(idx_rank, async);
        }
    } else {
        for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
            const bool is_active = set[idx_rank * NrDPUsInRank];
            for (dpu_id_t idx_dpu_in_rank = 1; idx_dpu_in_rank < NrDPUsInRank; idx_dpu_in_rank++) {
                if (set[idx_rank * NrDPUsInRank + idx_dpu_in_rank] != is_active) {
                    std::cerr << "callback bound to one DPU is not supported" << std::endl;
                    std::abort();
                }
            }

            if (is_active) {
                func(0, async);
            }
        }
    }
}
