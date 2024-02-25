#include "batch_transfer_buffer.hpp"
#include "dpu_set.hpp"
#include "emulation.hpp"
#include "host_params.hpp"
#include "sg_block_info.hpp"
#include "statistics.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>


Emulation emu[EMU_MAX_DPUS];


static void upmem_init_impl(const char*, bool)
{
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        all_dpu[i] = true;
        emu[i].init(i);
    }
}
static void upmem_release_impl()
{
    all_dpu ^= all_dpu;
    Emulation::terminate();
}


static uint32_t nr_dpus_in_set(const DPUSet& set)
{
    return set.count();
}

static void select_dpu(DPUSet* dst, dpu_id_t index)
{
    DPUSet mask;
    mask[index] = true;
    *dst = all_dpu & mask;
}


template <bool ToDPU, class BatchTransferBuffer>
static void xfer_with_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf)
{
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (dpu_id_t i = 0; i < EMU_MAX_DPUS;) {
        size_t max_xfer_bytes = 0;
        for (dpu_id_t j = 0; i < EMU_MAX_DPUS && j < NR_DPUS_IN_RANK; i++, j++) {
            if (set[i]) {
                auto* const ptr = buf.for_dpu(i);
                const auto size = buf.bytes_for_dpu(i);
                static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                    "non-trivial copying cannot be performed between CPU and DPU");

                void* mram_addr = emu[i].get_addr_of_symbol(symbol);
                if constexpr (ToDPU)
                    std::memcpy(mram_addr, ptr, size);
                else
                    std::memcpy(ptr, mram_addr, size);
                total_effective_bytes += size;
                max_xfer_bytes = std::max(max_xfer_bytes, size);
            }
        }
        total_xfer_bytes += max_xfer_bytes * NR_DPUS_IN_RANK;
    }
#ifdef MEASURE_XFER_BYTES
    xfer_statistics.add(symbol, total_xfer_bytes, total_effective_bytes);
#endif /* MEASURE_XFER_BYTES */
}

template <typename T>
static void broadcast_to_dpu(const DPUSet& set, const char* symbol, const Single<T>& datum)
{
    xfer_with_dpu<true>(set, symbol, datum);
}

template <bool ToDPU, class ScatteredBatchTransferBuffer>
static void scatter_gather_with_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf)
{
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (dpu_id_t i = 0; i < EMU_MAX_DPUS;) {
        size_t max_xfer_bytes = 0;
        for (dpu_id_t j = 0; i < EMU_MAX_DPUS && j < NR_DPUS_IN_RANK; i++, j++) {
            if (set[i]) {
                uintptr_t mram_addr = reinterpret_cast<uintptr_t>(emu[i].get_addr_of_symbol(symbol));
                const size_t xfer_bytes = buf.bytes_for_dpu(i);
                size_t left_xfer_bytes = xfer_bytes;

                sg_block_info block;
                for (block_id_t idx_block = 0; buf(&block, i, idx_block); idx_block++) {
                    assert(left_xfer_bytes >= block.length);
                    if constexpr (ToDPU)
                        std::memcpy(reinterpret_cast<void*>(mram_addr), block.addr, block.length);
                    else
                        std::memcpy(block.addr, reinterpret_cast<const void*>(mram_addr), block.length);
                    mram_addr += block.length;
                    left_xfer_bytes -= block.length;
                    total_effective_bytes += block.length;
                }
                max_xfer_bytes = std::max(max_xfer_bytes, xfer_bytes);
            }
        }
        total_xfer_bytes += max_xfer_bytes * NR_DPUS_IN_RANK;
    }
#ifdef MEASURE_XFER_BYTES
    xfer_statistics.add(symbol, total_xfer_bytes, total_effective_bytes);
#endif /* MEASURE_XFER_BYTES */
}

static void execute(const DPUSet& set)
{
    for (dpu_id_t i = 0; i < EMU_MAX_DPUS; i++)
        if (set[i])
            emu[i].execute();
    Emulation::wait_all();
}
