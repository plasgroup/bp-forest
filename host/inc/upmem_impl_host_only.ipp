#include "batch_transfer_buffer.hpp"
#include "dpu_set.hpp"
#include "emulation.hpp"
#include "host_params.hpp"
#include "sg_block_info.hpp"
#include "statistics.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>


static constexpr dpu_id_t NrDPUs = MAX_NR_DPUS;
static constexpr dpu_id_t NrDPUsInRank = MAX_NR_DPUS_IN_RANK;
Emulation emu[NrDPUs];


inline UPMEM_AsyncDuration::~UPMEM_AsyncDuration()
{
}

static inline void upmem_init_impl()
{
    all_dpu.reset();
    all_dpu.flip();

    for (dpu_id_t i = 0; i < NrDPUs; i++) {
        emu[i].init(i);
    }
}
static void upmem_release_impl()
{
    all_dpu.reset();
    Emulation::terminate();
}


static dpu_id_t nr_dpus_in_set(const DPUSet& set)
{
    return static_cast<dpu_id_t>(set.count());
}
dpu_id_t upmem_get_nr_dpus()
{
    return NrDPUs;
}
std::pair<dpu_id_t, dpu_id_t> upmem_get_dpu_range_in_rank(dpu_id_t idx_rank)
{
    return {NrDPUsInRank * idx_rank, NrDPUsInRank * (idx_rank + 1)};
}

static DPUSet select_dpu(dpu_id_t index)
{
    assert(index < upmem_get_nr_dpus());
    DPUSet result;
    result.set(index);
    return result;
}
static DPUSet select_rank(dpu_id_t index)
{
    assert(index < NR_RANKS);
    DPUSet result;
    for (dpu_id_t i = 0; i < NrDPUsInRank; i++) {
        result.set(index * NrDPUsInRank + i);
    }
    return result;
}


template <bool ToDPU, class BatchTransferBuffer>
static void xfer_with_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf, UPMEM_AsyncDuration&)
{
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

                void* mram_addr = emu[i].get_addr_of_symbol(symbol);
                if constexpr (ToDPU)
                    std::memcpy(mram_addr, ptr, size);
                else
                    std::memcpy(ptr, mram_addr, size);
                total_effective_bytes += size;
                max_xfer_bytes = std::max(max_xfer_bytes, size);
            }
        }
        total_xfer_bytes += max_xfer_bytes * NrDPUsInRank;
    }
#ifdef MEASURE_XFER_BYTES
    xfer_statistics.add(symbol, total_xfer_bytes, total_effective_bytes);
#endif /* MEASURE_XFER_BYTES */
}

template <typename T>
static void broadcast_to_dpu(const DPUSet& set, const char* symbol, const Single<T>& datum, UPMEM_AsyncDuration& async)
{
    xfer_with_dpu<true>(set, symbol, datum, async);
}

template <bool ToDPU, class ScatteredBatchTransferBuffer>
static void scatter_gather_with_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration&)
{
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (dpu_id_t i = 0; i < NrDPUs;) {
        size_t max_xfer_bytes = 0;
        for (dpu_id_t j = 0; i < NrDPUs && j < NrDPUsInRank; i++, j++) {
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
        total_xfer_bytes += max_xfer_bytes * NrDPUsInRank;
    }
#ifdef MEASURE_XFER_BYTES
    xfer_statistics.add(symbol, total_xfer_bytes, total_effective_bytes);
#endif /* MEASURE_XFER_BYTES */
}

static void execute(const DPUSet& set, UPMEM_AsyncDuration&)
{
    for (dpu_id_t i = 0; i < NrDPUs; i++)
        if (set[i])
            emu[i].execute();
    Emulation::wait_all();
}

template <class Func>
static void then_call(const DPUSet& set, Func& func, UPMEM_AsyncDuration& async)
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
