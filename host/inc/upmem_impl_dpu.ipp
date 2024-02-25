#include "dpu_set.hpp"
#include "host_params.hpp"

extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <type_traits>


static_assert(NR_DPUS % NR_DPUS_IN_RANK == 0, "NR_DPUS % NR_DPUS_IN_RANK != 0");
constexpr size_t NR_RANKS = NR_DPUS / NR_DPUS_IN_RANK;

static dpu_set_t all_dpu_impl;
static std::array<dpu_set_t, NR_RANKS> each_rank_impl;
static std::array<dpu_set_t, NR_DPUS> each_dpu_impl;


static void upmem_init_impl(const char* binary, bool is_simulator)
{
    std::ostringstream sstr;
#if defined(HOST_MULTI_THREAD) && defined(QUERY_GATHER_XFER)
    sstr << "sgXferEnable=true,sgXferMaxBlocksPerDpu=" << HOST_MULTI_THREAD << ",";
#endif
    if (is_simulator) {
        sstr << "backend=simulator,";
    }
    std::string profile = sstr.str();
    if (!profile.empty()) {
        profile.pop_back();  // remove trailing comma
    }

    DPU_ASSERT(dpu_alloc(NR_DPUS, profile.c_str(), &all_dpu_impl));
    all_dpu.idx_begin = 0;
    all_dpu.idx_end = NR_DPUS;
    all_dpu.impl = all_dpu_impl;

    dpu_set_t rank, dpu;
    dpu_id_t idx_rank = 0, idx_dpu = 0;
    DPU_RANK_FOREACH(all_dpu_impl, rank)
    {
        each_rank_impl[idx_rank++] = rank;

        [[maybe_unused]] const auto last_dpu_in_prev_rank = idx_dpu;
        DPU_FOREACH(rank, dpu)
        {
            each_dpu_impl[idx_dpu++] = dpu;
        }
        assert(idx_dpu == last_dpu_in_prev_rank + NR_DPUS_IN_RANK);
    }

    DPU_ASSERT(dpu_load(all_dpu_impl, binary, NULL));
}
static void upmem_release_impl()
{
    DPU_ASSERT(dpu_free(all_dpu_impl));
}

static uint32_t nr_dpus_in_set(const DPUSet& set)
{
    uint32_t nr_dpus;
    DPU_ASSERT(dpu_get_nr_dpus(set.impl, &nr_dpus));
    return nr_dpus;
}

static void select_dpu(DPUSet* dst, dpu_id_t index)
{
    assert(index < NR_DPUS);
    dst->idx_begin = index;
    dst->idx_end = index + 1;
    dst->impl = each_dpu_impl[index];
}

template <bool ToDPU, class BatchTransferBuffer>
static void xfer_with_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf)
{
    constexpr dpu_xfer_t Direction = ToDPU ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    if (buf.IsSizeVarying) {
        const dpu_id_t idx_dpu_end = set.idx_end;
        for (dpu_id_t idx_dpu = set.idx_begin, idx_rank = idx_dpu / NR_DPUS_IN_RANK; idx_dpu < idx_dpu_end; idx_rank++) {
            const dpu_id_t idx_dpu_end_in_rank = (idx_rank + 1) * NR_DPUS_IN_RANK;
            size_t max_xfer_bytes_in_rank = 0;
            for (; idx_dpu < idx_dpu_end && idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                auto* const ptr = buf.for_dpu(idx_dpu);
                const auto size = buf.bytes_for_dpu(idx_dpu);
                static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                    "non-trivial copying cannot be performed between CPU and DPU");

                DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));
                max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, size);
            }
            DPU_ASSERT(dpu_push_xfer(each_rank_impl[idx_rank], Direction, symbol, 0, max_xfer_bytes_in_rank, DPU_XFER_ASYNC));
        }
        DPU_ASSERT(dpu_sync(set.impl));

    } else {
        for (dpu_id_t idx_dpu = set.idx_begin; idx_dpu < set.idx_end; idx_dpu++) {
            DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], buf.for_dpu(idx_dpu)));
        }
        DPU_ASSERT(dpu_push_xfer(set.impl, Direction, symbol, 0, buf.bytes_for_dpu(set.idx_begin), DPU_XFER_DEFAULT));
    }
}

template <typename T>
static void broadcast_to_dpu(const DPUSet& set, const char* symbol, const Single<T>& datum)
{
    DPU_ASSERT(dpu_broadcast_to(set.impl, symbol, 0, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_DEFAULT));
}

template <class F>
static bool get_block_func_wrapper(sg_block_info* out, uint32_t dpu_index, uint32_t block_index, void* impl)
{
    return (*reinterpret_cast<F*>(impl))(out, dpu_index, block_index);
}

template <bool ToDPU, class ScatteredBatchTransferBuffer>
static void scatter_gather_with_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf)
{
    static_assert(std::is_trivially_copyable_v<std::remove_reference_t<ScatteredBatchTransferBuffer>>,
        "callable object for scatter_gather_with_dpu should be trivially copyable");

    constexpr dpu_xfer_t Direction = ToDPU ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;
    assert(set.idx_begin % NR_DPUS_IN_RANK == 0);
    assert(set.idx_end % NR_DPUS_IN_RANK == 0);

    const dpu_id_t idx_dpu_end = set.idx_end;
    for (dpu_id_t idx_dpu = set.idx_begin, idx_rank = idx_dpu / NR_DPUS_IN_RANK; idx_dpu < idx_dpu_end; idx_rank++) {
        const dpu_id_t idx_dpu_end_in_rank = idx_dpu + NR_DPUS_IN_RANK;
        size_t max_xfer_bytes_in_rank = 0;
        for (; idx_dpu < idx_dpu_end && idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
            max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, buf.bytes_for_dpu(idx_dpu));
        }
        get_block_t get_block{&get_block_func_wrapper<std::remove_reference_t<ScatteredBatchTransferBuffer>>, &buf, sizeof(buf)};
        DPU_ASSERT(dpu_push_sg_xfer(each_rank_impl[idx_rank], Direction, symbol, 0, max_xfer_bytes_in_rank, &get_block,
            static_cast<dpu_sg_xfer_flags_t>(DPU_SG_XFER_DISABLE_LENGTH_CHECK | DPU_SG_XFER_ASYNC)));
    }
    DPU_ASSERT(dpu_sync(set.impl));
}

static void execute(const DPUSet& set)
{
    DPU_ASSERT(dpu_launch(set.impl, DPU_SYNCHRONOUS));
#ifdef PRINT_DEBUG
    dpu_set_t set_impl = set.impl, dpu;
    DPU_FOREACH(set_impl, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif /* PRINT_DEBUG */
}
