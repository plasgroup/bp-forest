#include "batch_transfer_buffer.hpp"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "upmem.hpp"

extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <type_traits>
#include <utility>


static dpu_set_t all_dpu_impl;
static std::array<dpu_set_t, NR_RANKS> each_rank_impl;
static std::array<dpu_set_t, MAX_NR_DPUS> each_dpu_impl;

static std::array<dpu_id_t, NR_RANKS + 1> first_dpu_id_in_each_rank;


UPMEM_AsyncDuration::~UPMEM_AsyncDuration()
{
    DPU_ASSERT(dpu_sync(all_dpu_impl));
}

static void upmem_init_impl()
{
    // clang-format off
    constexpr const char* profile =
#if defined(HOST_MULTI_THREAD)
#  define STRINGFY_IMPL_(x) #x
#  define STRINGFY(x) STRINGFY_IMPL_(x)
#  ifdef UPMEM_SIMULATOR
        "backend=simulator,sgXferEnable=true,sgXferMaxBlocksPerDpu=" STRINGFY(HOST_MULTI_THREAD);
#  else
        "sgXferEnable=true,sgXferMaxBlocksPerDpu=" STRINGFY(HOST_MULTI_THREAD);
#  endif
#  undef STRINGFY
#  undef STRINGFY_IMPL_
#else
#  ifdef UPMEM_SIMULATOR
        "backend=simulator";
#  else
        NULL;
#  endif
#endif
    // clang-format on

    DPU_ASSERT(dpu_alloc_ranks(NR_RANKS, profile, &all_dpu_impl));

    dpu_set_t rank, dpu;
    dpu_id_t idx_rank = 0, idx_dpu = 0;
    DPU_RANK_FOREACH(all_dpu_impl, rank)
    {
        first_dpu_id_in_each_rank[idx_rank] = idx_dpu;
        each_rank_impl[idx_rank++] = rank;

        DPU_FOREACH(rank, dpu)
        {
            each_dpu_impl[idx_dpu++] = dpu;
        }
    }
    first_dpu_id_in_each_rank.back() = idx_dpu;

    extern dpu_incbin_t dpu_binary;
    DPU_ASSERT(dpu_load_from_incbin(all_dpu_impl, &dpu_binary, NULL));
}
static void upmem_release_impl()
{
    DPU_ASSERT(dpu_free(all_dpu_impl));
}

struct VisitorOf_nr_dpus_in_set {
    dpu_id_t operator()(const DPUSetAll&) const
    {
        return upmem_get_nr_dpus();
    }
    dpu_id_t operator()(const DPUSetRanks& ranks) const
    {
        return first_dpu_id_in_each_rank[ranks.idx_rank_end] - first_dpu_id_in_each_rank[ranks.idx_rank_begin];
    }
    dpu_id_t operator()(const DPUSetSingle&) const
    {
        return 1;
    }
};
static dpu_id_t nr_dpus_in_set(const DPUSet& set)
{
    return std::visit(VisitorOf_nr_dpus_in_set{}, set);
}
dpu_id_t upmem_get_nr_dpus()
{
    return first_dpu_id_in_each_rank.back();
}
std::pair<dpu_id_t, dpu_id_t> upmem_get_dpu_range_in_rank(dpu_id_t idx_rank)
{
    return {first_dpu_id_in_each_rank[idx_rank], first_dpu_id_in_each_rank[idx_rank + 1]};
}

static DPUSet select_dpu(dpu_id_t index)
{
    assert(index < upmem_get_nr_dpus());
    return {DPUSetSingle{index}};
}
static DPUSet select_rank(dpu_id_t index)
{
    assert(index < NR_RANKS);
    return {DPUSetRanks{index, index + 1}};
}

template <bool ToDPU, class BatchTransferBuffer>
struct VisitorOf_xfer_with_dpu {
    const char* symbol;
    BatchTransferBuffer&& buf;

    static constexpr dpu_xfer_t Direction = ToDPU ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    void operator()(const DPUSetAll&) const
    {
        if (buf.IsSizeVarying) {
            for (dpu_id_t idx_rank = 0, idx_dpu = 0; idx_rank < NR_RANKS; idx_rank++) {
                size_t max_xfer_bytes_in_rank = 0;
                const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
                for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                    auto* const ptr = buf.for_dpu(idx_dpu);
                    const auto size = buf.bytes_for_dpu(idx_dpu);
                    static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                        "non-trivial copying cannot be performed between CPU and DPU");

                    DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));
                    max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, size);
                }
                DPU_ASSERT(dpu_push_xfer(each_rank_impl[idx_rank], Direction, symbol, 0, max_xfer_bytes_in_rank, DPU_XFER_ASYNC));
            }

        } else {
            const dpu_id_t idx_dpu_end = upmem_get_nr_dpus();
            for (dpu_id_t idx_dpu = 0; idx_dpu < idx_dpu_end; idx_dpu++) {
                DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], buf.for_dpu(idx_dpu)));
            }
            DPU_ASSERT(dpu_push_xfer(all_dpu_impl, Direction, symbol, 0, buf.bytes_for_dpu(0), DPU_XFER_ASYNC));
        }
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin, idx_dpu = first_dpu_id_in_each_rank[idx_rank]; idx_rank < ranks.idx_rank_end; idx_rank++) {
            size_t max_xfer_bytes_in_rank = 0;
            const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
            for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                auto* const ptr = buf.for_dpu(idx_dpu);
                static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                    "non-trivial copying cannot be performed between CPU and DPU");
                DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));

                if (buf.IsSizeVarying) {
                    const auto size = buf.bytes_for_dpu(idx_dpu);
                    max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, size);
                }
            }
            DPU_ASSERT(dpu_push_xfer(each_rank_impl[idx_rank], Direction, symbol, 0,
                (buf.IsSizeVarying ? max_xfer_bytes_in_rank : buf.bytes_for_dpu(idx_dpu_end_in_rank)), DPU_XFER_ASYNC));
        }
    }

    void operator()(const DPUSetSingle& dpu) const
    {
        const dpu_id_t& idx_dpu = dpu.idx_dpu;
        auto* const ptr = buf.for_dpu(idx_dpu);
        const auto size = buf.bytes_for_dpu(idx_dpu);
        static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
            "non-trivial copying cannot be performed between CPU and DPU");
        DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));
        DPU_ASSERT(dpu_push_xfer(each_dpu_impl[idx_dpu], Direction, symbol, 0, size, DPU_XFER_ASYNC));
    }
};
template <bool ToDPU, class BatchTransferBuffer>
static void xfer_with_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf, UPMEM_AsyncDuration&)
{
    std::visit(VisitorOf_xfer_with_dpu<ToDPU, BatchTransferBuffer>{symbol, std::forward<BatchTransferBuffer>(buf)}, set);
}

template <typename T>
struct VisitorOf_broadcast_to_dpu {
    const char* symbol;
    const Single<T>& datum;

    static_assert(std::is_trivially_copyable_v<T>, "non-trivial copying cannot be performed between CPU and DPU");

    void operator()(const DPUSetAll&) const
    {
        DPU_ASSERT(dpu_broadcast_to(all_dpu_impl, symbol, 0, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_ASYNC));
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            DPU_ASSERT(dpu_broadcast_to(each_rank_impl[idx_rank], symbol, 0, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_ASYNC));
        }
    }

    void operator()(const DPUSetSingle& dpu) const
    {
        DPU_ASSERT(dpu_broadcast_to(each_dpu_impl[dpu.idx_dpu], symbol, 0, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_ASYNC));
    }
};
template <typename T>
static void broadcast_to_dpu(const DPUSet& set, const char* symbol, const Single<T>& datum, UPMEM_AsyncDuration&)
{
    std::visit(VisitorOf_broadcast_to_dpu<T>{symbol, datum}, set);
}

template <bool ToDPU, class ScatteredBatchTransferBuffer>
struct VisitorOf_scatter_gather_with_dpu {
    const char* symbol;

    ScatteredBatchTransferBuffer&& buf;
    static_assert(std::is_trivially_copyable_v<std::remove_reference_t<ScatteredBatchTransferBuffer>>,
        "callable object for scatter_gather_with_dpu should be trivially copyable");

    static constexpr dpu_xfer_t Direction = ToDPU ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    static bool get_block_func_wrapper(sg_block_info* out, uint32_t dpu_index, uint32_t block_index, void* impl)
    {
        return (*reinterpret_cast<std::remove_reference_t<ScatteredBatchTransferBuffer>*>(impl))(out, dpu_index, block_index);
    }

    void operator()(const DPUSetAll&) const
    {
        for (dpu_id_t idx_rank = 0, idx_dpu = 0; idx_rank < NR_RANKS; idx_rank++) {
            size_t max_xfer_bytes_in_rank = 0;
            const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
            for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, buf.bytes_for_dpu(idx_dpu));
            }
            get_block_t get_block{&get_block_func_wrapper, &buf, sizeof(buf)};
            DPU_ASSERT(dpu_push_sg_xfer(each_rank_impl[idx_rank], Direction, symbol, 0, max_xfer_bytes_in_rank, &get_block,
                static_cast<dpu_sg_xfer_flags_t>(DPU_SG_XFER_DISABLE_LENGTH_CHECK | DPU_SG_XFER_ASYNC)));
        }
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin, idx_dpu = first_dpu_id_in_each_rank[idx_rank]; idx_rank < ranks.idx_rank_end; idx_rank++) {
            size_t max_xfer_bytes_in_rank = 0;
            const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
            for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, buf.bytes_for_dpu(idx_dpu));
            }
            get_block_t get_block{&get_block_func_wrapper, &buf, sizeof(buf)};
            DPU_ASSERT(dpu_push_sg_xfer(each_rank_impl[idx_rank], Direction, symbol, 0, max_xfer_bytes_in_rank, &get_block,
                static_cast<dpu_sg_xfer_flags_t>(DPU_SG_XFER_DISABLE_LENGTH_CHECK | DPU_SG_XFER_ASYNC)));
        }
    }

    void operator()(const DPUSetSingle&) const
    {
        std::cerr << "scattered xfer to one DPU is not supported" << std::endl;
        DPU_ASSERT(DPU_ERR_INTERNAL);
    }
};
template <bool ToDPU, class ScatteredBatchTransferBuffer>
static void scatter_gather_with_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration&)
{
    std::visit(VisitorOf_scatter_gather_with_dpu<ToDPU, ScatteredBatchTransferBuffer>{symbol, std::forward<ScatteredBatchTransferBuffer>(buf)}, set);
}

struct VisitorOf_execute {
    static dpu_error_t bypass_logs(struct dpu_set_t set, uint32_t /* rank_id */, void* /* arg */)
    {
        dpu_set_t dpu;
        DPU_FOREACH(set, dpu)
        {
            DPU_ASSERT(dpu_log_read(dpu, stdout));
        }
        return DPU_OK;
    }

    void operator()(const DPUSetAll&) const
    {
        DPU_ASSERT(dpu_launch(all_dpu_impl, DPU_ASYNCHRONOUS));
#ifdef PRINT_DEBUG
        DPU_ASSERT(dpu_callback(all_dpu_impl, &bypass_logs, nullptr, DPU_CALLBACK_ASYNC));
#endif /* PRINT_DEBUG */
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            DPU_ASSERT(dpu_launch(each_rank_impl[idx_rank], DPU_ASYNCHRONOUS));
#ifdef PRINT_DEBUG
            DPU_ASSERT(dpu_callback(each_rank_impl[idx_rank], &bypass_logs, nullptr, DPU_CALLBACK_ASYNC));
#endif /* PRINT_DEBUG */
        }
    }

    void operator()(const DPUSetSingle& d) const
    {
        dpu_set_t dpu_impl = each_dpu_impl[d.idx_dpu];
        DPU_ASSERT(dpu_launch(dpu_impl, DPU_SYNCHRONOUS));
#ifdef PRINT_DEBUG
        DPU_ASSERT(dpu_callback(dpu_impl, &bypass_logs, nullptr, DPU_CALLBACK_ASYNC));
#endif /* PRINT_DEBUG */
    }
};
static void execute(const DPUSet& set, UPMEM_AsyncDuration&)
{
    std::visit(VisitorOf_execute{}, set);
}

template <class Func>
struct VisitorOf_then_call {
    Func& func;

    static UPMEM_AsyncDuration& get_async_duration()
    {
        alignas(UPMEM_AsyncDuration) static std::byte buf[sizeof(UPMEM_AsyncDuration)];
        static UPMEM_AsyncDuration& result = *(new (buf) UPMEM_AsyncDuration);
        return result;
    }

    static dpu_error_t callback_wrapper(struct dpu_set_t, uint32_t rank_id, void* impl)
    {
        (*reinterpret_cast<Func*>(impl))(rank_id, get_async_duration());
        return DPU_OK;
    }
    void operator()(const DPUSetAll&) const
    {
        DPU_ASSERT(dpu_callback(all_dpu_impl, &callback_wrapper, &func, DPU_CALLBACK_ASYNC));
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            DPU_ASSERT(dpu_callback(each_rank_impl[idx_rank], &callback_wrapper, &func, DPU_CALLBACK_ASYNC));
        }
    }

    void operator()(const DPUSetSingle&) const
    {
        std::cerr << "callback bound to one DPU is not supported" << std::endl;
        DPU_ASSERT(DPU_ERR_INTERNAL);
    }
};
template <class Func>
static void then_call(const DPUSet& set, Func& func, UPMEM_AsyncDuration&)
{
    std::visit(VisitorOf_then_call<Func>{func}, set);
}
