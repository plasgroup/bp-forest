#pragma once

#include "batch_transfer_buffer.hpp"
#include "common_params.h"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "log_buffer.hpp"
#include "upmem.hpp"

extern "C" {
#include <dpu.h>
#include <dpu_log.h>
#include <dpu_types.h>
}

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>
#include <variant>


inline dpu_set_t all_dpu_impl;
inline std::array<dpu_set_t, NR_RANKS> each_rank_impl;
inline std::array<dpu_set_t, MAX_NR_DPUS> each_dpu_impl;

inline std::array<dpu_id_t, NR_RANKS + 1> first_dpu_id_in_each_rank;

inline struct dpu_program_t* dpu_program_impl;
inline struct dpu_symbol_t comm_buffer_handler;


inline UPMEM_AsyncDuration::~UPMEM_AsyncDuration()
{
    if (all) {
auto status = dpu_sync(all_dpu_impl);
if (status != DPU_OK) {
    const auto log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
}
        // DPU_ASSERT(dpu_sync(all_dpu_impl));
    }
    for (dpu_id_t i = 0; i < NR_RANKS; i++) {
        if (rank[i]) {
auto status = dpu_sync(each_rank_impl[i]);
if (status != DPU_OK) {
    const auto log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
}
            // DPU_ASSERT(dpu_sync(each_rank_impl[i]));
        }
    }
}

inline void upmem_init_impl()
{
    std::ostringstream sstr;
    sstr <<
#ifdef UPMEM_SIMULATOR
        "backend=simulator,sgXferEnable=true,sgXferMaxBlocksPerDpu="
#else
        "sgXferEnable=true,sgXferMaxBlocksPerDpu="
#endif
         << std::max<size_t>({2 * MAX_NR_DPUS + 4, MAX_NR_SUMMARY_CHUNKS});

    DPU_ASSERT(dpu_alloc_ranks(NR_RANKS, sstr.str().c_str(), &all_dpu_impl));

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

#ifdef UPMEM_TRACE
    DPU_ASSERT(dpu_load(all_dpu_impl, DPU_BINARY_PATH, &dpu_program_impl));
#else
    extern dpu_incbin_t dpu_binary;
    DPU_ASSERT(dpu_load_from_incbin(all_dpu_impl, &dpu_binary, &dpu_program_impl));
#endif

    DPU_ASSERT(dpu_get_symbol(dpu_program_impl, DPU_MRAM_HEAP_POINTER_NAME, &comm_buffer_handler));
}
inline void upmem_release_impl()
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
inline dpu_id_t nr_dpus_in_set(const DPUSet& set)
{
    return std::visit(VisitorOf_nr_dpus_in_set{}, set);
}
inline dpu_id_t upmem_get_nr_dpus()
{
    return first_dpu_id_in_each_rank.back();
}
inline std::pair<dpu_id_t, dpu_id_t> upmem_get_dpu_range_in_rank(dpu_id_t idx_rank)
{
    return {first_dpu_id_in_each_rank[idx_rank], first_dpu_id_in_each_rank[idx_rank + 1]};
}

inline DPUSet select_dpu(dpu_id_t index)
{
    assert(index < upmem_get_nr_dpus());
    return {DPUSetSingle{index}};
}
inline DPUSet select_rank(dpu_id_t index)
{
    assert(index < NR_RANKS);
    return {DPUSetRanks{index, index + 1}};
}

template <bool ToDPU, class BatchTransferBuffer>
struct VisitorOf_xfer_with_dpu {
    uint32_t offset;
    BatchTransferBuffer&& buf;
    UPMEM_AsyncDuration& async;

    static constexpr dpu_xfer_t Direction = ToDPU ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    void operator()(const DPUSetAll&) const
    {
        if (buf.IsSizeVarying) {
            for (dpu_id_t idx_rank = 0, idx_dpu = 0; idx_rank < NR_RANKS; idx_rank++) {
                size_t max_xfer_bytes_in_rank = 0;
                const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
                for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                    const auto size = buf.bytes_for_dpu(idx_dpu);
                    if (size != 0) {
                        auto* const ptr = buf.for_dpu(idx_dpu);
                        static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                            "non-trivial copying cannot be performed between CPU and DPU");

                        DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));
                        max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, size);
                    }
                }
                if (max_xfer_bytes_in_rank != 0) {
                    DPU_ASSERT(dpu_push_xfer_symbol(each_rank_impl[idx_rank], Direction, comm_buffer_handler, offset, max_xfer_bytes_in_rank, DPU_XFER_ASYNC));
                    async.rank[idx_rank] = true;
                }
            }

        } else {
            const dpu_id_t idx_dpu_end = upmem_get_nr_dpus();
            for (dpu_id_t idx_dpu = 0; idx_dpu < idx_dpu_end; idx_dpu++) {
                DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], buf.for_dpu(idx_dpu)));
            }
            DPU_ASSERT(dpu_push_xfer_symbol(all_dpu_impl, Direction, comm_buffer_handler, offset, buf.bytes_for_dpu(0), DPU_XFER_ASYNC));
            async.all = true;
        }
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin, idx_dpu = first_dpu_id_in_each_rank[idx_rank]; idx_rank < ranks.idx_rank_end; idx_rank++) {
            size_t max_xfer_bytes_in_rank = 0;
            const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
            if (buf.IsSizeVarying) {
                for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                    const auto size = buf.bytes_for_dpu(idx_dpu);
                    if (size != 0) {
                        auto* const ptr = buf.for_dpu(idx_dpu);
                        static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                            "non-trivial copying cannot be performed between CPU and DPU");

                        DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));
                        max_xfer_bytes_in_rank = std::max(max_xfer_bytes_in_rank, size);
                    }
                }
                if (max_xfer_bytes_in_rank != 0) {
                    DPU_ASSERT(dpu_push_xfer_symbol(each_rank_impl[idx_rank], Direction, comm_buffer_handler, offset,
                        max_xfer_bytes_in_rank, DPU_XFER_ASYNC));
                    async.rank[idx_rank] = true;
                }
            } else {
                for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                    auto* const ptr = buf.for_dpu(idx_dpu);
                    static_assert(std::is_trivially_copyable_v<std::remove_pointer_t<decltype(ptr)>>,
                        "non-trivial copying cannot be performed between CPU and DPU");
                    DPU_ASSERT(dpu_prepare_xfer(each_dpu_impl[idx_dpu], ptr));
                }
                DPU_ASSERT(dpu_push_xfer_symbol(each_rank_impl[idx_rank], Direction, comm_buffer_handler, offset,
                    buf.bytes_for_dpu(idx_dpu_end_in_rank), DPU_XFER_ASYNC));
                async.rank[idx_rank] = true;
            }
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
        DPU_ASSERT(dpu_push_xfer_symbol(each_dpu_impl[idx_dpu], Direction, comm_buffer_handler, offset, size, DPU_XFER_DEFAULT));
    }
};
template <bool ToDPU, class BatchTransferBuffer>
inline void xfer_with_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    std::visit(VisitorOf_xfer_with_dpu<ToDPU, BatchTransferBuffer>{offset, std::forward<BatchTransferBuffer>(buf), async}, set);
}

template <typename T>
struct VisitorOf_broadcast_to_dpu {
    uint32_t offset;
    const Single<T>& datum;
    UPMEM_AsyncDuration& async;

    static_assert(std::is_trivially_copyable_v<T>, "non-trivial copying cannot be performed between CPU and DPU");

    void operator()(const DPUSetAll&) const
    {
        DPU_ASSERT(dpu_broadcast_to(all_dpu_impl, comm_buffer_handler, offset, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_ASYNC));
        async.all = true;
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            DPU_ASSERT(dpu_broadcast_to(each_rank_impl[idx_rank], comm_buffer_handler, offset, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_ASYNC));
            async.rank[idx_rank] = true;
        }
    }

    void operator()(const DPUSetSingle& dpu) const
    {
        DPU_ASSERT(dpu_broadcast_to(each_dpu_impl[dpu.idx_dpu], comm_buffer_handler, offset, datum.for_dpu(0), datum.bytes_for_dpu(0), DPU_XFER_DEFAULT));
    }
};
template <typename T>
inline void broadcast_to_dpu(const DPUSet& set, uint32_t offset, const Single<T>& datum, UPMEM_AsyncDuration& async)
{
    std::visit(VisitorOf_broadcast_to_dpu<T>{offset, datum, async}, set);
}

template <bool ToDPU, class ScatteredBatchTransferBuffer>
struct VisitorOf_scatter_gather_with_dpu {
    uint32_t offset;

    ScatteredBatchTransferBuffer&& buf;
    static_assert(std::is_trivially_copyable_v<std::remove_reference_t<ScatteredBatchTransferBuffer>>,
        "callable object for scatter_gather_with_dpu should be trivially copyable");

    UPMEM_AsyncDuration& async;

    static constexpr dpu_xfer_t Direction = ToDPU ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    static bool get_block_func_wrapper(sg_block_info* out, uint32_t dpu_index, uint32_t block_index, void* impl)
    {
        return (*reinterpret_cast<std::remove_reference_t<ScatteredBatchTransferBuffer>*>(impl))(out, dpu_index, block_index);
    }

    void operator()(const DPUSetAll&) const
    {
        for (dpu_id_t idx_rank = 0, idx_dpu = 0; idx_rank < NR_RANKS; idx_rank++) {
            size_t max_xfer_bytes = 0;
            const dpu_id_t idx_dpu_end_in_rank = first_dpu_id_in_each_rank[idx_rank + 1];
            for (; idx_dpu < idx_dpu_end_in_rank; idx_dpu++) {
                max_xfer_bytes = std::max(max_xfer_bytes, buf.bytes_for_dpu(idx_dpu));
            }
            get_block_t get_block{&get_block_func_wrapper, &buf, sizeof(buf)};
            DPU_ASSERT(dpu_push_sg_xfer_symbol(each_rank_impl[idx_rank], Direction, comm_buffer_handler, offset, (max_xfer_bytes + 7) / 8 * 8, &get_block,
                static_cast<dpu_sg_xfer_flags_t>(DPU_SG_XFER_DISABLE_LENGTH_CHECK | DPU_SG_XFER_ASYNC)));
            async.rank[idx_rank] = true;
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
            DPU_ASSERT(dpu_push_sg_xfer_symbol(each_rank_impl[idx_rank], Direction, comm_buffer_handler, offset, (max_xfer_bytes_in_rank + 7) / 8 * 8, &get_block,
                static_cast<dpu_sg_xfer_flags_t>(DPU_SG_XFER_DISABLE_LENGTH_CHECK | DPU_SG_XFER_ASYNC)));
            async.rank[idx_rank] = true;
        }
    }

    void operator()(const DPUSetSingle&) const
    {
        std::cerr << "scattered xfer to one DPU is not supported" << std::endl;
        DPU_ASSERT(DPU_ERR_INTERNAL);
    }
};
template <bool ToDPU, class ScatteredBatchTransferBuffer>
inline void scatter_gather_with_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    std::visit(VisitorOf_scatter_gather_with_dpu<ToDPU, ScatteredBatchTransferBuffer>{offset, std::forward<ScatteredBatchTransferBuffer>(buf), async}, set);
}

struct VisitorOf_execute {
    UPMEM_AsyncDuration& async;

    void operator()(const DPUSetAll&) const
    {
        DPU_ASSERT(dpu_launch(all_dpu_impl, DPU_ASYNCHRONOUS));
        async.all = true;
    }

    void operator()(const DPUSetRanks& ranks_) const
    {
        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            DPU_ASSERT(dpu_launch(each_rank_impl[idx_rank], DPU_ASYNCHRONOUS));
            async.rank[idx_rank] = true;
        }
    }

    void operator()(const DPUSetSingle& d) const
    {
        dpu_set_t dpu_impl = each_dpu_impl[d.idx_dpu];
        DPU_ASSERT(dpu_launch(dpu_impl, DPU_SYNCHRONOUS));
    }
};
inline void execute(const DPUSet& set, UPMEM_AsyncDuration& async)
{
    std::visit(VisitorOf_execute{async}, set);
}

struct VisitorOf_read_log {
    std::unique_ptr<LogBuffer> operator()(const DPUSetAll&) const
    {
        LogStream stream;

        dpu_set_t dpu;
        DPU_FOREACH(all_dpu_impl, dpu)
        {
            DPU_ASSERT(dpu_log_read(dpu, stream.get()));
        }

        return std::move(stream).close();
    }

    std::unique_ptr<LogBuffer> operator()(const DPUSetRanks& ranks_) const
    {
        LogStream stream;

        const DPUSetRanks ranks = ranks_;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            dpu_set_t dpu;
            DPU_FOREACH(each_rank_impl[idx_rank], dpu)
            {
                DPU_ASSERT(dpu_log_read(dpu, stream.get()));
            }
        }

        return std::move(stream).close();
    }

    std::unique_ptr<LogBuffer> operator()(const DPUSetSingle& d) const
    {
        LogStream stream;
        DPU_ASSERT(dpu_log_read(each_dpu_impl[d.idx_dpu], stream.get()));
        return std::move(stream).close();
    }
};
inline std::unique_ptr<LogBuffer> read_log(const DPUSet& set)
{
    return std::visit(VisitorOf_read_log{}, set);
}

template <class Func>
struct VisitorOf_then_call {
    Func& func;
    UPMEM_AsyncDuration& async;
    dpu_id_t nr_lives;

    VisitorOf_then_call(Func& func, UPMEM_AsyncDuration& async) : func{func}, async{async} {}

    static dpu_error_t callback_wrapper(struct dpu_set_t, uint32_t rank_id, void* impl)
    {
        VisitorOf_then_call* const self = reinterpret_cast<VisitorOf_then_call*>(impl);
        (self->func)(rank_id, self->async);

        --self->nr_lives;
        if (self->nr_lives == 0) {
            delete self;
        }
        return DPU_OK;
    }
    void operator()(const DPUSetAll&)
    {
        nr_lives = NR_RANKS;
        async.all = true;
        DPU_ASSERT(dpu_callback(all_dpu_impl, &callback_wrapper, this, DPU_CALLBACK_ASYNC));
    }

    void operator()(const DPUSetRanks& ranks_)
    {
        const DPUSetRanks ranks = ranks_;
        nr_lives = ranks.idx_rank_end - ranks.idx_rank_begin;
        for (dpu_id_t idx_rank = ranks.idx_rank_begin; idx_rank < ranks.idx_rank_end; idx_rank++) {
            async.rank[idx_rank] = true;
            DPU_ASSERT(dpu_callback(each_rank_impl[idx_rank], &callback_wrapper, this, DPU_CALLBACK_ASYNC));
        }
    }

    void operator()(const DPUSetSingle&) const
    {
        std::cerr << "callback bound to one DPU is not supported" << std::endl;
        DPU_ASSERT(DPU_ERR_INTERNAL);
        delete this;
    }
};
template <class Func>
inline void then_call(const DPUSet& set, Func& func, UPMEM_AsyncDuration& async)
{
    std::visit(*(new VisitorOf_then_call<Func>{func, async}), set);
}

inline std::unique_ptr<char[]> get_param_dump()
{
    uint64_t param_dump_size;
    DPU_ASSERT(dpu_copy_from(each_dpu_impl[0], "ParamDumpSize", 0, &param_dump_size, sizeof(uint64_t)));

    std::unique_ptr<char[]> result{new char[param_dump_size]};
    DPU_ASSERT(dpu_copy_from(each_dpu_impl[0], "ParamDump", 0, &result[0], param_dump_size));

    return result;
}
