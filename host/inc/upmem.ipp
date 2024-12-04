#pragma once

#include "upmem.hpp"

#include "dpu_set.hpp"
#include "host_params.hpp"

#ifndef HOST_ONLY
#include <array>
#endif

#ifdef PRINT_DEBUG
#include <cstdio>
#endif /* PRINT_DEBUG */


struct UPMEM_AsyncDuration {
    UPMEM_AsyncDuration() = default;
    ~UPMEM_AsyncDuration();

    UPMEM_AsyncDuration(const UPMEM_AsyncDuration&) = delete;
    UPMEM_AsyncDuration& operator=(const UPMEM_AsyncDuration&) = delete;

#ifndef HOST_ONLY
    bool all{};
    std::array<bool, NR_RANKS> rank{};
#endif
};

inline void upmem_init_impl();
inline void upmem_release_impl();


#ifdef HOST_ONLY
#include "upmem_impl_host_only.ipp"
#else
#include "upmem_impl_dpu.ipp"
#endif


//
//  UPMEM module interface
//
inline void upmem_init()
{
    upmem_init_impl();

#ifdef PRINT_DEBUG
    const dpu_id_t nr_dpus = upmem_get_nr_dpus();
    std::printf("Allocated %d DPU(s)\n", nr_dpus);
#endif /* PRINT_DEBUG */
}

inline void upmem_release()
{
    upmem_release_impl();
}

template <class BatchTransferBuffer>
inline void send_to_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    xfer_with_dpu<true>(set, offset, std::forward<BatchTransferBuffer>(buf), async);
}
template <class BatchTransferBuffer>
inline void recv_from_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    xfer_with_dpu<false>(set, offset, std::forward<BatchTransferBuffer>(buf), async);
}

template <class ScatteredBatchTransferBuffer>
inline void gather_to_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    scatter_gather_with_dpu<true>(set, offset, std::forward<ScatteredBatchTransferBuffer>(buf), async);
}
template <class ScatteredBatchTransferBuffer>
inline void scatter_from_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    scatter_gather_with_dpu<false>(set, offset, std::forward<ScatteredBatchTransferBuffer>(buf), async);
}
