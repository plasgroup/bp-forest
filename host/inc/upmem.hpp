#ifndef __UPMEM_HPP__
#define __UPMEM_HPP__

#include "batch_transfer_buffer.hpp"
#include "common.h"
#include "dpu_set.hpp"
#include "host_params.hpp"

#include <array>
#include <memory>
#include <utility>


void upmem_init(void);
void upmem_release(void);
dpu_id_t upmem_get_nr_dpus(void);
//! @return [first, last)
std::pair<dpu_id_t, dpu_id_t> upmem_get_dpu_range_in_rank(dpu_id_t idx_rank);


struct UPMEM_AsyncDuration;

inline DPUSet select_rank(dpu_id_t index);

template <bool ToDPU, class BatchTransferBuffer>
inline void xfer_with_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration&);
template <typename T>
inline void broadcast_to_dpu(const DPUSet& set, uint32_t offset, const Single<T>& datum, UPMEM_AsyncDuration&);
template <bool ToDPU, class ScatteredBatchTransferBuffer>
inline void scatter_gather_with_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration&);

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

inline void execute(const DPUSet& set, UPMEM_AsyncDuration&);

template <class Func>
inline void then_call(const DPUSet& set, Func&, UPMEM_AsyncDuration&);


#include "upmem.ipp"

#endif /* __UPMEM_HPP__ */
