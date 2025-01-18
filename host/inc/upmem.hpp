#pragma once

#include "batch_transfer_buffer.hpp"
#include "common.h"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "log_buffer.hpp"

#include <array>
#include <memory>
#include <utility>


#include <mutex>
inline std::mutex cout_mtx;  // TODO: delete

inline void upmem_init(void);
inline void upmem_release(void);
inline dpu_id_t nr_dpus_in_set(const DPUSet& set);
inline dpu_id_t upmem_get_nr_dpus(void);
//! @return [first, last)
inline std::pair<dpu_id_t, dpu_id_t> upmem_get_dpu_range_in_rank(dpu_id_t idx_rank);


struct UPMEM_AsyncDuration;

inline DPUSet select_dpu(dpu_id_t index);
inline DPUSet select_rank(dpu_id_t index);

template <bool ToDPU, class BatchTransferBuffer>
inline void xfer_with_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration&);
template <typename T>
inline void broadcast_to_dpu(const DPUSet& set, uint32_t offset, const Single<T>& datum, UPMEM_AsyncDuration&);
template <bool ToDPU, class ScatteredBatchTransferBuffer>
inline void scatter_gather_with_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration&);

template <class BatchTransferBuffer>
inline void send_to_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async);
template <class BatchTransferBuffer>
inline void recv_from_dpu(const DPUSet& set, uint32_t offset, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async);

template <class ScatteredBatchTransferBuffer>
inline void gather_to_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async);
template <class ScatteredBatchTransferBuffer>
inline void scatter_from_dpu(const DPUSet& set, uint32_t offset, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async);

inline void execute(const DPUSet& set, UPMEM_AsyncDuration&);
#ifndef HOST_ONLY
inline std::unique_ptr<LogBuffer> read_log(const DPUSet& set);
#endif

template <class Func>
inline void then_call(const DPUSet& set, Func&, UPMEM_AsyncDuration&);

#ifndef HOST_ONLY
inline std::unique_ptr<char[]> get_param_dump();
#endif


#include "upmem.ipp"
