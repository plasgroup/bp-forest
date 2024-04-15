#ifndef __UPMEM_HPP__
#define __UPMEM_HPP__

#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"

#include <memory>

extern std::unique_ptr<dpu_requests_t[]> dpu_requests;
union DPUResultsUnion {
    dpu_get_results_t get[MAX_NR_DPUS];
    dpu_succ_results_t succ[MAX_NR_DPUS];
};
extern std::unique_ptr<DPUResultsUnion> dpu_results;
extern dpu_init_param_t dpu_init_param[MAX_NR_DPUS];

void upmem_init(void);
void upmem_release(void);
dpu_id_t upmem_get_nr_dpus(void);

void upmem_send_task(const uint64_t task, BatchCtx& batch_ctx,
    float* send_time, float* exec_time);
void upmem_receive_get_results(BatchCtx& batch_ctx, float* receive_time);
void upmem_receive_succ_results(BatchCtx& batch_ctx, float* receive_time);
void upmem_receive_num_kvpairs(HostTree* host_tree, float* receive_time);
void upmem_migrate_kvpairs(std::array<migration_ratio_param_t, MAX_NR_DPUS>& plan);

#endif /* __UPMEM_HPP__ */