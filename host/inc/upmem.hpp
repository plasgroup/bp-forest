#ifndef __UPMEM_HPP__
#define __UPMEM_HPP__

#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"

#include <memory>

extern std::unique_ptr<dpu_requests_t[]> dpu_requests;
union DPUResultsUnion {
    dpu_get_results_t get[NR_DPUS];
    dpu_succ_results_t succ[NR_DPUS];
};
extern std::unique_ptr<DPUResultsUnion> dpu_results;
extern merge_info_t merge_info[NR_DPUS];
extern dpu_init_param_t dpu_init_param[NR_DPUS][NR_SEATS_IN_DPU];

void upmem_init(const char* binary, bool is_simulator);
void upmem_release(void);
uint32_t upmem_get_nr_dpus(void);

void upmem_send_task(const uint64_t task, BatchCtx& batch_ctx,
    float* send_time, float* exec_time);
void upmem_receive_get_results(BatchCtx& batch_ctx, float* receive_time);
void upmem_receive_succ_results(BatchCtx& batch_ctx, float* receive_time);
void upmem_receive_split_info(float* receive_time);
void upmem_receive_num_kvpairs(HostTree* host_tree, float* receive_time);
void upmem_send_nodes_from_dpu_to_dpu(uint32_t from_DPU, seat_id_t from_tree,
    uint32_t to_DPU, seat_id_t to_tree);

#endif /* __UPMEM_HPP__ */