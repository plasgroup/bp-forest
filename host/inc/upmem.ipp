#include "upmem.hpp"

#include "batch_transfer_buffer.hpp"
#include "common.h"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "scattered_batch_transfer_buffer.hpp"
#include "statistics.hpp"
#include "utils.hpp"
#include "workload_types.h"

#include <sys/time.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>

#ifdef PRINT_DEBUG
#include <cstdio>
#endif /* PRINT_DEBUG */


struct UPMEM_AsyncDuration {
    ~UPMEM_AsyncDuration();

#ifndef HOST_ONLY
    bool all{};
    std::array<bool, NR_RANKS> rank{};
#endif
};

static void upmem_init_impl();
static void upmem_release_impl();

[[maybe_unused]] static dpu_id_t nr_dpus_in_set(const DPUSet& set);
dpu_id_t upmem_get_nr_dpus();
[[maybe_unused]] static DPUSet select_dpu(dpu_id_t index);

#ifdef HOST_ONLY
#include "upmem_impl_host_only.ipp"
#else
#include "upmem_impl_dpu.ipp"
#endif


//
//  UPMEM module interface
//
void upmem_init()
{
    upmem_init_impl();

#ifdef PRINT_DEBUG
    const dpu_id_t nr_dpus = upmem_get_nr_dpus();
    std::printf("Allocated %d DPU(s)\n", nr_dpus);
#endif /* PRINT_DEBUG */
}

void upmem_release()
{
    upmem_release_impl();
}


#ifdef PRINT_DEBUG
[[maybe_unused]] static const char* task_name(uint64_t task)
{
    switch (TASK_GET_ID(task)) {
    case TASK_INIT:
        return "INIT";
    case TASK_GET:
        return "GET";
    case TASK_PRED:
        return "PRED";
    case TASK_SCAN:
        return "SCAN";
    case TASK_INSERT:
        return "INSERT";
    case TASK_DELETE:
        return "DELETE";
    case TASK_SUMMARIZE:
        return "SUMMARIZE";
    case TASK_EXTRACT:
        return "EXTRACT";
    case TASK_CONSTRUCT_HOT:
        return "CONSTRUCT_HOT";
    case TASK_FLATTEN_HOT:
        return "FLATTEN_HOT";
    case TASK_RESTORE:
        return "RESTORE";
    default:
        return "unknown-task";
    }
}
#endif /* PRINT_DEBUG */
