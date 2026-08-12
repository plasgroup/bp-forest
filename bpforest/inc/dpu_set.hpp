#pragma once

#include "host_params.hpp"


#if defined(FAKE_DPU) || defined(DPU_ON_CPU)

#include <bitset>

typedef std::bitset<MAX_NR_DPUS> DPUSet;


#else /* FAKE_DPU */

extern "C" {
#include <dpu.h>
}

#include <variant>

struct DPUSetAll {
};
struct DPUSetRanks {
    dpu_id_t idx_rank_begin, idx_rank_end;
};
struct DPUSetSingle {
    dpu_id_t idx_dpu;
};
using DPUSet = std::variant<DPUSetAll, DPUSetRanks, DPUSetSingle>;

#endif /* FAKE_DPU */


inline DPUSet all_dpu;
