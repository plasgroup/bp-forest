#pragma once

#include "host_params.hpp"


#ifdef HOST_ONLY

#include <bitset>

typedef std::bitset<MAX_NR_DPUS> DPUSet;


#else /* HOST_ONLY */

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

#endif /* HOST_ONLY */


inline DPUSet all_dpu;
