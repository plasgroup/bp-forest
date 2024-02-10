#pragma once

#ifdef HOST_ONLY

#include "emulation.hpp"
#include <bitset>

typedef std::bitset<EMU_MAX_DPUS> DPUSet;


#else /* HOST_ONLY */

#include "host_params.hpp"

extern "C" {
#include <dpu.h>
}

struct DPUSet {
    dpu_id_t idx_begin, idx_end;
    dpu_set_t impl;
};

#endif /* HOST_ONLY */


static DPUSet all_dpu;
