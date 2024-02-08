#ifdef HOST_ONLY

#include "emulation.hpp"
#include <bitset>

typedef std::bitset<EMU_MAX_DPUS> dpu_set_t;


#else /* HOST_ONLY */

extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}

#endif /* HOST_ONLY */
