#pragma once

#ifdef HOST_ONLY
#include <cstdint>

struct sg_block_info {
    uint8_t *addr;
    uint32_t length;
};

#else /* HOST_ONLY */
extern "C" {
#include <dpu.h>
}
#endif /* HOST_ONLY */
