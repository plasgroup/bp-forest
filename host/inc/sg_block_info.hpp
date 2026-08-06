#pragma once

#if defined(FAKE_DPU) || defined(DPU_ON_CPU)
#include <cstdint>

struct sg_block_info {
    uint8_t* addr;
    uint32_t length;
};

#else /* FAKE_DPU */
extern "C" {
#include <dpu.h>
}
#endif /* FAKE_DPU */
