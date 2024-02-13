#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#define HAS_SINGLE_BIT_UINT(n) ((n) != 0u && ((n) & ((n)-1u)) == 0u)

#define RSHIFT_OR(n, shift) ((n) | ((n) >> (shift)))
#define CEIL_TO_ONES_UINT32(n) (RSHIFT_OR(RSHIFT_OR(RSHIFT_OR(RSHIFT_OR(RSHIFT_OR((n), 1u), 2u), 4u), 8u), 16u))

#define BIT_CEIL_UINT32(n) (CEIL_TO_ONES_UINT32((n)-1u) + 1u)
#define BIT_FLOOR_UINT32(n) (CEIL_TO_ONES_UINT32(n) & ~(CEIL_TO_ONES_UINT32(n) >> 1u))

#define BITWIDTH_OF_POW2_UINT32(n)         \
    ((((n)&0xaaaaaaaau) != 0u) * 1u        \
        + (((n)&0xccccccccu) != 0u) * 2u   \
        + (((n)&0xf0f0f0f0u) != 0u) * 4u   \
        + (((n)&0xff00ff00u) != 0u) * 8u   \
        + (((n)&0xffff0000u) != 0u) * 16u) \
        + ((n) != 0u)

#define BITWIDTH_UINT32(n) (BITWIDTH_OF_POW2_UINT32(BIT_FLOOR_UINT32(n)))


#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus
