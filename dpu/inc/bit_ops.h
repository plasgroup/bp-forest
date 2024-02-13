#pragma once

#include <built_ins.h>

#include <stdint.h>


static inline uint32_t countl_zero_uint32(uint32_t n)
{
    __builtin_clz_rr(n, n);
    return n;
}

static inline uint32_t bit_ceil_uint32(uint32_t n)
{
    return n == 0u ? 1u : 1u << (32u - countl_zero_uint32(n - 1u));
}

static inline unsigned floor_log2_uint32(uint32_t n)
{
    return 31u - countl_zero_uint32(n);
}
