#pragma once

#include <built_ins.h>

#include <stdint.h>


static inline uint32_t countl_zero_uint32(uint32_t n)
{
    __builtin_clz_rr(n, n);
    return n;
}
static inline uint32_t countl_zero_uint64(uint64_t n)
{
    //  __asm__("clz %[r], %[hi], nmax, 1f\n\t"
    //          "clz %[r], %[lo]\n\t"
    //          "add %[r], %[r], 32\n"
    //          "1:"
    //          : [r] "=&r"(r)
    //          : [hi] "r"(hi), [lo] "r"(lo));
    const uint32_t hi = (uint32_t)(n >> 32);
    return (hi != 0 ? countl_zero_uint32(hi) : 32 + countl_zero_uint32((uint32_t)n));
}

static inline uint32_t bit_ceil_uint32(uint32_t n)
{
    return n == 0u ? 1u : 1u << (32u - countl_zero_uint32(n - 1u));
}

static inline unsigned floor_log2_uint32(uint32_t n)
{
    return 31u - countl_zero_uint32(n);
}
