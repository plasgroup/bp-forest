#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <built_ins.h>: the wrappers the DPU
//! program uses, with the DPU instructions' semantics and the SDK's
//! (destination, source) operand order.

#include "dpu_cpu.h"

#include <stdint.h>


//! @return number of leading zeros of x; 32 if x == 0 (the clz instruction's
//!         semantics, where the compiler builtin would be undefined)
static inline uint32_t dpu_cpu_clz32(uint32_t x)
{
    uint32_t n = 0;
    for (uint32_t bit = UINT32_C(1) << 31; bit != 0 && (x & bit) == 0; bit >>= 1) {
        n++;
    }
    return n;
}

static inline uint32_t dpu_cpu_popcount32(uint32_t x)
{
    uint32_t n = 0;
    for (; x != 0; x >>= 1) {
        n += x & 1u;
    }
    return n;
}

//! @return the value of the decimal string literal the SDK builtins take as
//!         immediate operand (e.g. "12")
static inline uint32_t dpu_cpu_imm(const char* s)
{
    uint32_t v = 0;
    for (; *s != '\0'; s++) {
        v = v * 10u + (uint32_t)(*s - '0');
    }
    return v;
}


#define __builtin_clz_rr(rc, ra) ((rc) = dpu_cpu_clz32((uint32_t)(ra)))

#define __builtin_cao_rr(rc, ra) ((rc) = dpu_cpu_popcount32((uint32_t)(ra)))

/* the lsr instruction uses only the low 5 bits of the shift amount
 * (shift:u5); the mask reproduces that and avoids UB for shifts >= 32 */
#define __builtin_lsr_rri(rc, ra, shift) ((rc) = (uint32_t)(ra) >> (dpu_cpu_imm("" shift) & 31u))
