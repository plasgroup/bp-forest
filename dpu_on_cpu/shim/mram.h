#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <mram.h>: MRAM-WRAM transfers become
//! plain memcpy, asserting the real DMA constraints so that a violation that
//! silently corrupts data on the device aborts here (asserts vanish in
//! NDEBUG builds).
//!
//! Divergence: sub-8-byte direct stores through __mram_ptr are 8-byte
//! read-modify-write on the device, so concurrent writes to different fields
//! of one 8-byte word can lose an update there but not here.

#include "attributes.h"
#include "dpu_cpu.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>


#define DPU_MRAM_HEAP_POINTER ((__mram_ptr void*)dpu_cpu_mram_heap)


static inline void dpu_cpu_check_dma(const void* mram, const void* wram, unsigned int nb_of_bytes)
{
    assert(nb_of_bytes != 0 && "DMA size must not be zero");
    assert(nb_of_bytes <= 2048 && "DMA size must be at most 2048");
    assert(nb_of_bytes % 8 == 0 && "DMA size must be a multiple of 8");
    assert((uintptr_t)mram % 8 == 0 && "MRAM address must be 8-byte aligned");
    assert((uintptr_t)wram % 8 == 0 && "WRAM address must be 8-byte aligned");
    /* on the device the two operands live in different memories, so an
     * overlap can only be a confusion of an MRAM address with a WRAM one */
    assert(((uintptr_t)mram + nb_of_bytes <= (uintptr_t)wram || (uintptr_t)wram + nb_of_bytes <= (uintptr_t)mram)
           && "MRAM and WRAM operands must not overlap");
    (void)mram;
    (void)wram;
    (void)nb_of_bytes;
}

static inline void mram_read(const __mram_ptr void* from, void* to, unsigned int nb_of_bytes)
{
    dpu_cpu_check_dma(from, to, nb_of_bytes);
    memcpy(to, from, nb_of_bytes);
}

static inline void mram_write(const void* from, __mram_ptr void* to, unsigned int nb_of_bytes)
{
    dpu_cpu_check_dma(to, from, nb_of_bytes);
    memcpy(to, from, nb_of_bytes);
}
