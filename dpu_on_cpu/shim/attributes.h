#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <attributes.h>: the placement attributes
//! vanish (MRAM variables become ordinary globals, __mram_ptr ordinary
//! pointers); only the 8-byte alignment the SDK attributes imply is kept, as
//! the mram_read/mram_write shims assert it.
//!
//! Divergences from the real toolchain: mixing up MRAM and WRAM pointers is
//! no longer a compile error, and __mram_noinit is zero-initialized here
//! (garbage at boot on the device).

#include "dpu_cpu.h"

#define __aligned(a) __attribute__((aligned(a)))
#define __dma_aligned __aligned(8)

#define __mram_ptr
#define __mram __dma_aligned
#define __mram_noinit __dma_aligned
#define __mram_keep __dma_aligned
#define __mram_noinit_keep __dma_aligned
#define __host __aligned(8)
#define __atomic_bit
