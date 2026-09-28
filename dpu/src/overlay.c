/* Resident loader of the IRAM overlay (docs/dpu_iram_overlay.md). */
#include "iram_overlay.h"

#ifdef IRAM_OVERLAY

#include <barrier.h>
#include <built_ins.h>
#include <defs.h>

#include <stdint.h>


BARRIER_INIT(ovl_barrier, NR_TASKLETS);

/* Byte address of the IRAM window (= instruction index x 8), right after the resident .text. */
extern uint8_t __ovl_window_byte_addr[];

/* The slot currently in the window. Right after the binary is loaded, nothing is. */
static uint32_t ovl_idx_loaded_slot = UINT32_MAX;

/*
 * The barriers before and after guarantee that no tasklet is executing code
 * in the window while it is being overwritten. The transfer is split among
 * all tasklets.
 *
 * ldmai (MRAM -> IRAM DMA) transfers at most 2048 bytes per instruction.
 * An immediate cannot be changed at run time, so the number of words to
 * transfer (in 64-bit units, 1 + imm + ra[31:24]) is put in the upper byte
 * of the IRAM-side address register. MRAM addresses start at 0x08000000 at
 * link time and at 0 at run time.
 */
void ovl_load_slot(const uint32_t idx_slot, const uint8_t* const image_lma, const uint32_t nbytes)
{
    if (ovl_idx_loaded_slot == idx_slot) {
        return;
    }
    barrier_wait(&ovl_barrier);

    const uint32_t mram_begin = (uint32_t)image_lma & 0x07ffffffu;
    const uint32_t iram_begin = (uint32_t)__ovl_window_byte_addr;
    for (uint32_t offset = 2048u * me(); offset < nbytes; offset += 2048u * NR_TASKLETS) {
        const uint32_t rest = nbytes - offset;
        const uint32_t chunk = (rest > 2048u ? 2048u : rest);
        const uint32_t nr_words = chunk / 8u;
        __builtin_ldmai_rri((iram_begin + offset) | ((nr_words - 1u) << 24), mram_begin + offset, "0");
    }

    if (me() == 0) {
        ovl_idx_loaded_slot = idx_slot;
    }
    barrier_wait(&ovl_barrier);
}

#endif /* IRAM_OVERLAY */
