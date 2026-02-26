#pragma once

#include "bitmap.h"

#include "bit_ops_macro.h"
#include "bitmap_fwd.h"
#include "div_by_const.h"
#include "dpu_params.h"
#include "sync.h"
#include "workspace.h"

#include <attributes.h>
#include <built_ins.h>
#include <defs.h>
#include <mram.h>

#include <stdint.h>
#include <string.h>


#ifdef BITMAP_IN_MRAM

#define TASK_INIT_NR_CACHED_WORDS_M1 (TASK_INIT_NR_CACHED_WORDS - 1)

static DEFINE_DIV_BY(TASK_INIT_NR_CACHED_WORDS_M1, NODE_PTR_WIDTH - LOG_BITS_IN_BMPWD, _BMP_WORDS);
static DEFINE_DIV_BY(TASK_INIT_BITMAP_NR_TASKLETS, BITWIDTH_UINT32(MAX_NR_NODES / BITS_IN_BMPWD / TASK_INIT_NR_CACHED_WORDS_M1), _ALLONE_WRITES);

static void INIT_barrier_wait_for_preparing_bitmap_cache(void)
{
    if (me() != 0) {
        wait_for_prev_ready();
    }
    if (me() != TASK_INIT_BITMAP_NR_TASKLETS - 1) {
        notify_next_of_readiness();
        wait_for_next_ready();
    }
    if (me() != 0) {
        notify_prev_of_readiness();
    }
}

static void bitmap_init(bitmap_word_ptr bitmap, const unsigned nr_bits, const unsigned nr_nodes)
{
    _Static_assert(TASK_INIT_BITMAP_NR_TASKLETS > 0, "TASK_INIT_BITMAP_NR_TASKLETS > 0");
    if (me() < TASK_INIT_BITMAP_NR_TASKLETS) {
        const unsigned unsigned_me = (unsigned)me();
        const unsigned nr_preparation_per_tasklet = TASK_INIT_NR_CACHED_WORDS_M1 / TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_remainder_preparation = TASK_INIT_NR_CACHED_WORDS_M1 % TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_preparation_by_me = nr_preparation_per_tasklet + (unsigned_me < nr_remainder_preparation),
                       prepare_begin = nr_preparation_per_tasklet * unsigned_me + (unsigned_me <= nr_remainder_preparation ? unsigned_me : nr_remainder_preparation),
                       prepare_end = prepare_begin + nr_preparation_by_me;
        for (unsigned i = prepare_begin; i < prepare_end; i++) {
            workspace.bitmap[i] = BITMAP_WORD_MAX;
        }

        INIT_barrier_wait_for_preparing_bitmap_cache();

        {
            const unsigned nr_all_one_words = nr_nodes / BITS_IN_BMPWD,
                           nr_allone_writes = DIV_BMP_WORDS_BY_TASK_INIT_NR_CACHED_WORDS_M1(nr_all_one_words),
                           nr_writes_per_tasklet = DIV_ALLONE_WRITES_BY_TASK_INIT_BITMAP_NR_TASKLETS(nr_allone_writes),
                           nr_remainder_writes = nr_allone_writes - nr_writes_per_tasklet * TASK_INIT_BITMAP_NR_TASKLETS,
                           nr_writes_by_me = nr_writes_per_tasklet + (unsigned_me < nr_remainder_writes),
                           idx_writes_begin = nr_writes_per_tasklet * unsigned_me + (unsigned_me <= nr_remainder_writes ? unsigned_me : nr_remainder_writes),
                           idx_dest_begin = idx_writes_begin * TASK_INIT_NR_CACHED_WORDS_M1,
                           idx_writes_end = idx_writes_begin + nr_writes_by_me,
                           idx_dest_end = idx_writes_end * TASK_INIT_NR_CACHED_WORDS_M1;

            unsigned idx_dest = idx_dest_begin;
            for (; idx_dest < idx_dest_end; idx_dest += TASK_INIT_NR_CACHED_WORDS_M1) {
                mram_write(&workspace.bitmap[0], &bitmap[idx_dest], sizeof(bitmap_word_t) * TASK_INIT_NR_CACHED_WORDS_M1);
            }

            if (me() == TASK_INIT_BITMAP_NR_TASKLETS - 1) {
                const unsigned nr_remained_bits = nr_nodes % BITS_IN_BMPWD,
                               nr_remained_allones = nr_all_one_words - nr_allone_writes * TASK_INIT_NR_CACHED_WORDS_M1;
                unsigned nr_words_written = nr_remained_allones;

                if (nr_remained_bits != 0) {
                    nr_words_written += 1;
                    workspace.bitmap[TASK_INIT_NR_CACHED_WORDS_M1] = BITMAP_WORD_MAX >> (BITS_IN_BMPWD - nr_remained_bits);
                }
                if (nr_remained_allones != 0) {
                    mram_write(&workspace.bitmap[TASK_INIT_NR_CACHED_WORDS_M1 - nr_remained_allones],
                        &bitmap[idx_dest], sizeof(bitmap_word_t) * nr_words_written);
                }
            }
        }

        INIT_barrier_wait_for_preparing_bitmap_cache();

        for (unsigned i = prepare_begin; i < prepare_end; i++) {
            workspace.bitmap[i] = 0;
        }

        INIT_barrier_wait_for_preparing_bitmap_cache();

        {
            const unsigned nr_words = (nr_bits + BITS_IN_BMPWD - 1) / BITS_IN_BMPWD,
                           nr_nonzero_words = (nr_nodes + BITS_IN_BMPWD - 1) / BITS_IN_BMPWD,
                           nr_zero_words = nr_words - nr_nonzero_words,
                           nr_zero_writes = DIV_BMP_WORDS_BY_TASK_INIT_NR_CACHED_WORDS_M1(nr_zero_words),
                           nr_writes_per_tasklet = DIV_ALLONE_WRITES_BY_TASK_INIT_BITMAP_NR_TASKLETS(nr_zero_writes),
                           nr_remainder_writes = nr_zero_writes - nr_writes_per_tasklet * TASK_INIT_BITMAP_NR_TASKLETS,
                           nr_writes_by_me = nr_writes_per_tasklet + (unsigned_me < nr_remainder_writes),
                           idx_writes_begin = nr_writes_per_tasklet * unsigned_me + (unsigned_me <= nr_remainder_writes ? unsigned_me : nr_remainder_writes),
                           idx_dest_begin = nr_nonzero_words + idx_writes_begin * TASK_INIT_NR_CACHED_WORDS_M1,
                           idx_writes_end = idx_writes_begin + nr_writes_by_me,
                           idx_dest_end = nr_nonzero_words + idx_writes_end * TASK_INIT_NR_CACHED_WORDS_M1;

            unsigned idx_dest = idx_dest_begin;
            for (; idx_dest < idx_dest_end; idx_dest += TASK_INIT_NR_CACHED_WORDS_M1) {
                mram_write(&workspace.bitmap[0], &bitmap[idx_dest], sizeof(bitmap_word_t) * TASK_INIT_NR_CACHED_WORDS_M1);
            }

            if (me() == TASK_INIT_BITMAP_NR_TASKLETS - 1) {
                const unsigned nr_remained_zeros = nr_zero_words - nr_zero_writes * TASK_INIT_NR_CACHED_WORDS_M1;
                if (nr_remained_zeros != 0) {
                    mram_write(&workspace.bitmap[0], &bitmap[idx_dest], sizeof(bitmap_word_t) * nr_remained_zeros);
                }
            }
        }
    }
}
#else  /* BITMAP_IN_MRAM */

static DEFINE_DIV_BY(TASK_INIT_BITMAP_NR_TASKLETS, NODE_PTR_WIDTH - LOG_BITS_IN_BMPWD, _BMP_WORDS);

static void bitmap_init(bitmap_word_ptr bitmap, const unsigned nr_bits, const unsigned nr_nodes)
{
    _Static_assert(TASK_INIT_BITMAP_NR_TASKLETS > 0, "TASK_INIT_BITMAP_NR_TASKLETS > 0");
    if (me() < TASK_INIT_BITMAP_NR_TASKLETS) {
        const unsigned unsigned_me = (unsigned)me();
        const unsigned nr_words = (nr_bits + BITS_IN_BMPWD - 1) / BITS_IN_BMPWD,
                       nr_words_per_tasklet = DIV_BMP_WORDS_BY_TASK_INIT_BITMAP_NR_TASKLETS(nr_words),
                       nr_remainder_words = nr_words - nr_words_per_tasklet * TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_words_for_me = nr_words_per_tasklet + (unsigned_me < nr_remainder_words),
                       idx_word_begin = nr_words_per_tasklet * unsigned_me + (unsigned_me <= nr_remainder_words ? unsigned_me : nr_remainder_words),
                       idx_words_end = idx_word_begin + nr_words_for_me;

        const unsigned nr_all_one_words = nr_nodes / BITS_IN_BMPWD;
        if (idx_words_end <= nr_all_one_words) {
            // All words to be set to all-one
            for (unsigned i = idx_word_begin; i < idx_words_end; i++) {
                bitmap[i] = BITMAP_WORD_MAX;
            }
        } else if (nr_all_one_words < idx_word_begin) {
            // All words to be set to zero
            for (unsigned i = idx_word_begin; i < idx_words_end; i++) {
                bitmap[i] = 0;
            }
        } else {
            // Some words to be set to all-one, some words to be set to zero, and possibly one partial word
            unsigned i = idx_word_begin;
            for (; i < nr_all_one_words; i++) {
                bitmap[i] = BITMAP_WORD_MAX;
            }
            const unsigned nr_remained_bits = nr_nodes % BITS_IN_BMPWD;
            if (nr_remained_bits != 0) {
                bitmap[i] = BITMAP_WORD_MAX >> (BITS_IN_BMPWD - nr_remained_bits);
                i++;
            }
            for (; i < idx_words_end; i++) {
                bitmap[i] = 0;
            }
        }
    }
}
#endif /* BITMAP_IN_MRAM */


static void bitmap_set(bitmap_word_ptr bitmap, unsigned n)
{
    unsigned index = n >> LOG_BITS_IN_BMPWD;
    unsigned offset = n & (BITS_IN_BMPWD - 1);
    bitmap[index] |= ((bitmap_word_t)1) << offset;
}

static void bitmap_clear(bitmap_word_ptr bitmap, unsigned n)
{
    unsigned index = n >> LOG_BITS_IN_BMPWD;
    unsigned offset = n & (BITS_IN_BMPWD - 1);
    bitmap[index] &= ~(((bitmap_word_t)1) << offset);
}

static int bitmap_find_and_set_first_zero_range(bitmap_word_ptr bitmap,
    unsigned start, unsigned end)
{
    unsigned end_index = end >> LOG_BITS_IN_BMPWD;
    for (unsigned id = start; id < end;) {
        unsigned index = id >> LOG_BITS_IN_BMPWD;
        bitmap_word_t word = bitmap[index];
        bitmap_word_t pat;
        if (index == end_index)
            word |= (~(bitmap_word_t)0) << (end & (BITS_IN_BMPWD - 1u));
        if (word == ~(bitmap_word_t)0) {
            id = (id + BITS_IN_BMPWD) & ~(BITS_IN_BMPWD - 1u);
            continue;
        }
        for (pat = BITMAP_WORD_C(1) << (id & (BITS_IN_BMPWD - 1u)); pat != 0;
             id++, pat <<= 1)
            if ((word & pat) == 0) {
                bitmap[index] |= pat;
                return (int)id;
            }
    }
    return -1;
}

static int bitmap_find_and_set_first_zero(bitmap_word_ptr bitmap, unsigned next, unsigned size)
{
    int id;

    id = bitmap_find_and_set_first_zero_range(bitmap, next, size);
    if (id >= 0)
        return id;
    id = bitmap_find_and_set_first_zero_range(bitmap, 0, next);
    if (id >= 0)
        return id;
    return -1;
}

__attribute__((unused)) static uint32_t bitmap_count_ones(bitmap_word_ptr bitmap, unsigned size)
{
    uint32_t result = 0;
    const unsigned nr_words = (size + BITS_IN_BMPWD - 1) / BITS_IN_BMPWD;
    for (unsigned i = 0; i < nr_words; i++) {
        __dma_aligned bitmap_word_t word;
#ifdef BITMAP_IN_MRAM
        mram_read(&bitmap[i], &word, sizeof(bitmap_word_t));
#else
        word = bitmap[i];
#endif
        const uint32_t upper = (uint32_t)(word / (UINT64_C(1) << 32)),
                       lower = (uint32_t)upper;

        uint32_t tmp;
        __builtin_cao_rr(tmp, upper);
        result += tmp;
        __builtin_cao_rr(tmp, lower);
        result += tmp;
    }
    return result;
}
