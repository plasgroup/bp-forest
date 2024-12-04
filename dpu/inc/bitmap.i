#pragma once

#include "bitmap.h"

#include "bit_ops_macro.h"
#include "bitmap_fwd.h"
#include "div_by_const.h"
#include "dpu_params.h"
#include "sync.h"
#include "workspace.h"

#include <attributes.h>
#include <defs.h>
#include <mram.h>

#include <stdint.h>
#include <string.h>


#ifdef BITMAP_IN_MRAM

#define TASK_INIT_NR_CACHED_ALLONE_WORDS (TASK_INIT_NR_CACHED_WORDS - 1)

static DEFINE_DIV_BY(TASK_INIT_NR_CACHED_ALLONE_WORDS, NODE_PTR_WIDTH - LOG_BITS_IN_BMPWD, _BMP_WORDS);
static DEFINE_DIV_BY(TASK_INIT_BITMAP_NR_TASKLETS, BITWIDTH_UINT32(MAX_NR_NODES / BITS_IN_BMPWD / TASK_INIT_NR_CACHED_ALLONE_WORDS), _ALLONE_WRITES);

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

static void bitmap_init(bitmap_word_ptr bitmap, const unsigned nr_nodes)
{
    _Static_assert(TASK_INIT_BITMAP_NR_TASKLETS > 0, "TASK_INIT_BITMAP_NR_TASKLETS > 0");
    if (me() < TASK_INIT_BITMAP_NR_TASKLETS) {
        const unsigned unsigned_me = (unsigned)me();
        const unsigned nr_preparation_per_tasklet = TASK_INIT_NR_CACHED_ALLONE_WORDS / TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_remainder_preparation = TASK_INIT_NR_CACHED_ALLONE_WORDS % TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_preparation_by_me = nr_preparation_per_tasklet + (unsigned_me < nr_remainder_preparation),
                       prepare_begin = nr_preparation_per_tasklet * me() + (unsigned_me <= nr_remainder_preparation ? me() : nr_remainder_preparation),
                       prepare_end = prepare_begin + nr_preparation_by_me;
        for (unsigned i = prepare_begin; i < prepare_end; i++) {
            workspace.bitmap[i] = BITMAP_WORD_MAX;
        }

        INIT_barrier_wait_for_preparing_bitmap_cache();

        const unsigned nr_all_one_words = nr_nodes / BITS_IN_BMPWD,
                       nr_allone_writes = DIV_BMP_WORDS_BY_TASK_INIT_NR_CACHED_ALLONE_WORDS(nr_all_one_words),
                       nr_writes_per_tasklet = DIV_ALLONE_WRITES_BY_TASK_INIT_BITMAP_NR_TASKLETS(nr_allone_writes),
                       nr_remainder_allones = nr_allone_writes - nr_writes_per_tasklet * TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_writes_by_me = nr_writes_per_tasklet + (unsigned_me < nr_remainder_allones),
                       idx_writes_begin = nr_writes_per_tasklet * unsigned_me + (unsigned_me <= nr_remainder_allones ? unsigned_me : nr_remainder_allones),
                       idx_dest_begin = idx_writes_begin * TASK_INIT_NR_CACHED_ALLONE_WORDS,
                       idx_writes_end = idx_writes_begin + nr_writes_by_me,
                       idx_dest_end = idx_writes_end * TASK_INIT_NR_CACHED_ALLONE_WORDS;

        unsigned idx_dest = idx_dest_begin;
        for (; idx_dest < idx_dest_end; idx_dest += TASK_INIT_NR_CACHED_ALLONE_WORDS) {
            mram_write(&workspace.bitmap[0], &bitmap[idx_dest], sizeof(bitmap_word_t) * TASK_INIT_NR_CACHED_ALLONE_WORDS);
        }

        if (me() == TASK_INIT_BITMAP_NR_TASKLETS - 1) {
            const unsigned nr_remained_bits = nr_nodes % BITS_IN_BMPWD,
                           nr_remained_allones = nr_all_one_words - nr_allone_writes * TASK_INIT_NR_CACHED_ALLONE_WORDS;
            unsigned nr_words_written = nr_remained_allones;

            if (nr_remained_bits != 0) {
                nr_words_written += 1;
                workspace.bitmap[TASK_INIT_NR_CACHED_ALLONE_WORDS] = BITMAP_WORD_MAX >> (BITS_IN_BMPWD - nr_remained_bits);
            }
            mram_write(&workspace.bitmap[TASK_INIT_NR_CACHED_ALLONE_WORDS - nr_remained_allones],
                &bitmap[idx_dest], sizeof(bitmap_word_t) * nr_words_written);
        }
    }
}
#else  /* BITMAP_IN_MRAM */

static DEFINE_DIV_BY(TASK_INIT_BITMAP_NR_TASKLETS, NODE_PTR_WIDTH - LOG_BITS_IN_BMPWD, _BMP_WORDS);

static void bitmap_init(bitmap_word_ptr bitmap, const unsigned nr_nodes)
{
    _Static_assert(TASK_INIT_BITMAP_NR_TASKLETS > 0, "TASK_INIT_BITMAP_NR_TASKLETS > 0");
    if (me() < TASK_INIT_BITMAP_NR_TASKLETS) {
        const unsigned unsigned_me = (unsigned)me();
        const unsigned nr_all_one_words = nr_nodes / BITS_IN_BMPWD,
                       nr_words_per_tasklet = DIV_BMP_WORDS_BY_TASK_INIT_BITMAP_NR_TASKLETS(nr_all_one_words),
                       nr_remainder_words = nr_all_one_words - nr_words_per_tasklet * TASK_INIT_BITMAP_NR_TASKLETS,
                       nr_words_for_me = nr_words_per_tasklet + (unsigned_me < nr_remainder_words),
                       idx_word_begin = nr_words_per_tasklet * unsigned_me + (unsigned_me <= nr_remainder_words ? unsigned_me : nr_remainder_words),
                       idx_words_end = idx_word_begin + nr_words_for_me;

        for (unsigned idx_word = idx_word_begin; idx_word < idx_words_end; idx_word++) {
            bitmap[idx_word] = BITMAP_WORD_MAX;
        }

        if (me() == TASK_INIT_BITMAP_NR_TASKLETS - 1) {
            const unsigned nr_remained_bits = nr_nodes % BITS_IN_BMPWD;
            if (nr_remained_bits != 0) {
                bitmap[nr_all_one_words] = BITMAP_WORD_MAX >> (BITS_IN_BMPWD - nr_remained_bits);
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
        for (pat = 1 << (id & (BITS_IN_BMPWD - 1u)); pat != 0;
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
