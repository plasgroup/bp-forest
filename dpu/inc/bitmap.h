#pragma once

#include <attributes.h>

#include <stdint.h>
#include <string.h>


typedef uint32_t bitmap_word_t;
#define LOG_BITS_IN_BMPWD 5
#define BITS_IN_BMPWD (1 << LOG_BITS_IN_BMPWD)

#ifdef BITMAP_IN_MRAM
#define DEFINE_BITMAP(NAME, NBITS) __mram_noinit bitmap_word_t NAME[(NBITS + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD]
typedef __mram_ptr bitmap_word_t* bitmap_word_ptr;
#else
#define DEFINE_BITMAP(NAME, NBITS) bitmap_word_t NAME[(NBITS + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD]
typedef bitmap_word_t* bitmap_word_ptr;
#endif

static void bitmap_clear_all(bitmap_word_ptr bitmap, unsigned size)
{
    memset(bitmap, 0, (size + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD);
}

__attribute__((unused)) static void bitmap_set(bitmap_word_ptr bitmap, unsigned n)
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
