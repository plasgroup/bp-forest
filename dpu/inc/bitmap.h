#pragma once

#include <stdint.h>


typedef uint32_t bitmap_word_t;
#define LOG_BITS_IN_BMPWD 5
#define BITS_IN_BMPWD (1 << LOG_BITS_IN_BMPWD)


static void bitmap_set(bitmap_word_t* bitmap, int n)
{
    int index = n >> LOG_BITS_IN_BMPWD;
    int offset = n & (BITS_IN_BMPWD - 1);
    bitmap[index] |= ((bitmap_word_t)1) << offset;
}

static void bitmap_clear(bitmap_word_t* bitmap, int n)
{
    int index = n >> LOG_BITS_IN_BMPWD;
    int offset = n & (BITS_IN_BMPWD - 1);
    bitmap[index] &= ~(((bitmap_word_t)1) << offset);
}

static int bitmap_find_and_set_first_zero_range(bitmap_word_t* bitmap,
    int start, int end)
{
    int end_index = end >> LOG_BITS_IN_BMPWD;
    for (int id = start; id < end;) {
        int index = id >> LOG_BITS_IN_BMPWD;
        bitmap_word_t word = bitmap[index];
        bitmap_word_t pat;
        if (index == end_index)
            word |= (~(bitmap_word_t)0) << (end & (BITS_IN_BMPWD - 1));
        if (word == ~(bitmap_word_t)0) {
            id = (id + BITS_IN_BMPWD) & ~(BITS_IN_BMPWD - 1);
            continue;
        }
        for (pat = 1 << (id & (BITS_IN_BMPWD - 1)); pat != 0;
             id++, pat <<= 1)
            if ((word & pat) == 0) {
                bitmap[index] |= pat;
                return id;
            }
    }
    return -1;
}

static int bitmap_find_and_set_first_zero(bitmap_word_t* bitmap, int next, int size)
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
