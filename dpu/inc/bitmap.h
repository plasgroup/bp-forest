#pragma once

#include "bitmap_fwd.h"


static void bitmap_init(bitmap_word_ptr bitmap, const unsigned nr_nodes);

__attribute__((unused)) static void bitmap_set(bitmap_word_ptr bitmap, unsigned n);
static void bitmap_clear(bitmap_word_ptr bitmap, unsigned n);
static int bitmap_find_and_set_first_zero_range(bitmap_word_ptr bitmap,
    unsigned start, unsigned end);
static int bitmap_find_and_set_first_zero(bitmap_word_ptr bitmap, unsigned next, unsigned size);


#include "bitmap.i"  // implementation
