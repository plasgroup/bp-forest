#pragma once

#include "dpu_params.h"

#include <attributes.h>

#include <stdint.h>


typedef uint64_t bitmap_word_t;
#define BITMAP_WORD_MAX UINT64_MAX
#define BITMAP_WORD_C(literal) UINT64_C(literal)

#define LOG_BITS_IN_BMPWD 6
#define BITS_IN_BMPWD (BITMAP_WORD_C(1) << LOG_BITS_IN_BMPWD)


#ifdef BITMAP_IN_MRAM
#define DEFINE_BITMAP(NAME, NBITS) __mram bitmap_word_t NAME[(NBITS + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD] = {}
typedef __mram_ptr bitmap_word_t* bitmap_word_ptr;
#else
#define DEFINE_BITMAP(NAME, NBITS) bitmap_word_t NAME[(NBITS + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD] = {}
typedef bitmap_word_t* bitmap_word_ptr;
#endif


typedef bitmap_word_t BitmapInitWorkspace[TASK_INIT_NR_CACHED_WORDS];
