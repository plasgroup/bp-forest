#pragma once

#include "dpu_params.h"
#include "tree_impl.h"

#include <stdio.h>


inline void print_params(void)
{
    printf("NR_TASKLETS: %u\n"
           "MRAM_FOR_TREE: %u\n"
           "BITMAP_IN_MRAM: %u\n"
           "SIZEOF_NODE: %u\n"
           "USE_RBTREE: %u\n"
#ifndef USE_RBTREE
           "TASK_INIT_NR_TASKLETS: %u\n"
           "TASK_INIT_NR_CACHED_KVPAIRS: %u\n"
           "TASK_INIT_NR_CACHED_INPUT_LIFT: %u\n"
           "TASK_INIT_NR_CACHED_OUTPUT_LIFT: %u\n"
           "TASK_INIT_NR_CACHED_NODES: %u\n"
           "TASK_INIT_CHECK: %u\n"
#endif
           "TASK_INIT_BITMAP_NR_TASKLETS: %u\n"
           "TASK_INIT_NR_CACHED_WORDS: %u\n",
        NR_TASKLETS,
        MRAM_FOR_TREE,
#ifdef BITMAP_IN_MRAM
        1,
#else
        0,
#endif
        SIZEOF_NODE,
#ifdef USE_RBTREE
        1,
#else
        0,
        TASK_INIT_NR_TASKLETS,
        TASK_INIT_NR_CACHED_KVPAIRS,
        TASK_INIT_NR_CACHED_INPUT_LIFT,
        TASK_INIT_NR_CACHED_OUTPUT_LIFT,
        TASK_INIT_NR_CACHED_NODES,
#ifdef TASK_INIT_CHECK
        1,
#else
        0,
#endif
#endif
        TASK_INIT_BITMAP_NR_TASKLETS,
        TASK_INIT_NR_CACHED_WORDS);
}
