#pragma once
/* Listing parameters and setting default values */

/* !!!!!!!!!!!!!!! UPDATE print_params.h TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE print_params.h TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE print_params.h TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE print_params.h TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE print_params.h TOGETHER !!!!!!!!!!!!!! */

#ifndef MRAM_FOR_TREE
#define MRAM_FOR_TREE (27u * 1024u * 1024u)
#endif

// #define BITMAP_IN_MRAM


#ifdef USE_RBTREE
#define SIZEOF_NODE (24u)


#else /* USE_RBTREE */

#ifndef SIZEOF_NODE
#define SIZEOF_NODE (256u)
#endif


#ifndef TASK_INIT_NR_TASKLETS
#define TASK_INIT_NR_TASKLETS NR_TASKLETS
#endif
_Static_assert(TASK_INIT_NR_TASKLETS <= NR_TASKLETS, "TASK_INIT_NR_TASKLETS <= NR_TASKLETS");

#ifndef TASK_INIT_NR_CACHED_KVPAIRS
#define TASK_INIT_NR_CACHED_KVPAIRS 1
#endif

#ifndef TASK_INIT_NR_CACHED_INPUT_LIFT
#define TASK_INIT_NR_CACHED_INPUT_LIFT 2
#endif

#ifndef TASK_INIT_NR_CACHED_OUTPUT_LIFT
#define TASK_INIT_NR_CACHED_OUTPUT_LIFT (MAX_NR_CHILDREN / 2 * 2)
#endif

#ifndef TASK_INIT_NR_CACHED_NODES
#define TASK_INIT_NR_CACHED_NODES 1
#endif

// #define TASK_INIT_CHECK

#endif /* USE_RBTREE */


#ifndef TASK_INIT_BITMAP_NR_TASKLETS
#define TASK_INIT_BITMAP_NR_TASKLETS NR_TASKLETS
#endif
_Static_assert(TASK_INIT_BITMAP_NR_TASKLETS <= NR_TASKLETS, "TASK_INIT_BITMAP_NR_TASKLETS <= NR_TASKLETS");

#ifndef TASK_INIT_NR_CACHED_WORDS
#define TASK_INIT_NR_CACHED_WORDS 2
#endif
