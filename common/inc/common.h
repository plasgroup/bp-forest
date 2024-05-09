#ifndef __COMMON_H__
#define __COMMON_H__

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#include "bit_ops_macro.h"
#include "workload_types.h"

#include <stdint.h>


/*
 * MRAM Layout
 */

#define MRAM_CABIN_BYTES (27 * 1024 * 1024)
#define MRAM_REQUEST_BUFFER_BYTES (8 * 1024 * 1024)


/*
 * Parameters
 */

#ifndef NR_TASKLETS
#error NR_TASKLETS is always used but never defined
#endif

#define SIZEOF_NODE (2048)
#define MAX_NUM_NODES_IN_SEAT (MRAM_CABIN_BYTES / SIZEOF_NODE)
#define MAX_NR_PAIRS ((SIZEOF_NODE - 16) / 16)        // depends on the definition of `Node' in dpu/inc/bplustree.h
#ifdef CACHE_CHILD_HEADER_IN_LINK
#define MAX_NR_CHILDREN ((SIZEOF_NODE / 16) / 2 * 2)  // depends on the definition of `Node' in dpu/inc/bplustree.h
#else
#define MAX_NR_CHILDREN ((SIZEOF_NODE / 12) / 2 * 2)  // depends on the definition of `Node' in dpu/inc/bplustree.h
#endif

#ifndef MAX_REQ_NUM_IN_A_DPU
#define MAX_REQ_NUM_IN_A_DPU (BIT_FLOOR_UINT32(MRAM_REQUEST_BUFFER_BYTES / sizeof(each_request_t) / 2))
#endif


// #define PRINT_DEBUG
// #define DEBUG_ON
// #define PRINT_ON


/*
 * Shared Data Structures
 */

/* one request */
typedef struct {
    key_int64_t key;            // key of each request
    value_ptr_t write_val_ptr;  // write pointer to the value if request is write
} each_request_t;

typedef struct {
    key_int64_t key;
    value_ptr_t get_result;
} each_get_result_t;

typedef each_get_result_t dpu_get_results_t[MAX_REQ_NUM_IN_A_DPU];

typedef struct {
    key_int64_t succ_key;
    value_ptr_t succ_val_ptr;
} each_succ_result_t;

typedef each_succ_result_t dpu_succ_results_t[MAX_REQ_NUM_IN_A_DPU];

typedef union {
    dpu_get_results_t get;
    dpu_succ_results_t succ;
} dpu_results_t;

typedef struct {
    key_int64_t start;
    key_int64_t end_inclusive;
    key_int64_t interval;
} dpu_init_param_t;

typedef struct {
    uint32_t left_npairs_ratio_x2147483648;
    uint32_t right_npairs_ratio_x2147483648;
} migration_ratio_param_t;
typedef struct {
    key_int64_t left_delim_key;
    key_int64_t right_delim_key;
} migration_key_param_t;
#ifdef BULK_MIGRATION
typedef uint32_t migration_pairs_param_t;
#else
typedef struct {
    uint32_t num_left_kvpairs;
    uint32_t num_right_kvpairs;
} migration_pairs_param_t;
#endif

typedef struct {
    key_int64_t key;
    value_ptr_t value;
} KVPair;

/* Tasks */
#define TASK_INIT (UINT32_C(0))
#define TASK_GET (UINT32_C(10))
#define TASK_INSERT (UINT32_C(11))
#define TASK_DELETE (UINT32_C(12))
#define TASK_SUCC (UINT32_C(13))
#define TASK_PRED (UINT32_C(14))
#define TASK_FROM (UINT32_C(100))
#define TASK_TO (UINT32_C(101))

#define TASK_OPERAND_SHIFT 32
#define TASK_ID_MASK ((UINT64_C(1) << TASK_OPERAND_SHIFT) - 1)
#define TASK_WITH_OPERAND(task, rand) \
    ((task) | (((uint64_t)(rand)) << TASK_OPERAND_SHIFT))
#define TASK_GET_ID(task) ((task)&TASK_ID_MASK)
#define TASK_GET_OPERAND(task) ((task) >> TASK_OPERAND_SHIFT)


#define PRINT_POSITION_AND_VARIABLE(NAME, FORMAT) \
    printf("[Debug at %s:%d] " #NAME " = " #FORMAT "\n", __FILE__, __LINE__, NAME);
#define PRINT_POSITION_AND_MESSAGE(MESSAGE) \
    printf("[Debug at %s:%d] " #MESSAGE "\n", __FILE__, __LINE__);


_Static_assert(HAS_SINGLE_BIT_UINT(MAX_REQ_NUM_IN_A_DPU), "BIT_CEIL_UINT32 macro is broken");


#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus

#endif /* __COMMON_H__ */