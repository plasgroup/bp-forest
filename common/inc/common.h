#ifndef __COMMON_H__
#define __COMMON_H__

#ifdef __cplusplus
extern "C" {

#ifndef _Static_assert
#define _Static_assert static_assert
#endif

#endif  // ifdef __cplusplus


#include "bit_ops_macro.h"
#include "workload_types.h"

#include <stdint.h>


/*
 * MRAM Layout
 */

#define MRAM_CABIN_BYTES (27u * 1024u * 1024u)
#define MRAM_REQUEST_BUFFER_BYTES (8 * 1024 * 1024)


/*
 * Parameters
 */

#ifndef NR_TASKLETS
#error NR_TASKLETS is always used but never defined
#endif


#ifdef USE_RBTREE
#define SIZEOF_NODE (24u)
#define MAX_NUM_NODES_IN_DPU (MRAM_CABIN_BYTES / SIZEOF_NODE)
#define MAX_NUM_PAIRS_IN_DPU (MAX_NUM_NODES_IN_DPU)

#else /* USE_RBTREE */

#ifndef SIZEOF_NODE
#define SIZEOF_NODE (256u)
#endif

#define MAX_NUM_NODES_IN_DPU (MRAM_CABIN_BYTES / SIZEOF_NODE)
#define MAX_NR_PAIRS ((SIZEOF_NODE - 16) / 16)  // depends on the definition of `Node' in dpu/inc/bplustree.h
#define MAX_NUM_PAIRS_IN_DPU (MAX_NUM_NODES_IN_DPU * MAX_NR_PAIRS)

#endif /* USE_RBTREE */


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
    key_uint64_t key;            // key of each request
    value_uint64_t write_val_ptr;  // write pointer to the value if request is write
} each_request_t;

typedef struct {
    key_uint64_t key;
    value_uint64_t get_result;
} each_get_result_t;

typedef each_get_result_t dpu_get_results_t[MAX_REQ_NUM_IN_A_DPU];

typedef struct {
    key_uint64_t succ_key;
    value_uint64_t succ_val_ptr;
} each_succ_result_t;

typedef each_succ_result_t dpu_succ_results_t[MAX_REQ_NUM_IN_A_DPU];

typedef union {
    dpu_get_results_t get;
    dpu_succ_results_t succ;
} dpu_results_t;

typedef struct {
    key_uint64_t start;
    key_uint64_t end_inclusive;
    key_uint64_t interval;
} dpu_init_param_t;

typedef struct {
    uint32_t left_npairs_ratio_x2147483648;
    uint32_t right_npairs_ratio_x2147483648;
} migration_ratio_param_t;
typedef struct {
    key_uint64_t left_delim_key;
    key_uint64_t right_delim_key;
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
    key_uint64_t key;
    value_uint64_t value;
} KVPair;
typedef struct {
    key_uint64_t begin, end;
} KeyRange;
typedef struct {
    uint32_t begin, end;
} IndexRange;
typedef struct {
    uint16_t nr_keys[4];
    key_uint64_t head_keys[4];
} SummaryBlock;

/* Tasks */
enum TaskID : uint32_t {
    TASK_INIT,
    TASK_GET,
    TASK_PRED,
    TASK_SCAN,
    TASK_INSERT,
    TASK_DELETE,
    TASK_SUMMARIZE,
    TASK_EXTRACT,
    TASK_CONSTRUCT_HOT,
    TASK_FLATTEN_HOT,
    TASK_RESTORE,
    TASK_NONE
};

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
