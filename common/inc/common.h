#ifndef __COMMON_H__
#define __COMMON_H__

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#include "workload_types.h"

#include <stdint.h>


/*
 * MRAM Layout
 */

#define MRAM_CABIN_BYTES (30 * 1024 * 1024)
#define MRAM_REQUEST_BUFFER_BYTES (15 * 1024 * 1024)


/*
 * Parameters
 */

#ifndef NR_TASKLETS
#error NR_TASKLETS is always used but never defined
#endif

#ifndef NR_SEATS_IN_DPU
#define NR_SEATS_IN_DPU (20)
#endif

#define MAX_NUM_NODES_IN_SEAT (MRAM_CABIN_BYTES / NR_SEATS_IN_DPU / sizeof(BPTreeNode))

#ifndef MAX_REQ_NUM_IN_A_DPU
#define MAX_REQ_NUM_IN_A_DPU (MRAM_REQUEST_BUFFER_BYTES / sizeof(each_request_t) / 2)
#endif


/*
 * split related parameter
 */
#define MAX_NUM_SPLIT (5)

#ifndef SPLIT_THRESHOLD
#define SPLIT_THRESHOLD (4000)
#endif  // ifndef SPLIT_THRESHOLD

#define NR_ELEMS_AFTER_SPLIT (SPLIT_THRESHOLD / 2)


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

/* requests for a DPU in a batch */
typedef struct {
    each_request_t requests[MAX_REQ_NUM_IN_A_DPU];
} dpu_requests_t;

typedef struct {
    value_ptr_t get_result;
} each_get_result_t;

typedef struct {
    each_get_result_t results[MAX_REQ_NUM_IN_A_DPU];
} dpu_get_results_t;

typedef struct {
    key_int64_t succ_key;
    value_ptr_t succ_val_ptr;
} each_succ_result_t;

typedef struct {
    each_succ_result_t results[MAX_REQ_NUM_IN_A_DPU];
} dpu_succ_results_t;

typedef union {
    dpu_get_results_t get;
    dpu_succ_results_t succ;
} dpu_results_t;

typedef struct {
    int use;
    key_int64_t start;
    key_int64_t end_inclusive;
    key_int64_t interval;
} dpu_init_param_t;

typedef int seat_id_t;
#define INVALID_SEAT_ID (-1)
typedef uint64_t seat_set_t;

typedef struct {
    /*  num_elems: number of elements(k-v pair) in the tree
    new_tree_index: the tree_index of the new tree made by split
    split_key: the border key of the split  */
    int num_split;
    int num_elems[MAX_NUM_SPLIT];
    key_int64_t split_key[MAX_NUM_SPLIT - 1];
    int new_tree_index[MAX_NUM_SPLIT];
} split_info_t;

typedef struct {
    seat_id_t merge_to[NR_SEATS_IN_DPU];
} merge_info_t;

typedef struct KVPair {
    key_int64_t key;
    value_ptr_t value;
} KVPair;

/* Tasks */
#define TASK_INIT (UINT32_C(0))
#define TASK_GET (UINT32_C(10))
#define TASK_INSERT (UINT32_C(11))
#define TASK_DELETE (UINT32_C(12))
#define TASK_SUCC (UINT32_C(13))
#define TASK_FROM (UINT32_C(100))
#define TASK_TO (UINT32_C(101))
#define TASK_MERGE (UINT32_C(102))

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


#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus

#endif /* __COMMON_H__ */