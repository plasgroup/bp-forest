#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>
#include "workload_types.h"

/*
 * MRAM Layout
 */

#define MRAM_CABIN_BYTES           (30 * 1024 * 1024)
#define MRAM_REQUEST_BUFFER_BYTES  (15 * 1024 * 1024)

/*
 * Parameters
 */

#ifndef NR_DPUS
#define NR_DPUS (4)
#endif

#ifndef NR_TASKLETS
#define NR_TASKLETS (2)
#endif

#ifndef NR_SEATS_IN_DPU
#define NR_SEATS_IN_DPU (20)
#endif

#ifndef NR_INITIAL_TREES_IN_DPU
#define NR_INITIAL_TREES_IN_DPU (12)
#endif
#if NR_SEATS_IN_DPU < NR_INITIAL_TREES_IN_DPU
#error parameter error: NR_SEAT_IN_DPU < NR_INITIAL_TREES_IN_DPU
#endif

#ifndef NUM_REQUESTS_PER_BATCH
#define NUM_REQUESTS_PER_BATCH (1000000)
#endif

#ifndef NUM_INIT_REQS
#define NUM_INIT_REQS (2000 * (NR_DPUS * NR_INITIAL_TREES_IN_DPU))
#endif

#define MAX_NUM_NODES_IN_SEAT (MRAM_CABIN_BYTES / NR_SEATS_IN_DPU / sizeof(BPTreeNode))

#ifndef SOFT_LIMIT_NR_TREES_IN_DPU
#define SOFT_LIMIT_NR_TREES_IN_DPU (NR_SEATS_IN_DPU - MAX_NUM_SPLIT - 1)
#endif

#define MERGE_THRESHOLD (1500)
#define NUM_ELEMS_AFTER_MERGE (2000)

#ifndef MAX_REQ_NUM_IN_A_DPU
#define MAX_REQ_NUM_IN_A_DPU (MRAM_REQUEST_BUFFER_BYTES / sizeof(each_request_t) / 2)
#endif

/*
 * split related parameter
 */
#define MAX_NUM_SPLIT (5)
#define SPLIT_THRESHOLD (4000)
#define NR_ELEMS_AFTER_SPLIT (SPLIT_THRESHOLD / 2)


// #define PRINT_DEBUG
// #define VARY_REQUESTNUM
// #define DEBUG_ON
// #define STATS_ON
// #define PRINT_ON
// #define WRITE_CSV

#ifdef VARY_REQUESTNUM  // for experiment: xaxis is requestnum
#define NUM_VARS (8)    // number of point of xs
#endif

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
} each_result_t;

typedef struct {
    each_result_t results[MAX_REQ_NUM_IN_A_DPU];
} dpu_results_t;

#ifdef VARY_REQUESTNUM
typedef struct {  // for returning the result of the experiment
    int x;
    int cycle_insert;
    int cycle_get;
} dpu_stats_t;

typedef struct {  // for specifing the points of x in the experiment
    int vars[NUM_VARS];
    int gap;
    int DPUnum;
} dpu_experiment_var_t;
#endif

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
#define TASK_INIT (0ULL)
#define TASK_GET (10ULL)
#define TASK_INSERT (11ULL)
#define TASK_DELETE (12ULL)
#define TASK_FROM (100ULL)
#define TASK_TO (101ULL)
#define TASK_MERGE (102ULL)

#define TASK_OPERAND_SHIFT 32
#define TASK_ID_MASK ((1ULL << TASK_OPERAND_SHIFT) - 1)
#define TASK_WITH_OPERAND(task,rand) \
    ((task) | (((uint64_t) (rand)) << TASK_OPERAND_SHIFT))
#define TASK_GET_ID(task) ((task) & TASK_ID_MASK)
#define TASK_GET_OPERAND(task) ((task) >> TASK_OPERAND_SHIFT)



#define PRINT_POSITION_AND_VARIABLE(NAME, FORMAT) \
    printf("[Debug at %s:%d] " #NAME " = " #FORMAT "\n", __FILE__, __LINE__, NAME);
#define PRINT_POSITION_AND_MESSAGE(MESSAGE) \
    printf("[Debug at %s:%d] " #MESSAGE "\n", __FILE__, __LINE__);

#endif /* __COMMON_H__ */