#pragma once
#include <stdint.h>

#define NR_ELEMS_PER_DPU (RAND_MAX / NR_DPUS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)
#ifndef NR_DPUS
#define NR_DPUS (4)
#endif
#ifndef NR_TASKLETS
#define NR_TASKLETS (2)
#endif
#ifndef NR_SEATS_IN_DPU
#define NR_SEATS_IN_DPU (12)
#endif
/* maximum node data size for a DPU, MB */
#define NODE_DATA_SIZE (30)
#ifndef MAX_NUM_NODES_IN_SEAT
#define MAX_NUM_NODES_IN_SEAT ((NODE_DATA_SIZE << 20) / NR_SEATS_IN_DPU / sizeof(BPTreeNode))
#endif
#define MAX_NUM_SPLIT (5)
#ifndef MAX_NUM_SEATS_BEFORE_INSERT
#define MAX_NUM_SEATS_BEFORE_INSERT (NR_SEATS_IN_DPU - MAX_NUM_SPLIT - 1)
#endif
#define NUM_INIT_TREES_IN_DPU (NR_SEATS_IN_DPU / 4)
#define REQUEST_SIZE (sizeof(each_request_t))
/* buffer size for request in a DPU(default:15MB/64MB) */
#ifndef MRAM_REQUEST_BUFFER_SIZE
#define MRAM_REQUEST_BUFFER_SIZE (15 * 1024 * 1024)
#endif
#ifndef MAX_REQ_NUM_IN_A_DPU
#define MAX_REQ_NUM_IN_A_DPU (MRAM_REQUEST_BUFFER_SIZE / REQUEST_SIZE / 2)
#endif
#define NUM_REQUESTS_PER_BATCH (10000)
#define READ (1)
#define WRITE (0)
#define W50R50 (1)
#define W05R95 (2)
#ifndef WORKLOAD
#define WORKLOAD (W50R50)
#endif
// #define PRINT_DEBUG
// #define VARY_REQUESTNUM
// #define DEBUG_ON
// #define STATS_ON
// #define PRINT_ON
// #define WRITE_CSV

#ifdef VARY_REQUESTNUM  // for experiment: xaxis is requestnum
#define NUM_VARS (8)    // number of point of xs
#endif

/* Structure used by both the host and the dpu to communicate information */

typedef uint64_t key_int64_t;
#define KEY_MIN (0)
#define KEY_MAX (-1ULL)
typedef uint64_t value_ptr_t;

typedef struct KVPair {
    key_int64_t key;
    value_ptr_t value;
} KVPair;

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

typedef struct {
    /*  num_elems: number of elements(k-v pair) in the tree
    new_tree_index: the tree_index of the new tree made by split
    split_key: the border key of the split  */
    int num_split;
    int num_elems[MAX_NUM_SPLIT];
    key_int64_t split_key[MAX_NUM_SPLIT - 1];
    int new_tree_index[MAX_NUM_SPLIT];
} split_info_t;

/* Tasks */
#define TASK_INIT (0ULL)
#define TASK_GET (10ULL)
#define TASK_INSERT (11ULL)
#define TASK_FROM (100ULL)
#define TASK_TO (101ULL)

#define PRINT_POSITION_AND_VARIABLE(NAME, FORMAT) \
    printf("[Debug at %s:%d] " #NAME " = " #FORMAT "\n", __FILE__, __LINE__, NAME);
#define PRINT_POSITION_AND_MESSAGE(MESSAGE) \
    printf("[Debug at %s:%d] " #MESSAGE "\n", __FILE__, __LINE__);
