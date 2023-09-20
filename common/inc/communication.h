#pragma once
/* Structure used by both the host and the dpu to communicate information */

#include <stdint.h>
#include "common.h"

typedef uint64_t key_int64_t;
typedef uint64_t value_ptr_t;
// one request
typedef struct {
    key_int64_t key;            // key of each request
    value_ptr_t write_val_ptr;  // write pointer to the value if request is write
    uint8_t operation;          // 1→read or 0→write
} each_request_t;

// requests for a DPU in a batch
typedef struct {
    int end_idx[MAX_NUM_BPTREE_IN_DPU];  // number of requests
    each_request_t requests[MAX_REQ_NUM_IN_A_DPU];
} dpu_requests_t;

typedef struct {
    /*  num_elems: number of elements(k-v pair) in the tree
    new_tree_index: the tree_index of the new tree made by split
    split_key: the border key of the split  */
    int num_elems[MAX_NUM_SPLIT];
    key_int64_t split_key[MAX_NUM_SPLIT];
    int new_tree_index[MAX_NUM_SPLIT];
} split_info_t;


extern split_info_t split_result[MAX_NUM_BPTREE_IN_DPU];

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

typedef union {
    value_ptr_t val_ptr;
} dpu_result_t;
