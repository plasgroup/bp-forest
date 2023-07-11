// #pragma once
// #include "bplustree.h"
// #include <stdint.h>
// #include <string>
// #include <vector>

// #define DPU_ASSERT(func) func
// #define DPU_ASYNCHRONOUS 1
// #define NUM_REQUESTS_PER_BATCH (1000000)

// #define NR_DPUS (1)

// #define NUM_TOTAL_TREES (NR_DPUS * MAX_NUM_BPTREE_IN_DPU)
// #define MAX_NUM_SPLIT (10)

// /* Structure used by both the host and the dpu to communicate information */
// typedef uint64_t key_int64_t;
// typedef uint64_t value_ptr_t;
// typedef struct {
//     /*  num_elems: number of elements(k-v pair) in the tree
//     new_tree_index: the tree_index of the new tree made by split
//     split_key: the border key of the split  */
//     int num_elems[MAX_NUM_SPLIT];
//     key_int64_t split_key[MAX_NUM_SPLIT];
//     int new_tree_index[MAX_NUM_SPLIT];
// } split_info_t;

// extern split_info_t split_result[MAX_NUM_BPTREE_IN_DPU];
// // one request
// typedef struct each_request_t {
//     key_int64_t key;            // key of each request
//     value_ptr_t write_val_ptr;  // write pointer to the value if request is write
//     uint8_t operation;          // 1→read or 0→write
// } each_request_t;

// typedef std::vector<std::vector<each_request_t>> dpu_requests_t;


// typedef struct dpu_set_t {
//     int dummy;
// } dpu_set_t;


// void dpu_launch(dpu_set_t, int);
// void dpu_load(dpu_set_t, std::string, void*);
// void dpu_free(dpu_set_t);
// void dpu_alloc(int, void*, dpu_set_t*);
// // dpu_prepare_xfer(dpu, &nb_cycles_get[each_dpu])
// // dpu_push_xfer(set, DPU_XFER_FROM_DPU, "nb_cycles_get", 0, sizeof(uint64_t), DPU_XFER_DEFAULT)