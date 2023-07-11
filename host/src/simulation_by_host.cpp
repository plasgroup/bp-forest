// #pragma once

// #include "bplustree.h"
// #include "simulation_by_host.hpp"
// #include <stdio.h>
// extern BPlusTree* bptrees[MAX_NUM_BPTREE_IN_DPU];
// extern dpu_requests_t dpu_requests;
// extern int num_reqs_for_each_tree[NUM_TOTAL_TREES];
// extern int num_trees;
// /* DPU program */
// void dpu_launch(dpu_set_t set, int mode) {
//     if(num_trees == 0) new_BPTree();
//     for (int tree_i = 0; tree_i < NUM_TOTAL_TREES; tree_i++) {
//         for (int req_i; req_i < num_reqs_for_each_tree[tree_i]; req_i++){
//             each_request_t req = dpu_requests.at(tree_i).at(req_i);
//             switch(req.operation){
//                 case WRITE:
//                 BPTreeInsert(bptrees[tree_i], req.key, req.write_val_ptr);
//                 case READ:
//                 BPTreeGet(bptrees[tree_i], req.key);
//                 default: continue;
//             }
//         }
//     }
//     /* split phase */
//     int num_of_split_tree = 0;
//     for(int tree_i = 0; tree_i < NUM_TOTAL_TREES; tree_i++){
//         if(do_split_phase(bptrees[tree_i])){
//             num_of_split_tree++;
//             printf("tree_%d splitted\n",tree_i);
//         }
//     }
//     return;
// }
// void dpu_load(dpu_set_t set, std::string, void* mode){return;}
// void dpu_free(dpu_set_t set){return;}
// void dpu_alloc(int nr_dpus, void* mode, dpu_set_t* set){return;}