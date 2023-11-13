#pragma once
#include "common.h"
#include <map>
#include <stdlib.h>

/* Data structures in host for managing subtrees in DPUs*/
class HostTree {
    public:
        uint64_t tree_bitmap[NR_DPUS];
        int num_seats_used[NR_DPUS];
        key_int64_t tree_to_key_map[NR_DPUS][NR_SEATS_IN_DPU];
        std::map<key_int64_t, std::pair<int, int>> key_to_tree_map;
        HostTree(key_int64_t range)
        {
            /* init key_to_tree_map */
            for (int i = 0; i < NR_DPUS * NUM_INIT_TREES_IN_DPU; i++) {
               
                key_to_tree_map[range * (i + 1)] = std::make_pair(i / NUM_INIT_TREES_IN_DPU, i % NUM_INIT_TREES_IN_DPU);
            }
            /* init tree_to_key_map */
            for (int i = 0; i < NR_DPUS; i++) {
                for (int j = 0; j < NUM_INIT_TREES_IN_DPU; j++) {
                    tree_to_key_map[i][j] = range * (i * NUM_INIT_TREES_IN_DPU + j + 1);
                }
            }
            /* init tree_bitmap */
            for (uint64_t i = 0; i < NR_DPUS; i++) {
                tree_bitmap[i] = (1 << NUM_INIT_TREES_IN_DPU) - 1;
                num_seats_used[i] = NUM_INIT_TREES_IN_DPU;
            }
        }

    private:

};

/* Data structures in host for managing queries in a batch*/
class BatchCtx {
    public:
        /* key_index: i番目のDPUのj番目の木へのクエリの開始インデックス */
        int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1] {};
        int DPU_idx[NR_DPUS] {};
        int num_keys_for_DPU[NR_DPUS] {};
        int num_keys_for_tree[NR_DPUS][NR_SEATS_IN_DPU] {};
        int send_size {};
        BatchCtx()
        {
            for (int i = 0; i < NR_DPUS; i++) {
                DPU_idx[i] = i;
            }
        }
    private:

};
