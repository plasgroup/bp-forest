#include "migration.hpp"
#include "common.h"
#include <iostream>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <utility>

Migration_plan_ptr migration_plan_init(Migration_plan_ptr arr)
{
    for (int i = 0; i < NR_DPUS; ++i) {
        for (int j = 0; j < NR_SEATS_IN_DPU; ++j) {
            arr[i][j] = std::make_pair(-1, -1);
        }
    }

    return arr;
}

Migration_plan_ptr migration_plan_get(int num_migration, int* idx, Migration_plan_ptr migration_dest, uint64_t* tree_bitmap, int* num_keys_for_DPU, int num_keys_for_tree[NR_DPUS][NR_SEATS_IN_DPU], std::map<key_int64_t, std::pair<int, int>>& key_to_tree_map)
{
    int migrated_tree_num = 0;
    int num_trees_to_be_migrated = num_migration;  // 何個の木を移動するか
    int count = 0;                                 // チェックしたDPUの数(移動しなかった場合も含む)
    while (num_trees_to_be_migrated) {
        if (count >= NR_DPUS - 1 - (num_migration - num_trees_to_be_migrated)) {
            //printf("migration limit because of NR_DPUS, %d migration done\n", num_migration - num_trees_to_be_migrated);
            break;
        }
        int from_DPU = idx[count];
        int to_DPU = idx[NR_DPUS - 1 - (num_migration - num_trees_to_be_migrated)];
        int diff_before = num_keys_for_DPU[from_DPU] - num_keys_for_DPU[to_DPU];
        //printf("from_DPU=%d,to_DPU=%d,diff_before=%d\n", from_DPU, to_DPU, diff_before);
        if (diff_before < 200) {  // すでに負荷分散出来ているのでこの先のペアは移動不要
            //printf("migration limit because the workload is already balanced, %d migration done\n", num_migration - num_trees_to_be_migrated);
            break;
        }
        if (__builtin_popcount(tree_bitmap[from_DPU]) > 1) {  // 木が1つだけだったら移動しない
            int from_tree = 0;
            int min_diff_after = 1 << 30;
            /* determine from_tree */
            for (int i = 0; i < NR_SEATS_IN_DPU; i++) {                                                                                                                      // 最も負荷を分散出来る木を移動
                if ((tree_bitmap[from_DPU] & (1 << i)) != 0ULL) {                                                                                                            // if the tree exists
                    int diff_after = std::abs((num_keys_for_DPU[from_DPU] - num_keys_for_tree[from_DPU][i]) - (num_keys_for_DPU[to_DPU] + num_keys_for_tree[from_DPU][i]));  // 移動後のクエリ数の差
                    if (diff_after < min_diff_after) {
                        from_tree = i;
                        min_diff_after = diff_after;
                    }
                }
            }
            if (diff_before > min_diff_after) {  // 木を移動した結果、さらに偏ってしまう場合はのぞく
                /* determine to_tree */
                int to_tree;
                for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
                    if (!(tree_bitmap[to_DPU] & (1 << i))) {  // i番目の木が空いているかどうか
                        to_tree = i;
                        //printf("migration: (%d,%d)->(%d,%d)\n", from_DPU, from_tree, to_DPU, to_tree);
                        migration_dest[from_DPU][from_tree] = std::make_pair(to_DPU, to_tree);
                        // send_nodes_from_dpu_to_dpu(from_DPU, from_tree, to_DPU, to_tree, set, dpu);
                        num_keys_for_DPU[from_DPU] -= num_keys_for_tree[from_DPU][from_tree];
                        num_keys_for_DPU[to_DPU] += num_keys_for_tree[from_DPU][from_tree];
                        num_keys_for_tree[to_DPU][to_tree] = num_keys_for_tree[from_DPU][from_tree];
                        num_keys_for_tree[from_DPU][from_tree] = 0;
                        num_trees_to_be_migrated--;
                        migrated_tree_num++;
                        break;
                    }
                }
            }
        }
        count++;
    }
    return migration_dest;
}
