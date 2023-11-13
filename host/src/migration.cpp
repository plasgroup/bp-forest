#include "migration.hpp"
#include "common.h"
#include "host_data_structures.hpp"
#include "node_defs.hpp"
#include <algorithm>
extern "C" {
    #include <dpu.h>
    #include <dpu_log.h>
}
#include <iostream>
#include <map>
#include <stdio.h>
#include <stdlib.h>

#define QUERY_BALANCED (200)

// 比較関数
auto comp_idx(int* ptr)
{
    return [ptr](int l_idx, int r_idx) {
        return ptr[l_idx] > ptr[r_idx];
    };
}

Migration_plan_t migration_plan_new()
{
    Migration_plan_t new_plan(NR_DPUS, Migration_DPU_t(NR_SEATS_IN_DPU));
    return new_plan;
}

Migration_plan_t& migration_plan_init(Migration_plan_t& plan)
{
    for (int i = 0; i < NR_DPUS; ++i) {
        for (int j = 0; j < NR_SEATS_IN_DPU; ++j) {
            plan[i][j] = std::make_pair(-1, -1);
        }
    }

    return plan;
}

Migration_plan_t& migration_plan_query_balancing(Migration_plan_t& migration_plan, HostTree* tree, BatchCtx& batch_ctx, int num_migration, int max_num_seats_in_DPU)
{
    extern int migrated_tree_num; /* for stats */
    migrated_tree_num = 0;
    int num_trees_to_be_migrated = num_migration;
    int idx_fromDPU = 0;
    int idx_toDPU = NR_DPUS - 1;
    while (num_trees_to_be_migrated) {
        int from_DPU = batch_ctx.DPU_idx[idx_fromDPU];
        int to_DPU = batch_ctx.DPU_idx[idx_toDPU];
        int diff_before = batch_ctx.num_keys_for_DPU[from_DPU] - batch_ctx.num_keys_for_DPU[to_DPU];
        if (__builtin_popcount(tree->tree_bitmap[from_DPU]) > 1) {  // 木が1つだけだったら移動しない
            /* Search for an avalable to_DPU. The conditions are:
            1. There exists avalable seat in to_DPU
            2. There is more than QUERY_BALANCED difference in the number of queries between from_DPU and to_DPU
            3. search limit: idx_fromDPU == idx_toDPU
            */
            while (__builtin_popcount((tree->tree_bitmap[to_DPU]) > max_num_seats_in_DPU || diff_before < QUERY_BALANCED) && idx_fromDPU < idx_toDPU) {
                idx_toDPU--;
                to_DPU = batch_ctx.DPU_idx[idx_toDPU];
                diff_before = batch_ctx.num_keys_for_DPU[from_DPU] - batch_ctx.num_keys_for_DPU[to_DPU];
            }
            if (idx_fromDPU >= idx_toDPU) {
                printf("migration limit because of NR_DPUS, %d migrations\n", migrated_tree_num);
                break;
            }
            printf("from_DPU=%d, to_DPU=%d, diff_before=%d\n", from_DPU, to_DPU, diff_before);
            int from_tree = 0;
            int min_diff_after = 1 << 30;
            /* determine from_tree */
            for (int i = 0; i < NR_SEATS_IN_DPU; i++) {  // 最も負荷を分散出来る木を移動
                if ((tree->tree_bitmap[from_DPU] & (1 << i)) != 0ULL) {   // if the tree exists
                    int diff_after = std::abs((batch_ctx.num_keys_for_DPU[from_DPU] - batch_ctx.num_keys_for_tree[from_DPU][i])
                                            - (batch_ctx.num_keys_for_DPU[to_DPU] + batch_ctx.num_keys_for_tree[from_DPU][i]));  // 移動後のクエリ数の差
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
                    if (!(tree->tree_bitmap[to_DPU] & (1 << i))) {  // i番目の木が空いているかどうか
                        /* Enable one migration */
                        to_tree = i;
                        //printf("migration: (%d,%d)->(%d,%d)\n", from_DPU, from_tree, to_DPU, to_tree);
                        /* update batch_ctx */
                        migration_plan[from_DPU][from_tree] = std::make_pair(to_DPU, to_tree);
                        batch_ctx.num_keys_for_DPU[from_DPU] -= batch_ctx.num_keys_for_tree[from_DPU][from_tree];
                        batch_ctx.num_keys_for_DPU[to_DPU] += batch_ctx.num_keys_for_tree[from_DPU][from_tree];
                        batch_ctx.num_keys_for_tree[to_DPU][to_tree] = batch_ctx.num_keys_for_tree[from_DPU][from_tree];
                        batch_ctx.num_keys_for_tree[from_DPU][from_tree] = 0;
                        /* update tree */
                        tree->tree_bitmap[from_DPU] &= ~(1 << from_tree);
                        tree->tree_bitmap[to_DPU] |= (1 << to_tree);
                        tree->key_to_tree_map[tree->tree_to_key_map[from_DPU][from_tree]] = std::make_pair(to_DPU, to_tree);
                        tree->tree_to_key_map[to_DPU][to_tree] = tree->tree_to_key_map[from_DPU][from_tree];
                        tree->tree_to_key_map[from_DPU][from_tree] = -1;

                        num_trees_to_be_migrated--; /* i.e. idx_toDPU--; */
                        migrated_tree_num++; /* for stats */
                        break;
                    }
                }
            }
        }
        idx_fromDPU++;
    }
    return migration_plan;
}

Migration_plan_t& migration_plan_memory_balancing(Migration_plan_t& migration_plan, HostTree* tree, BatchCtx& batch_ctx, int max_num_seats_in_DPU)
{
    extern int migrated_tree_num; /* for stats */
    migrated_tree_num = 0;
    int idx_fromDPU = 0;
    int idx_toDPU = NR_DPUS - 1;
    while (true) {
        int from_DPU = batch_ctx.DPU_idx[idx_fromDPU];
        int to_DPU = batch_ctx.DPU_idx[idx_toDPU];
        if (idx_fromDPU >= idx_toDPU) {
            printf("migration limit because of NR_DPUS, %d migrations\n", migrated_tree_num);
            break;
        }
        if (batch_ctx.DPU_idx[idx_fromDPU] <= max_num_seats_in_DPU) {
            printf("migration planning for memory balancing is finished, %d migrations\n", migrated_tree_num);
            break;
        }
        /* migrate trees from from_DPU to to_DPU */
        while (tree->num_seats_used[from_DPU] > max_num_seats_in_DPU && tree->num_seats_used[to_DPU] < max_num_seats_in_DPU) {
            printf("from_DPU=%d, to_DPU=%d\n", from_DPU, to_DPU);
            int from_tree = 0;
            int min_diff_after = 1 << 30;
            /* determine from_tree */
            for (int i = 0; i < NR_SEATS_IN_DPU; i++) {  // 最も負荷を分散出来る木を移動
                if ((tree->tree_bitmap[from_DPU] & (1 << i)) != 0ULL) {   // if the tree exists
                    int diff_after = std::abs((batch_ctx.num_keys_for_DPU[from_DPU] - batch_ctx.num_keys_for_tree[from_DPU][i])
                                            - (batch_ctx.num_keys_for_DPU[to_DPU] + batch_ctx.num_keys_for_tree[from_DPU][i]));  // 移動後のクエリ数の差
                    if (diff_after < min_diff_after) {
                        from_tree = i;
                        min_diff_after = diff_after;
                    }
                }
            }
            /* determine to_tree */
            int to_tree;
            for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
                if (!(tree->tree_bitmap[to_DPU] & (1 << i))) {  // i番目の木が空いているかどうか
                    /* Enable one migration */
                    to_tree = i;
                    //printf("migration: (%d,%d)->(%d,%d)\n", from_DPU, from_tree, to_DPU, to_tree);
                    /* update batch_ctx */
                    migration_plan[from_DPU][from_tree] = std::make_pair(to_DPU, to_tree);
                    batch_ctx.num_keys_for_DPU[from_DPU] -= batch_ctx.num_keys_for_tree[from_DPU][from_tree];
                    batch_ctx.num_keys_for_DPU[to_DPU] += batch_ctx.num_keys_for_tree[from_DPU][from_tree];
                    batch_ctx.num_keys_for_tree[to_DPU][to_tree] = batch_ctx.num_keys_for_tree[from_DPU][from_tree];
                    batch_ctx.num_keys_for_tree[from_DPU][from_tree] = 0;
                    /* update tree */
                    tree->tree_bitmap[from_DPU] &= ~(1 << from_tree);
                    tree->tree_bitmap[to_DPU] |= (1 << to_tree);
                    tree->key_to_tree_map[tree->tree_to_key_map[from_DPU][from_tree]] = std::make_pair(to_DPU, to_tree);
                    tree->tree_to_key_map[to_DPU][to_tree] = tree->tree_to_key_map[from_DPU][from_tree];
                    tree->tree_to_key_map[from_DPU][from_tree] = -1;
                    tree->num_seats_used[from_DPU]--;
                    tree->num_seats_used[to_DPU]++;

                    migrated_tree_num++; /* for stats */
                    break;
                }
            }
            bool flag = false; /* whether (to_DPU is full) or (there is no need to move tree from from_DPU)  */
            if (tree->num_seats_used[from_DPU] <= max_num_seats_in_DPU) { /* to_DPU is full */
                idx_fromDPU++;
                flag = true;
            }
            if (tree->num_seats_used[to_DPU] >= max_num_seats_in_DPU) { /* there is no need to move tree from from_DPU */
                idx_toDPU--;
                flag = true;
            }
            if (flag) break;
        }
    }
    return migration_plan;
}

Migration_plan_t& migration_plan_get(Migration_plan_t& migration_plan, HostTree* tree, BatchCtx& batch_ctx, int num_migration)
{
    printf("migration_plan_get\n");

    /* DPUをクエリの数が多い順に並び替える */
    for (int i = 0; i < NR_DPUS; i++) {
        batch_ctx.DPU_idx[i] = i;
    }
    std::sort(batch_ctx.DPU_idx, batch_ctx.DPU_idx + NR_DPUS, comp_idx(batch_ctx.num_keys_for_DPU));

    return migration_plan_query_balancing(migration_plan, tree, batch_ctx, num_migration, NR_SEATS_IN_DPU);
}

Migration_plan_t& migration_plan_insert(Migration_plan_t& migration_plan, HostTree* tree, BatchCtx& batch_ctx, int num_migration)
{
    printf("migration_plan_insert\n");

    /* DPUを木の数が多い順に並び替える */
    for (int i = 0; i < NR_DPUS; i++) {
        batch_ctx.DPU_idx[i] = i;
    }
    std::sort(batch_ctx.DPU_idx, batch_ctx.DPU_idx + NR_DPUS, comp_idx(tree->num_seats_used));

    migration_plan_memory_balancing(migration_plan, tree, batch_ctx, MAX_NUM_SEATS_BEFORE_INSERT);

    /* DPUをクエリの数が多い順に並び替える */
    for (int i = 0; i < NR_DPUS; i++) {
        batch_ctx.DPU_idx[i] = i;
    }
    std::sort(batch_ctx.DPU_idx, batch_ctx.DPU_idx + NR_DPUS, comp_idx(batch_ctx.num_keys_for_DPU));
    return migration_plan_query_balancing(migration_plan, tree, batch_ctx, num_migration, MAX_NUM_SEATS_BEFORE_INSERT);
}

void send_nodes_from_dpu_to_dpu(int from_DPU, int from_tree, int to_DPU, int to_tree, dpu_set_t set, dpu_set_t dpu)
{
    uint64_t task;
    /* grobal variables difined in host.cpp */
    extern int each_dpu;
    extern BPTreeNode nodes_buffer[MAX_NUM_NODES_IN_SEAT];
    extern uint64_t nodes_num;
    DPU_FOREACH(set, dpu, each_dpu)
    {
        //printf("ed = %d\n", each_dpu);
        if (each_dpu == from_DPU) {
            task = (((uint64_t)from_tree) << 32) | TASK_FROM;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            // TODO: nodes_bufferを増やし、並列にmigrationを行う
            DPU_ASSERT(dpu_copy_from(dpu, "tree_transfer_num", 0, &nodes_num, sizeof(uint64_t)));
            DPU_ASSERT(dpu_copy_from(dpu, "tree_transfer_buffer", 0, &nodes_buffer, nodes_num * sizeof(BPTreeNode)));
            break;
        }
    }
    DPU_FOREACH(set, dpu, each_dpu)
    {
        // printf("ed = %d\n", each_dpu);
        if (each_dpu == to_DPU) {
            task = (((uint64_t)to_tree) << 32) | TASK_TO;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_num));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "tree_transfer_num", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_buffer));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "tree_transfer_buffer", 0, nodes_num * sizeof(BPTreeNode), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            break;
        }
    }
}

/* execute migration according to migration_plan */
void do_migration(Migration_plan_t& migration_plan, dpu_set_t set, dpu_set_t dpu)
{
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
            auto& to = migration_plan[i][j];
            if (to.first != -1 && to.first != i) {
                send_nodes_from_dpu_to_dpu(i, j, to.first, to.second, set, dpu);
                migration_plan[i][j] = std::make_pair(-1, -1);
            }
        }
    }
}
