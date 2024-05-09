#include "allocator.h"
#include "binary_search.h"
#include "bplustree.h"
#include "common.h"
#include "sort.h"
#include "workload_types.h"

#include <assert.h>
#include <attributes.h>
#include <barrier.h>
#include <defs.h>
#include <mram.h>
#include <mutex.h>
#include <perfcounter.h>

#include <stdint.h>
#include <stdio.h>

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__mram_noinit dpu_init_param_t dpu_init_param;
__mram_noinit each_request_t request_buffer[MAX_REQ_NUM_IN_A_DPU];
__mram_noinit dpu_results_t results;

#ifndef BULK_MIGRATION
__host migration_ratio_param_t migration_ratio_param;
__host migration_key_param_t migration_key_param;
#endif
__host migration_pairs_param_t migration_pairs_param;
__mram_noinit key_int64_t migrated_keys[MAX_NUM_NODES_IN_SEAT * MAX_NR_PAIRS];
__mram_noinit value_ptr_t migrated_values[MAX_NUM_NODES_IN_SEAT * MAX_NR_PAIRS];

__host unsigned end_idx;
__host uint64_t task_no;

int main()
{
    unsigned tid = me();
    const uint32_t task = (uint32_t)TASK_GET_ID(task_no);
    if (tid == 0) {
#ifdef DEBUG_ON
        printf("end_idx = %d\n", end_idx);
#endif
    }
    switch (task) {
    case TASK_INIT: {
        if (tid == 0) {
            init_BPTree();
            if (dpu_init_param.end_inclusive >= dpu_init_param.start) {
                key_int64_t k = dpu_init_param.start;
                while (true) {
                    value_ptr_t v = k;
                    BPTreeInsert(k, v);
                    if (dpu_init_param.end_inclusive - k < dpu_init_param.interval)
                        break;
                    k += dpu_init_param.interval;
                }
            }
        }
        break;
    }
        //     case TASK_INSERT: {
        //         if (tid == 0) {
        //             queries_per_tasklet = end_idx[NR_SEATS_IN_DPU - 1] / NR_TASKLETS;
        //             current_tree = 0;
        //             printf("insert task\n");
        //         }
        //         barrier_wait(&my_barrier);
        //         /* determine which tree to execute */
        //         /* TODO: better load balancing */
        //         seat_id_t start_tree;
        //         seat_id_t end_tree;
        //         int num_queries = 0;
        //         for (int tasklet = 0; tasklet < NR_TASKLETS; tasklet++) {
        //             if (tid == tasklet) {
        //                 start_tree = current_tree;
        //                 while (num_queries < queries_per_tasklet && current_tree < NR_SEATS_IN_DPU) {
        //                     current_tree++;
        //                     if (current_tree == 0)
        //                         num_queries += end_idx[0];
        //                     else
        //                         num_queries += end_idx[current_tree] - end_idx[current_tree - 1];
        //                 }
        //                 end_tree = current_tree;
        //             }
        //             barrier_wait(&my_barrier);
        //         }
        // #ifdef PRINT_DEBUG
        //         mutex_lock(my_mutex);
        //         printf("[tasklet %d] inserting tree [%d, %d)\n", tid, start_tree, end_tree);
        //         mutex_unlock(my_mutex);
        //         barrier_wait(&my_barrier);
        // #endif
        //         /* execution */
        //         for (seat_id_t tree = start_tree; tree < end_tree; tree++) {
        //             if (Seat_is_used(tree)) {
        //                 for (int index = tree == 0 ? 0 : end_idx[tree - 1]; index < end_idx[tree]; index++) {
        //                     BPTreeInsert(request_buffer[index].key, request_buffer[index].write_val_ptr, tree);
        //                 }
        // #ifdef PRINT_DEBUG
        //                 mutex_lock(my_mutex);
        //                 printf("[tasklet %d] inserted seat %d\n", tid, tree);
        //                 printf("[tasklet %d] total num of nodes of seat %d = %d\n", tid, tree, Seat_get_n_nodes(tree));
        //                 printf("[tasklet %d] height of seat %d = %d\n", tid, tree, Seat_get_height(tree));
        //                 printf("[tasklet %d] num of KV-Pairs of seat %d = %d\n", tid, tree, BPTree_Serialize(tree, tree_transfer_buffer));
        //                 mutex_unlock(my_mutex);
        // #endif
        //             }
        //         }


        // #ifdef DEBUG_ON
        //         barrier_wait(&my_barrier);
        //         // DPU側で負荷分散する
        //         int start_index = queries_per_tasklet * tid;
        //         int end_index = tid == NR_TASKLETS - 1 ? end_idx[NR_SEATS_IN_DPU - 1] : queries_per_tasklet * (tid + 1);
        //         int tree = 0;
        //         while (end_idx[tree] <= start_index) {
        //             tree++;
        //         }
        //         int index = start_index;
        //         while (true) {
        //             if (end_idx[tree] < end_index) { /* not last tree */
        //                 for (; index < end_idx[tree]; index++) {
        //                     results.get[index].get_result = BPTreeGet(request_buffer[index].key, tree);
        //                 }
        //                 tree++;
        //             } else { /* last tree */
        //                 for (; index < end_index; index++) {
        //                     results.get[index].get_result = BPTreeGet(request_buffer[index].key, tree);
        //                 }
        //                 break;
        //             }
        //         }
        // #endif
        //         /* split large trees */
        //         barrier_wait(&my_barrier);
        //         if (tid == 0) {
        //             split_phase();
        // #ifdef PRINT_DEBUG
        //             for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++) {
        //                 if (Seat_is_used(seat_id)) {
        //                     printf("[after split] total num of nodes of seat %d = %d\n", seat_id, Seat_get_n_nodes(seat_id));
        //                     printf("[after split] height of seat %d = %d\n", seat_id, Seat_get_height(seat_id));
        //                     printf("[after split] num of KV-Pairs of seat %d = %d\n", seat_id, BPTree_Serialize(seat_id, tree_transfer_buffer));
        //                 }
        //             }
        // #endif
        //         }
        //         break;
        //     }
    case TASK_GET: {
        // DPU側で負荷分散する
        const unsigned start_index = end_idx * tid / NR_TASKLETS;
        const unsigned end_index = end_idx * (tid + 1) / NR_TASKLETS;

        for (unsigned idx_req = start_index; idx_req < end_index; idx_req++) {
            const key_int64_t key = request_buffer[idx_req].key;
            results.get[idx_req].key = key;
            results.get[idx_req].get_result = BPTreeGet(key);
        }
        break;
    }
    // case TASK_SUCC: {
    //     if (tid == 0) {
    //         queries_per_tasklet = end_idx[NR_SEATS_IN_DPU - 1] / NR_TASKLETS;
    //     }
    //     barrier_wait(&my_barrier);
    //     // DPU側で負荷分散する
    //     int start_index = queries_per_tasklet * tid;
    //     int end_index = tid == NR_TASKLETS - 1 ? end_idx[NR_SEATS_IN_DPU - 1] : queries_per_tasklet * (tid + 1);
    //     int tree = 0;
    //     while (end_idx[tree] <= start_index) {
    //         tree++;
    //     }
    //     int index = start_index;
    //     while (true) {
    //         if (end_idx[tree] < end_index) { /* not last tree */
    //             for (; index < end_idx[tree]; index++) {
    //                 KVPair succ = BPTreeSucc(request_buffer[index].key, tree);
    //                 results.succ[index].succ_key = succ.key;
    //                 results.succ[index].succ_val_ptr = succ.value;
    //             }
    //             tree++;
    //         } else { /* last tree */
    //             for (; index < end_index; index++) {
    //                 KVPair succ = BPTreeSucc(request_buffer[index].key, tree);
    //                 results.succ[index].succ_key = succ.key;
    //                 results.succ[index].succ_val_ptr = succ.value;
    //             }
    //             break;
    //         }
    //     }
    //     break;
    // }
    case TASK_FROM: {
        if (tid == 0) {
#ifdef BULK_MIGRATION
            migration_pairs_param = num_kvpairs;
            BPTreeSerialize(&migrated_keys[0], &migrated_values[0]);
#else  /* BULK_MIGRATION */
            const migration_ratio_param_t ratio_param = migration_ratio_param;
            assert(ratio_param.left_npairs_ratio_x2147483648 + ratio_param.right_npairs_ratio_x2147483648 <= 2147483648u);

            migration_pairs_param_t npairs = (migration_pairs_param_t){
                (uint32_t)((uint64_t)ratio_param.left_npairs_ratio_x2147483648 * num_kvpairs / 2147483648u),
                num_kvpairs - (uint32_t)((2147483648u - (uint64_t)ratio_param.right_npairs_ratio_x2147483648) * num_kvpairs / 2147483648u),
            };

            migration_key_param_t delimiters;
            if (ratio_param.left_npairs_ratio_x2147483648 != 2147483648u) {
                delimiters.left_delim_key = BPTreeNthKeyFromLeft(npairs.num_left_kvpairs);
            }
            if (ratio_param.right_npairs_ratio_x2147483648 != 0u) {
                delimiters.right_delim_key = BPTreeNthKeyFromRight(npairs.num_right_kvpairs - 1u);
            }
            migration_key_param = delimiters;

            switch (ratio_param.left_npairs_ratio_x2147483648) {
            case 0u:
                break;
            case 2147483648u:
                npairs.num_left_kvpairs = num_kvpairs;
                BPTreeSerialize(&migrated_keys[0], &migrated_values[0]);
                break;
            default:
                npairs.num_left_kvpairs = BPTreeExtractFirstPairs(&migrated_keys[0], &migrated_values[0], delimiters.left_delim_key);
                break;
            }

            // assert(ratio_param.right_npairs_ratio_x2147483648 == 0u);
            switch (ratio_param.right_npairs_ratio_x2147483648) {
            case 0u:
                break;
            case 2147483648u:
                npairs.num_right_kvpairs = num_kvpairs;
                BPTreeSerialize(&migrated_keys[0], &migrated_values[0]);
                break;
            default:
                // npairs.num_right_kvpairs = BPTreeExtractLastPairs(&migrated_keys[npairs.num_left_kvpairs], &migrated_values[npairs.num_left_kvpairs], delimiters.right_delim_key);
                break;
            }

            migration_pairs_param = npairs;
#endif /* BULK_MIGRATION */
        }
        break;
    }
    case TASK_TO: {
        if (tid == 0) {
#ifdef BULK_MIGRATION
            BPTreeInsertSortedPairsToLeft(&migrated_keys[0], &migrated_values[0], migration_pairs_param);
#else /* BULK_MIGRATION */
            BPTreeInsertSortedPairsToLeft(&migrated_keys[0], &migrated_values[0], migration_pairs_param.num_left_kvpairs);
            BPTreeInsertSortedPairsToRight(&migrated_keys[migration_pairs_param.num_left_kvpairs], &migrated_values[migration_pairs_param.num_left_kvpairs], migration_pairs_param.num_right_kvpairs);
#endif /* BULK_MIGRATION */
        }
        break;
    }
    default: {
        printf("no such a task: task %d\n", task);
        return -1;
    }
    }

#if defined(PRINT_ON) && defined(DEBUG_ON)
    barrier_wait(&my_barrier);
    if (tid == 0) {
        printf("\n");
        printf("Printing Nodes...\n");
        printf("===========================================\n");
        BPTreePrintKeys();
        if (!BPTreeCheckStructure())
            BPTreePrintAll();
        printf("===========================================\n");
    }
#endif
    return 0;
}
