#include "bplustree.h"
#include "cabin.h"
#include "common.h"
#include "merge_phase.h"
#include "split_phase.h"
#include <assert.h>
#include <barrier.h>
#include <defs.h>
#include <mram.h>
#include <perfcounter.h>
#include <sem.h>
#include <stdio.h>
BARRIER_INIT(my_barrier, NR_TASKLETS);

#include <barrier.h>
SEMAPHORE_INIT(my_semaphore, 1);

__mram each_request_t request_buffer[MAX_REQ_NUM_IN_A_DPU];
__mram int end_idx[NR_SEATS_IN_DPU];
__mram dpu_results_t results;
__mram_ptr void* ptr;
__mram KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
__mram uint64_t tree_transfer_num;
__mram split_info_t split_result[NR_SEATS_IN_DPU];
__mram merge_info_t merge_info;
__mram dpu_init_param_t dpu_init_param[NR_SEATS_IN_DPU];

__host uint64_t task_no;
__host int num_kvpairs_in_seat[NR_SEATS_IN_DPU];
int queries_per_tasklet;
seat_id_t current_tree;
uint32_t task;
int num_invoked = 0;
#ifdef PRINT_DISTRIBUTION
__host int numofnodes[NR_SEATS_IN_DPU];
#endif
#ifdef DEBUG_ON
__mram_ptr void* getval;
#endif

int main()
{
    int tid = me();
    if (tid == 0) {
        num_invoked++;
        task = (uint32_t) TASK_GET_ID(task_no);
#ifdef DEBUG_ON
        for (int t = 0; t < NR_SEATS_IN_DPU; t++) {
            printf("end_idx[%d] = %d\n", t, end_idx[t]);
        }
#endif
    }
    barrier_wait(&my_barrier);
    switch (task) {
    case TASK_INIT: {
        if (tid == 0) {
            Cabin_init();
            for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++) {
                __mram_ptr dpu_init_param_t* param = &dpu_init_param[seat_id];
                if (param->use != 0) {
                    Cabin_allocate_seat(seat_id);
                    init_BPTree(seat_id);
                    if (param->end_inclusive < param->start)
                        continue;
                    key_int64_t k = param->start;
                    while (true) {
                        value_ptr_t v = k;
                        BPTreeInsert(k, v, seat_id);
                        if (param->end_inclusive - k < param->interval)
                            break;
                        k += param->interval;
                    }
                }
            }
        }
        break;
    }
    case TASK_INSERT: {
        if (tid == 0) {
            queries_per_tasklet = end_idx[NR_SEATS_IN_DPU - 1] / NR_TASKLETS;
            current_tree = 0;
            printf("insert task\n");
        }
        barrier_wait(&my_barrier);
        /* determine which tree to execute */
        /* TODO: better load balancing */
        seat_id_t start_tree;
        seat_id_t end_tree;
        int num_queries = 0;
        for (int tasklet = 0; tasklet < NR_TASKLETS; tasklet++) {
            if (tid == tasklet) {
                start_tree = current_tree;
                while (num_queries < queries_per_tasklet && current_tree < NR_SEATS_IN_DPU) {
                    current_tree++;
                    if (current_tree == 0)
                        num_queries += end_idx[0];
                    else
                        num_queries += end_idx[current_tree] - end_idx[current_tree - 1];
                }
                end_tree = current_tree;
            }
            barrier_wait(&my_barrier);
        }
#ifdef PRINT_DEBUG
        sem_take(&my_semaphore);
        printf("[tasklet %d] inserting tree [%d, %d)\n", tid, start_tree, end_tree);
        sem_give(&my_semaphore);
        barrier_wait(&my_barrier);
#endif
        /* execution */
        for (seat_id_t tree = start_tree; tree < end_tree; tree++) {
            if (Seat_is_used(tree)) {
                for (int index = tree == 0 ? 0 : end_idx[tree - 1]; index < end_idx[tree]; index++) {
                    BPTreeInsert(request_buffer[index].key, request_buffer[index].write_val_ptr, tree);
                }
#ifdef PRINT_DEBUG
                sem_take(&my_semaphore);
                printf("[tasklet %d] inserted seat %d\n", tid, tree);
                printf("[tasklet %d] total num of nodes of seat %d = %d\n", tid, tree, Seat_get_n_nodes(tree));
                printf("[tasklet %d] height of seat %d = %d\n", tid, tree, Seat_get_height(tree));
                printf("[tasklet %d] num of KV-Pairs of seat %d = %d\n", tid, tree, BPTree_Serialize(tree, tree_transfer_buffer));
                sem_give(&my_semaphore);
#endif
            }
        }


#ifdef DEBUG_ON
        barrier_wait(&my_barrier);
        // DPU側で負荷分散する
        int start_index = queries_per_tasklet * tid;
        int end_index = tid == NR_TASKLETS - 1 ? end_idx[NR_SEATS_IN_DPU - 1] : queries_per_tasklet * (tid + 1);
        int tree = 0;
        while (end_idx[tree] <= start_index) {
            tree++;
        }
        int index = start_index;
        while (true) {
            if (end_idx[tree] < end_index) { /* not last tree */
                for (; index < end_idx[tree]; index++) {
                    results.get.results[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                tree++;
            } else { /* last tree */
                for (; index < end_index; index++) {
                    results.get.results[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                break;
            }
        }
#endif
        /* split large trees */
        barrier_wait(&my_barrier);
        if (tid == 0) {
            split_phase();
#ifdef PRINT_DISTRIBUTION
            for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++) {
                if (Seat_is_used(seat_id)) 
                    numofnodes[seat_id] = Seat_get_n_nodes(seat_id);
                else 
                    numofnodes[seat_id] = 0;
            }
#endif
#ifdef PRINT_DEBUG
            for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++) {
                if (Seat_is_used(seat_id)) {
                    printf("[after split] total num of nodes of seat %d = %d\n", seat_id, Seat_get_n_nodes(seat_id));
                    printf("[after split] height of seat %d = %d\n", seat_id, Seat_get_height(seat_id));
                    printf("[after split] num of KV-Pairs of seat %d = %d\n", seat_id, BPTree_Serialize(seat_id, tree_transfer_buffer));
                }
            }
#endif
        }
        break;
    }
    case TASK_GET: {
        if (tid == 0) {
            queries_per_tasklet = end_idx[NR_SEATS_IN_DPU - 1] / NR_TASKLETS;
        }
        barrier_wait(&my_barrier);
        // DPU側で負荷分散する
        int start_index = queries_per_tasklet * tid;
        int end_index = tid == NR_TASKLETS - 1 ? end_idx[NR_SEATS_IN_DPU - 1] : queries_per_tasklet * (tid + 1);
        int tree = 0;
        while (end_idx[tree] <= start_index) {
            tree++;
        }
        int index = start_index;
        while (true) {
            if (end_idx[tree] < end_index) { /* not last tree */
                for (; index < end_idx[tree]; index++) {
                    results.get.results[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                tree++;
            } else { /* last tree */
                for (; index < end_index; index++) {
                    results.get.results[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                break;
            }
        }
#ifdef PRINT_DISTRIBUTION
            for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++) {
                if (Seat_is_used(seat_id)) 
                    numofnodes[seat_id] = Seat_get_n_nodes(seat_id);
                else 
                    numofnodes[seat_id] = 0;
            }
#endif
        break;
    }
    case TASK_SUCC: {
        if (tid == 0) {
            queries_per_tasklet = end_idx[NR_SEATS_IN_DPU - 1] / NR_TASKLETS;
        }
        barrier_wait(&my_barrier);
        // DPU側で負荷分散する
        int start_index = queries_per_tasklet * tid;
        int end_index = tid == NR_TASKLETS - 1 ? end_idx[NR_SEATS_IN_DPU - 1] : queries_per_tasklet * (tid + 1);
        int tree = 0;
        while (end_idx[tree] <= start_index) {
            tree++;
        }
        int index = start_index;
        while (true) {
            if (end_idx[tree] < end_index) { /* not last tree */
                for (; index < end_idx[tree]; index++) {
                    KVPair succ = BPTreeSucc(request_buffer[index].key, tree);
                    results.succ.results[index].succ_key = succ.key;
                    results.succ.results[index].succ_val_ptr = succ.value;
                }
                tree++;
            } else { /* last tree */
                for (; index < end_index; index++) {
                    KVPair succ = BPTreeSucc(request_buffer[index].key, tree);
                    results.succ.results[index].succ_key = succ.key;
                    results.succ.results[index].succ_val_ptr = succ.value;
                }
                break;
            }
        }
        break;
    }
    case TASK_FROM: {
        if (tid == 0) {
            seat_id_t seat_id = (task_no >> 32);
            tree_transfer_num = BPTree_Serialize(seat_id, tree_transfer_buffer);
            //printf("TASK FROM, tree_transfer num: %lu\n", tree_transfer_num);
            Cabin_release_seat(seat_id);
        }
        break;
    }
    case TASK_TO: {
        if (tid == 0) {
            seat_id_t seat_id = (task_no >> 32);
            Cabin_allocate_seat(seat_id);
            init_BPTree(seat_id);
            //printf("TASK TO, tree_transfer num: %lu\n", tree_transfer_num);
            BPTree_Deserialize(seat_id, tree_transfer_buffer, 0, tree_transfer_num);
            //printf("TASK TO, after deserialize: %d\n", num_kvpairs_in_seat[seat_id]);
        }
        break;
    }
    case TASK_MERGE: {
        if (tid == 0) {
            merge_phase();
        }
        break;
    }
    default: {
        printf("no such a task: task %d\n", task);
        return -1;
    }
    }

#ifdef PRINT_ON
    // sem_take(&my_semaphore);
    // printf("\n");
    // printf("Printing Nodes of tasklet#%d...\n", tid);
    // printf("===========================================\n");
    // BPTreePrintAll(tid);
    // printf("===========================================\n");
    // sem_give(&my_semaphore);
    // barrier_wait(&my_barrier);
#endif
    return 0;
}