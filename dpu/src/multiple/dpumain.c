#include "bplustree.h"
#include "cabin.h"
#include "common.h"
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
__mram each_result_t result[MAX_REQ_NUM_IN_A_DPU];
__mram_ptr void* ptr;
__mram KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
__mram uint64_t tree_transfer_num;
__mram split_info_t split_result[NR_SEATS_IN_DPU];
__host uint64_t task_no;
int queries_per_tasklet;
seat_id_t current_tree;
uint32_t task;
int num_invoked = 0;

#ifdef DEBUG_ON
__mram_ptr void* getval;
#endif

int main()
{
    int tid = me();
    if (tid == 0) {
        num_invoked++;
        printf("%dth invocation\n", num_invoked);
        task = (uint32_t)task_no;
        printf("task_no: %016lx\n", task_no);
        printf("split threshold: %d\n", SPLIT_THRESHOLD);
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
            for (seat_id_t seat_id = 0; seat_id < NUM_INIT_TREES_IN_DPU; seat_id++) {
                Cabin_allocate_seat(seat_id);
                init_BPTree(seat_id);
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
        int end_index = tid == NR_TASKLETS - 1 ? end_idx[tid] : queries_per_tasklet * (tid + 1);
        int tree = 0;
        while (end_idx[tree] <= start_index) {
            tree++;
        }
        int index = start_index;
        while (true) {
            if (end_idx[tree] < end_index) { /* not last tree */
                for (; index < end_idx[tree]; index++) {
                    result[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                tree++;
            } else { /* last tree */
                for (; index < end_index; index++) {
                    result[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                break;
            }
        }
#endif
        /* split large trees */
        barrier_wait(&my_barrier);
        if (tid == 0) {
            split_phase();
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
    }
    case TASK_GET: {
        if (tid == 0) {
            queries_per_tasklet = end_idx[NR_SEATS_IN_DPU - 1] / NR_TASKLETS;
        }
        barrier_wait(&my_barrier);
        // DPU側で負荷分散する
        int start_index = queries_per_tasklet * tid;
        int end_index = tid == NR_TASKLETS - 1 ? end_idx[tid] : queries_per_tasklet * (tid + 1);
        int tree = 0;
        while (end_idx[tree] <= start_index) {
            tree++;
        }
        int index = start_index;
        while (true) {
            if (end_idx[tree] < end_index) { /* not last tree */
                for (; index < end_idx[tree]; index++) {
                    // result[index].get_result = BPTreeGet(request_buffer[index].key, tree);
                }
                tree++;
            } else { /* last tree */
                for (; index < end_index; index++) {
                    result[index].get_result = BPTreeGet(request_buffer[index].key, tree);
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
            printf("tree_transfer num: %lu, size of buffer = %d\n", tree_transfer_num, MAX_NUM_NODES_IN_SEAT * MAX_CHILD);
            Cabin_release_seat(seat_id);
        }
        break;
    }
    case TASK_TO: {
        if (tid == 0) {
            seat_id_t seat_id = (task_no >> 32);
            Cabin_allocate_seat(seat_id);
            init_BPTree(seat_id);
            BPTree_Deserialize(seat_id, tree_transfer_buffer, 0, tree_transfer_num);
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