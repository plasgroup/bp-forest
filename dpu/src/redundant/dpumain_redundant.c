#include "bplustree_redundant.h"
#include "common.h"
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
__mram int end_idx[NUM_BPTREE_IN_DPU];
__mram int batch_num;
#ifdef VARY_REQUESTNUM
__mram dpu_experiment_var_t expvars;
__mram dpu_stats_t stats[NUM_VARS];
#endif
__mram dpu_result_t result;
__mram_ptr void* ptr;
#ifdef STATS_ON
__mram uint64_t nb_cycles_get;
__mram uint64_t nb_cycles_insert;
#endif
#ifdef DEBUG_ON
__mram_ptr void* getval;
#endif
int main()
{
    int tid = me();
    // for(int i = 0; i < request_buffer[me()].num_req; i++){
    //     printf("[tasklet %d] key:%ld\n",tid, request_buffer[tid].key[i]);
    // }
    if (tid == 0) {
        batch_num++;
    }
    barrier_wait(&my_barrier);
    if (batch_num == 1) {
        // printf("[tasklet %d] initializing BPTree\n", tid);
        init_BPTree();
    }
    // printf("[tasklet %d] %d times insertion\n",tid,
    // request_buffer[tid].num_req);
    int index = 0;
    value_ptr_t volatile res[NR_TASKLETS];
    barrier_wait(&my_barrier);

    /* insertion */
    if (tid == 0) {
        for (int index = 0; index < end_idx[NUM_BPTREE_IN_DPU - 1]; index++) {
            BPTreeInsert(request_buffer[index].key,
                request_buffer[index].write_val_ptr);
        }
    }
    barrier_wait(&my_barrier);
#ifdef PRINT_DEBUG
    /* sequential execution using semaphore */
    sem_take(&my_semaphore);
    printf("total num of nodes = %d\n",
        BPTree_GetNumOfNodes());
    printf("height = %d\n", BPTree_GetHeight());
    sem_give(&my_semaphore);
    barrier_wait(&my_barrier);
#endif
#ifdef PRINT_ON
    sem_take(&my_semaphore);
    printf("\n");
    printf("Printing Nodes of tasklet#%d...\n", tid);
    printf("===========================================\n");
    BPTreePrintAll(tid);
    printf("===========================================\n");
    sem_give(&my_semaphore);
    barrier_wait(&my_barrier);
#endif
/* write intensive */
#if WORKLOAD = W50R50
    for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
        res[tid] = BPTreeGet(request_buffer[index].key);
    }
#endif
/* read intensive */
#if WORKLOAD = W05R95
    for (int i = 0; i < 19; i++) {
        for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
            res[tid] = BPTreeGet(request_buffer[index].key);
        }
    }
#endif
    barrier_wait(&my_barrier);
    return 0;
}