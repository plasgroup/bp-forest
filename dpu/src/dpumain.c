#include "bplustree.h"
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
    printf("[tasklet %d/%d] end_index:%d\n", tid, NR_TASKLETS, end_idx[tid]);
    // for(int i = 0; i < request_buffer[me()].num_req; i++){
    //     printf("[tasklet %d] key:%ld\n",tid, request_buffer[tid].key[i]);
    // }
    if (tid == 0) {

        // printf("size of request_buffer:%u\n", sizeof(request_buffer));
        // printf("using up to %d MB for value data, can store up to %d values per
        // tasklet\n", VALUE_DATA_SIZE, MAX_VALUE_NUM); printf("using up to %d MB for
        // node data, node size = %u, can store up to %d nodes per tasklet\n",
        // NODE_DATA_SIZE,sizeof(BPTreeNode), MAX_NODE_NUM);
        // printf("batch_num:%d\n", batch_num);
        batch_num++;
    }
    barrier_wait(&my_barrier);
    if (batch_num == 1) {
        // printf("[tasklet %d] initializing BPTree\n", tid);
        init_BPTree(tid);
    }
    // printf("[tasklet %d] %d times insertion\n",tid,
    // request_buffer[tid].num_req);
    int index = 0;
    value_ptr_t volatile res[NR_TASKLETS];
#ifdef VARY_REQUESTNUM
    for (int i = 0; i < NUM_VARS; i++) {
#ifdef STATS_ON
        perfcounter_config(COUNT_CYCLES, true);
#endif
        for (; index < expvars.vars[i] - expvars.gap; index++) {
            BPTreeInsert(request_buffer.key[index], request_buffer.write_val[index]);
        }
#ifdef STATS_ON
        nb_cycles_insert += perfcounter_get();
#endif
#ifdef STATS_ON
        perfcounter_config(COUNT_CYCLES, true);
#endif
        for (; index < expvars.vars[i] + expvars.gap; index++) {
            BPTreeInsert(request_buffer.key[index], request_buffer.write_val[index]);
        }
#ifdef STATS_ON
        stats[i].cycle_insert = perfcounter_get();
        nb_cycles_insert += stats[i].cycle_insert;
        stats[i].x = expvars.vars[i];
#endif
#ifdef STATS_ON
        perfcounter_config(COUNT_CYCLES, true);
#endif
        for (index = expvars.vars[i] - expvars.gap;
             index < expvars.vars[i] + expvars.gap; index++) {
            res = BPTreeGet(request_buffer.key[index]);
        }
#ifdef STATS_ON
        stats[i].cycle_get = perfcounter_get();
#endif
    }
#ifdef STATS_ON
    perfcounter_config(COUNT_CYCLES, true);
#endif
    for (; index < request_buffer.num_req; index++) {
        BPTreeInsert(request_buffer.key[index], request_buffer.write_val[index]);
    }
#ifdef STATS_ON
    nb_cycles_insert += perfcounter_get();
#endif
    printf("insertion finished\n");
#endif
#ifndef VARY_REQUESTNUM
    barrier_wait(&my_barrier);
#ifdef STATS_ON
    perfcounter_config(COUNT_CYCLES, true);
#endif
    /* insertion */
    for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
        BPTreeInsert(request_buffer[index].key,
            request_buffer[index].write_val_ptr, tid);
    }
#ifdef STATS_ON
    nb_cycles_insert = perfcounter_get();
    barrier_wait(&my_barrier);
    if (tid == 0) {
        // printf("insertion finished\n");
    }
#endif

#endif
#ifdef PRINT_DEBUG
    /* sequential execution using semaphore */
    sem_take(&my_semaphore);
    printf("[tasklet %d] total num of nodes = %d\n", tid,
        BPTree_GetNumOfNodes(tid));
    printf("[tasklet %d] height = %d\n", tid, BPTree_GetHeight(tid));
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

#ifdef STATS_ON
    printf("[tasklet %d] %d times search\n", tid, request_buffer[tid].num_req);
    barrier_wait(&my_barrier);
    perfcounter_config(COUNT_CYCLES, true);
#endif
    for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
        res[tid] = BPTreeGet(request_buffer[index].key, tid);
    }
#ifdef STATS_ON
    nb_cycles_get = perfcounter_get();

    if (tid == 0) {
        printf("search finished\n");
    }
    barrier_wait(&my_barrier);
#endif
    // for(uint32_t i = 0; i < request_buffer.num_req; i++) {
    //     res = BPTreeGet(request_buffer.key[i]);
    //     //memcpy(result.val, BPTreeGet(request_buffer.key[i]), MAXIMAM_LENGTH);
    //     #ifdef DEBUG_ON
    //     //     if(getval!= NULL){
    //     //         memcpy(wram_str, getval, MAXIMAM_LENGTH);
    //     //         printf(", value: \"%s\"\n", wram_str);
    //     //     } else {printf(", value: NULL\n");}

    //     #endif
    // }
    // #ifdef STATS_ON
    //     nb_cycles_get = perfcounter_get();
    // #endif

#ifdef STATS_ON
    if (tid == 0) {
        printf("nb_cycles_insert = %lu\n", nb_cycles_insert);
        // printf("cycles/insert = %d\n", nb_cycles_insert/request_buffer.num_req);
        printf("nb_cycles_get = %lu\n", nb_cycles_get);
        // printf("cycles/get = %d\n", nb_cycles_get/request_buffer.num_req);
    }
    if (tid == 0)
        printf("\n");
#endif
    return 0;
}
