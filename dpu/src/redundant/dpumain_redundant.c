#include "bplustree_redundant.h"
#include "common.h"
#include "tasks.h"
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

int traverse_and_print_nodes(MBPTptr node)
{
    int elems = node->numKeys;
    if (!node->isLeaf) {
        for (int i = 0; i <= node->numKeys; i++) {
            elems += traverse_and_print_nodes(node->ptrs.inl.children[i]);
        }
    }
    showNode(node, 0);
    return elems;
}

__mram BPTreeNode nodes_transfer_buffer[MAX_NODE_NUM];
__mram uint64_t nodes_transfer_num;
__mram uint64_t task_no;
__mram uint64_t transfer_tree_index;

void traverse_and_copy_nodes(MBPTptr node)
{
    showNode(node, nodes_transfer_num);
    memcpy(&nodes_transfer_buffer[nodes_transfer_num++], node, sizeof(BPTreeNode));
    if (!node->isLeaf) {
        for (int i = 0; i <= node->numKeys; i++) {
            traverse_and_copy_nodes(node->ptrs.inl.children[i]);
        }
    }
    return;
}

void serialize(MBPTptr root)
{
    nodes_transfer_num = 0;
    traverse_and_copy_nodes(root);
    freeBPTreeNode_recursive(root);
    return;
}

MBPTptr deserialize_node(int tree_index)
{
    MBPTptr node;
    if (nodes_transfer_num != 0) {
        node = newBPTreeNode();
    } else {
        node = root[tree_index];
    }
    memcpy(node, &nodes_transfer_buffer[nodes_transfer_num++], sizeof(BPTreeNode));
    if (!node->isLeaf) {
        for (int i = 0; i <= node->numKeys; i++) {
            node->ptrs.inl.children[i] = deserialize_node(tree_index);
            node->ptrs.inl.children[i]->parent = node;
        }
    }
    return node;
}

int deserialize()
{
    nodes_transfer_num = 0;
    int tree_index = new_BPTree();
    deserialize_node(tree_index);
    return tree_index;
}

int main()
{
    int tid = me();
    switch (task_no) {
    case INIT_TASK: {
        init_BPTree();
    }
    case SEARCH_TASK: {
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
    }
    case INSERT_TASK: {
        /* insertion */
        if (tid == 0) {
            for (int index = 0; index < end_idx[NUM_BPTREE_IN_DPU - 1]; index++) {
                BPTreeInsert(request_buffer[index].key,
                    request_buffer[index].write_val_ptr);
            }
        }
    }
    case REMOVE_TASK: {
    }
    case RANGE_SEARCH_TASK: {
    }
    case SERIALIZE_TASK: {
        if (tid == 0)
            serialize(root[transfer_tree_index]);
        //printf("num_elems = %lu\n", nodes_transfer_num);
    }
    case DESERIALIZE_TASK: {
        if (tid == 0)
            transfer_tree_index = deserialize();
        // traverse_and_print_nodes(root[transfer_tree_index]);
    }
    default: {
        if (tid == 0)
            fprintf(stderr, "unspecified task %d\n", task_no);
        return -1;
    }
    }
    // for(int i = 0; i < request_buffer[me()].num_req; i++){
    //     printf("[tasklet %d] key:%ld\n",tid, request_buffer[tid].key[i]);
    // }
    if (tid == 0) {
        batch_num++;
    }
    barrier_wait(&my_barrier);
    if (batch_num == 1) {
        // printf("[tasklet %d] initializing BPTree\n", tid);
    }
    // printf("[tasklet %d] %d times insertion\n",tid,
    // request_buffer[tid].num_req);
    int index = 0;
    value_ptr_t volatile res[NR_TASKLETS];
    barrier_wait(&my_barrier);


    barrier_wait(&my_barrier);

/* write intensive */
#if WORKLOAD == W50R50
    for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
        res[tid] = BPTreeGet(request_buffer[index].key);
    }
#endif
/* read intensive */
#if WORKLOAD == W05R95
    for (int i = 0; i < 19; i++) {
        for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
            res[tid] = BPTreeGet(request_buffer[index].key);
        }
    }
#endif
    barrier_wait(&my_barrier);
    return 0;
}