#include "bplustree.h"
#include "common.h"
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
__mram int end_idx[MAX_NUM_BPTREE_IN_DPU];
__mram dpu_result_t result;
__mram_ptr void* ptr;
__mram BPTreeNode nodes_transfer_buffer[MAX_NODE_NUM];
__mram uint64_t nodes_transfer_num;
__host uint64_t task_no;
uint32_t task;

#ifdef DEBUG_ON
__mram_ptr void* getval;
#endif


void traverse_and_copy_nodes(MBPTptr node)
{
    // showNode(node, nodes_transfer_num);
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
    return;
}

MBPTptr deserialize_node(uint32_t tid)
{
    MBPTptr node = newBPTreeNode(tid);
    memcpy(node, &nodes_transfer_buffer[nodes_transfer_num++], sizeof(BPTreeNode));
    if (!node->isLeaf) {
        for (int i = 0; i <= node->numKeys; i++) {
            node->ptrs.inl.children[i] = deserialize_node(tid);
            node->ptrs.inl.children[i]->parent = node;
        }
    }
    return node;
}

MBPTptr deserialize(uint32_t tid)
{
    nodes_transfer_num = 0;
    return deserialize_node(tid);
}

int main()
{
    int tid = me();
    if (tid == 0) {
        // printf("size of request_buffer:%u\n", sizeof(request_buffer));
        // printf("using up to %d MB for value data, can store up to %d values per
        // tasklet\n", VALUE_DATA_SIZE, MAX_VALUE_NUM); printf("using up to %d MB for
        // node data, node size = %u, can store up to %d nodes per tasklet\n",
        // NODE_DATA_SIZE,sizeof(BPTreeNode), MAX_NODE_NUM);
        // printf("batch_num:%d\n", batch_num);
        task = (uint32_t)task_no;
        printf("task = %d\n", task);

        for (int t = 0; t < 10; t++) {
            printf("end_idx[%d] = %d\n", t, end_idx[t]);
        }
    }
    barrier_wait(&my_barrier);

    switch (task) {
    case TASK_INIT: {
        init_BPTree(tid);
        break;
    }
    case TASK_INSERT: {
        /* insertion */
        printf("insert task\n");
        for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
            if (tid == 0)
                printf("[tasklet %d] insert (%ld, %ld)\n", tid, request_buffer[index].key, request_buffer[index].write_val_ptr);
            BPTreeInsert(request_buffer[index].key,
                request_buffer[index].write_val_ptr, tid);
        }
        // if(BPTree_GetNumOfNodes(tid) > MAX_NODE_NUM-100){
        //     assert(0);
        // }
        break;
    }
    case TASK_GET: {
        value_ptr_t volatile res;
        for (int index = tid == 0 ? 0 : end_idx[tid - 1]; index < end_idx[tid]; index++) {
            res = BPTreeGet(request_buffer[index].key, tid);
        }
        break;
    }
    case TASK_FROM: {
        if (tid == (uint32_t)(task_no >> 32)) {
            serialize(root[tid]);
            freeBPTree(root[tid], tid);
        }
        break;
    }
    case TASK_TO: {
        if (tid == (uint32_t)(task_no >> 32)) {
            MBPTptr new_root;
            malloc_tree(new_root);
            new_root = deserialize(tid);
        }
        break;
    }
    default: {
        printf("no such a task: task %d\n", task);
        return -1;
    }
    }

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