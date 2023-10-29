#ifndef __bplustree_H__
#define __bplustree_H__

#define MAX_CHILD (127)  // split occurs if numKeys >= MAX_CHILD

#define NODE_DATA_SIZE (30)  // maximum node data size, MB
#ifndef MAX_NODE_NUM_PER_TREE
#define MAX_NODE_NUM_PER_TREE (NODE_DATA_SIZE / MAX_NUM_BPTREE_IN_DPU / sizeof(BPTreeNode))
#endif
#define MAX_NODE_NUM \
    ((NODE_DATA_SIZE << 20) / sizeof(BPTreeNode) / MAX_NUM_BPTREE_IN_DPU)  // NODE_DATA_SIZE MB for Node data
#include "common.h"
#include <mram.h>
#include <string.h>

typedef __mram_ptr union BPTreeNode* MBPTptr;
extern MBPTptr root[NR_TASKLETS];

typedef struct InternalNodePtrs {
    MBPTptr children[MAX_CHILD + 1];

} InternalNodePtrs;
typedef struct LeafNodePtrs {
    value_ptr_t value[MAX_CHILD];
    MBPTptr right;
    MBPTptr left;
} LeafNodePtrs;

typedef union BPTreeNode {
    struct Node {
        int isRoot : 8;
        int isLeaf : 8;
        int numKeys : 16;
        key_int64_t key[MAX_CHILD];
        MBPTptr parent;
        union {
            InternalNodePtrs inl;
            LeafNodePtrs lf;
        } ptrs;
    } node;
    int offset_next;
} BPTreeNode;

extern void
init_BPTree();

/**
 *    @param key key to insert
 *    @param pos pos
 *    @param value value to insert
 **/
extern int BPTreeInsert(key_int64_t, value_ptr_t, uint32_t);
/**
 *    @param key key to search
 **/
extern value_ptr_t BPTreeGet(key_int64_t, uint32_t);
extern void BPTreeGetRange(key_int64_t, int);
extern void BPTreeDelete(key_int64_t);
extern int BPTree_GetNumOfNodes();
extern void BPTreePrintLeaves();
extern void BPTreePrintRoot();
extern void BPTreePrintAll();
extern int BPTree_GetHeight();
extern MBPTptr newBPTreeNode(uint32_t);
extern void freeBPTree(MBPTptr, int);
extern MBPTptr malloc_tree();
#ifdef ALLOC_WITH_FREE_LIST
extern void init_free_list(int);
#endif
#ifdef ALLOC_WITH_BITMAP
extern void init_node_bitmap(uint32_t);
#endif
#endif