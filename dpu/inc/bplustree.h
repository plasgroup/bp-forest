#ifndef __bplustree_H__
#define __bplustree_H__

#define MAX_CHILD (18)  // split occurs if numKeys >= MAX_CHILD

#define NODE_DATA_SIZE (40)  // maximum node data size, MB
#define MAX_NODE_NUM \
    ((NODE_DATA_SIZE << 20) / sizeof(BPTreeNode) / NR_TASKLETS)  // NODE_DATA_SIZE MB for Node data
#include "common.h"
#include <mram.h>
#include <string.h>

typedef __mram_ptr struct BPTreeNode* MBPTptr;
extern MBPTptr root[NR_TASKLETS];

typedef struct InternalNodePtrs {
    MBPTptr children[MAX_CHILD + 1];

} InternalNodePtrs;
typedef struct LeafNodePtrs {
    value_ptr_t value[MAX_CHILD];
    MBPTptr right;
    MBPTptr left;
} LeafNodePtrs;

typedef struct BPTreeNode {
    int isRoot : 8;
    int isLeaf : 8;
    int numKeys : 16;
    intkey_t key[MAX_CHILD];
    MBPTptr parent;
    union {
        InternalNodePtrs inl;
        LeafNodePtrs lf;
    } ptrs;
} BPTreeNode;

extern void init_BPTree();

/**
 *    @param key key to insert
 *    @param pos pos
 *    @param value value to insert
 **/
extern int BPTreeInsert(intkey_t, value_ptr_t, uint32_t);
/**
 *    @param key key to search
 **/
extern value_ptr_t BPTreeGet(intkey_t, uint32_t);
extern void BPTreeGetRange(intkey_t, int);
extern void BPTreeDelete(intkey_t);
extern int BPTree_GetNumOfNodes();
extern void BPTreePrintLeaves();
extern void BPTreePrintRoot();
extern void BPTreePrintAll();
extern int BPTree_GetHeight();

#endif