#ifndef __bplustree_H__
#define __bplustree_H__

#define MAX_CHILD (126)  // split occurs if numKeys >= MAX_CHILD

#define NODE_DATA_SIZE (40)                                         // maximum node data size, MB
#define MAX_NODE_NUM ((NODE_DATA_SIZE << 20) / sizeof(BPTreeNode))  // NODE_DATA_SIZE MB for Node data
#define MAX_NUM_BPTREE_IN_DPU (100)
#include "../../../common/inc/common.h"
#include <mram.h>
#include <stdint.h>
#include <string.h>

typedef __mram_ptr struct BPTreeNode* MBPTptr;
extern MBPTptr root[MAX_NUM_BPTREE_IN_DPU];

typedef struct InternalNodePtrs {
    MBPTptr children[MAX_CHILD + 1];

} InternalNodePtrs;
typedef struct LeafNodePtrs {
    value_ptr_t value[MAX_CHILD];
    MBPTptr right;
    MBPTptr left;
    uint64_t dummy_pad;
} LeafNodePtrs;

typedef struct BPTreeNode {
    int isRoot : 8;
    int isLeaf : 8;
    int numKeys : 16;
    key_int64_t key[MAX_CHILD];
    MBPTptr parent;
    union {
        InternalNodePtrs inl;
        LeafNodePtrs lf;
    } ptrs;
} BPTreeNode;

extern void init_BPTree();
extern int new_BPTree();

/**
 *    @param key key to insert
 *    @param pos pos
 *    @param value value to insert
 **/
extern int BPTreeInsert(key_int64_t, value_ptr_t, int);
/**
 *    @param key key to search
 **/
extern value_ptr_t BPTreeGet(key_int64_t);
extern void BPTreeGetRange(key_int64_t, int);
extern void BPTreeDelete(key_int64_t);
extern int BPTree_GetNumOfNodes();
extern void BPTreePrintLeaves();
extern void BPTreePrintRoot();
extern void BPTreePrintAll();
extern int BPTree_GetHeight();

#endif