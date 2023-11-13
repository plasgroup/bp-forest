#pragma once

#define MAX_CHILD (126)  // split occurs if numKeys >= MAX_CHILD

#include "cabin.h"
#include "common.h"
#include <mram.h>
#include <string.h>

typedef __mram_ptr struct BPTreeNode* MBPTptr;
typedef __mram_ptr KVPair* KVPairPtr;
extern MBPTptr root[NR_SEATS_IN_DPU];

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
    key_int64_t key[MAX_CHILD];
    MBPTptr parent;
    union {
        InternalNodePtrs inl;
        LeafNodePtrs lf;
    } ptrs;
} BPTreeNode;

typedef union NodeOrOffset {
    BPTreeNode node;
    int offset;
} NodeOrOffset;

extern void init_BPTree(seat_id_t seat_id);

/**
 *    @param key key to insert
 *    @param pos pos
 *    @param value value to insert
 **/
extern int BPTreeInsert(key_int64_t key, value_ptr_t value , seat_id_t seat_id);

/**
 *    @param key key to search
 **/
extern value_ptr_t BPTreeGet(key_int64_t key, seat_id_t seat_id);
extern void BPTreeGetRange(key_int64_t, int);
extern void BPTreeDelete(key_int64_t);
extern int BPTree_GetNumOfNodes();
extern void BPTreePrintLeaves();
extern void BPTreePrintRoot();
extern void BPTreePrintAll();
extern int BPTree_GetHeight();
extern int BPTree_Serialize(seat_id_t seat_id, KVPairPtr dest);
extern int BPTree_Serialize_j_Last_Subtrees(MBPTptr tree, KVPairPtr dest, int j);
extern void BPTree_Deserialize(seat_id_t seat_id, KVPairPtr src, int start_index, int n);
