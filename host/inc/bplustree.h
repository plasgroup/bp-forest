#pragma once

#ifndef MAX_CHILD
#define MAX_CHILD (126)  // split occurs if numKeys >= MAX_CHILD
#endif
#define READ (1)
#define WRITE (0)
#define MAX_NUM_SPLIT (10)
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include "common.h"
typedef uint64_t key_t_;
typedef uint64_t value_ptr_t_;
typedef struct BPTreeNode* BPTptr;

typedef struct InternalNodePtrs {
    BPTptr children[MAX_CHILD + 1];
} InternalNodePtrs;
typedef struct LeafNodePtrs {
    value_ptr_t_ value[MAX_CHILD];
    BPTptr right;
    BPTptr left;
} LeafNodePtrs;

typedef struct BPTreeNode {
    int isRoot : 8;
    int isLeaf : 8;
    int numKeys : 16;
    key_t_ key[MAX_CHILD];
    BPTptr parent;
    union {
        InternalNodePtrs inl;
        LeafNodePtrs lf;
    } ptrs;
} BPTreeNode;

typedef struct BPlusTree {
    BPTptr root;
    int height;
    int NumOfNodes;
    int num_elems;
    int tree_index;
} BPlusTree;

/**
 *    @param key key to insert
 *    @param pos pos
 *    @param value pointer to value to insert
 **/
extern int BPTreeInsert(BPlusTree*, key_t_, value_ptr_t_);
/**
 *    @param key key to search
 **/
extern value_ptr_t_ BPTreeGet(BPlusTree*, key_t_);
extern int BPTree_GetNumOfNodes(BPlusTree*);
extern void BPTreePrintLeaves(BPlusTree*);
extern void BPTreePrintRoot(BPlusTree*);
extern void BPTreePrintAll(BPlusTree*);
extern int BPTree_GetHeight(BPlusTree*);
extern BPlusTree* new_BPTree();
/* for load balancing */
extern int traverse_and_count_elems(BPTptr);
extern bool do_split_phase(BPlusTree*);
extern void split_tree(BPlusTree*);
extern void serialize(BPlusTree*);
extern BPlusTree* deserialize();
extern BPTreeNode nodes_buffer[MAX_NODE_NUM_TRANSFER];
extern uint64_t nodes_num;

typedef struct request_t {  // number of requests
    key_t_ key;             // key of each request
    uint8_t read_or_write;  // 1→read or 0→write
    value_ptr_t_ val;       // pointer to value if request is write
} request_t;
