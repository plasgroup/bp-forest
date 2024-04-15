#pragma once

#include "common.h"
#include "workload_types.h"

#include <mram.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>


union Node;
typedef __mram_ptr union Node* MBPTptr;
extern MBPTptr root;
extern uint32_t num_kvpairs;

typedef struct {
    key_int64_t keys[MAX_NR_CHILDREN - 1];
    MBPTptr children[MAX_NR_CHILDREN];
} InternalNodeBody;
typedef struct {
    key_int64_t keys[MAX_NR_PAIRS];
    value_ptr_t values[MAX_NR_PAIRS];
    MBPTptr right;
    MBPTptr left;
} LeafNodeBody;

typedef union Node {
    struct {
        int isRoot : 8;
        int isLeaf : 8;
        unsigned numKeys : 16;
        MBPTptr parent;
        union {
            InternalNodeBody inl;  // also update MAX_NR_CHILDREN in common/inc/common.h
            LeafNodeBody lf;       // also update MAX_NR_PAIRS in common/inc/common.h
        } body;
    };
    char size_adjuster[SIZEOF_NODE];
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");

extern void init_BPTree(void);

/**
 *    @param key key to insert
 *    @param pos pos
 *    @param value value to insert
 **/
extern void BPTreeInsert(key_int64_t key, value_ptr_t value);

/**
 *    @param key key to search
 **/
extern value_ptr_t BPTreeGet(key_int64_t key);
extern KVPair BPTreeSucc(key_int64_t key);
extern void BPTreeGetRange(key_int64_t, int);
extern void BPTreeDelete(key_int64_t);
extern void BPTreePrintLeaves(void);
extern void BPTreePrintKeys(void);
extern bool BPTreeCheckStructure(void);
extern void BPTreePrintRoot(void);
extern void BPTreePrintAll(void);

extern void BPTreeSerialize(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest);
/**
 *    @return number of copied key-value pairs
 **/
extern uint32_t BPTreeExtractFirstPairs(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest, key_int64_t delimiter);
/**
 *    @pre nth < (Num of key-value pairs in the tree)
 */
extern key_int64_t BPTreeNthKeyFromLeft(uint32_t nth);
/**
 *    @pre nth < (Num of key-value pairs in the tree)
 */
extern key_int64_t BPTreeNthKeyFromRight(uint32_t nth);

extern void BPTreeInsertSortedPairsToLeft(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs);
extern void BPTreeInsertSortedPairsToRight(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs);
