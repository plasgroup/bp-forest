#pragma once

#include "common.h"
#include "workload_types.h"

#include <attributes.h>

#include <stdbool.h>
#include <stdint.h>


extern uint32_t num_kvpairs;


extern void init_Tree(void);

/**
 *    @param key key to insert
 *    @param value value to insert
 **/
extern void TreeInsert(key_int64_t key, value_ptr_t value);

/**
 *    @param key key to search
 **/
extern value_ptr_t TreeGet(key_int64_t key);
extern KVPair TreeSucc(key_int64_t key);
extern void TreeGetRange(key_int64_t, int);
extern void TreeDelete(key_int64_t);

// extern void TreePrintLeaves(void);
extern void TreePrintKeys(void);
extern bool TreeCheckStructure(void);
extern void TreePrintRoot(void);
extern void TreePrintAll(void);


#ifndef DISABLE_MIGRATION

extern void TreeSerialize(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest);
/**
 *    @return number of copied key-value pairs
 **/
extern uint32_t TreeExtractFirstPairs(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest, key_int64_t delimiter);
/**
 *    @pre nth < (Num of key-value pairs in the tree)
 */
extern key_int64_t TreeNthKeyFromLeft(uint32_t nth);
/**
 *    @pre nth < (Num of key-value pairs in the tree)
 */
extern key_int64_t TreeNthKeyFromRight(uint32_t nth);

extern void TreeInsertSortedPairsToLeft(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs);
extern void TreeInsertSortedPairsToRight(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs);

#endif /* !DISABLE_MIGRATION */
