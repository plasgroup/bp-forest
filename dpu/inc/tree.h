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
extern void TreeInsert(key_uint64_t key, value_uint64_t value);

/**
 *    @param key key to search
 **/
extern value_uint64_t TreeGet(key_uint64_t key);
extern KVPair TreeSucc(key_uint64_t key);
extern void TreeGetRange(key_uint64_t, int);
extern void TreeDelete(key_uint64_t);

// extern void TreePrintLeaves(void);
extern void TreePrintKeys(void);
extern bool TreeCheckStructure(void);
extern void TreePrintRoot(void);
extern void TreePrintAll(void);


#ifndef DISABLE_MIGRATION

extern void TreeSerialize(key_uint64_t __mram_ptr* keys_dest, value_uint64_t __mram_ptr* values_dest);
/**
 *    @return number of copied key-value pairs
 **/
extern uint32_t TreeExtractFirstPairs(key_uint64_t __mram_ptr* keys_dest, value_uint64_t __mram_ptr* values_dest, key_uint64_t delimiter);
/**
 *    @pre nth < (Num of key-value pairs in the tree)
 */
extern key_uint64_t TreeNthKeyFromLeft(uint32_t nth);
/**
 *    @pre nth < (Num of key-value pairs in the tree)
 */
extern key_uint64_t TreeNthKeyFromRight(uint32_t nth);

extern void TreeInsertSortedPairsToLeft(const key_uint64_t __mram_ptr* keys_src, const value_uint64_t __mram_ptr* values_src, uint32_t nr_pairs);
extern void TreeInsertSortedPairsToRight(const key_uint64_t __mram_ptr* keys_src, const value_uint64_t __mram_ptr* values_src, uint32_t nr_pairs);

#endif /* !DISABLE_MIGRATION */
