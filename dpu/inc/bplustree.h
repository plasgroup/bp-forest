#pragma once

#include "bit_ops_macro.h"
#include "common.h"
#include "node_ptr.h"
#include "workload_types.h"

#include <attributes.h>

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>


typedef struct __attribute__((packed, aligned(4))) {
    uint8_t numKeys;
#if NODE_PTR_WIDTH < CHAR_BIT * 2
    bool isLeaf;
    NodePtr parent;
#else
    bool isLeaf : 1;
    NodePtr parent : NODE_PTR_WIDTH;
#endif
} NodeHeader;
_Static_assert(sizeof(NodeHeader) == 4, "sizeof(NodeHeader) == 4");


#ifdef CACHE_CHILD_HEADER_IN_LINK
typedef struct __attribute__((packed, aligned(4))) {
    uint8_t numKeys;
#if NODE_PTR_WIDTH < CHAR_BIT * 2
    bool isLeaf;
    NodePtr ptr;
#else
    bool isLeaf : 1;
    NodePtr ptr : NODE_PTR_WIDTH;
#endif
} ChildInfo;
_Static_assert(sizeof(NodeHeader) == sizeof(ChildInfo), "sizeof(NodeHeader) == sizeof(ChildInfo)");

#else  /* CACHE_CHILD_HEADER_IN_LINK */
typedef struct {
    NodePtr ptr;
} ChildInfo;
#endif /* CACHE_CHILD_HEADER_IN_LINK */

_Static_assert(HAS_SINGLE_BIT_UINT(sizeof(ChildInfo)), "HAS_SINGLE_BIT_UINT(sizeof(ChildInfo))");
#define ALIGNOF_CHILDINFO_DMA (8 / sizeof(ChildInfo))


#define MAX_NR_CHILDREN ((SIZEOF_NODE / (sizeof(key_int64_t) + sizeof(ChildInfo))) / 2 * 2)

#define MIN_NR_KEYS ((MAX_NR_CHILDREN - 1) / 2)
#define MIN_NR_PAIRS ((MAX_NR_PAIRS + 1) / 2)
#define GUARANTEED_CAPACITY_OF_PAIRS ((MIN_NR_KEYS * MIN_NR_PAIRS * (MAX_NUM_NODES_IN_DPU - 1) + 2 * MIN_NR_PAIRS) \
                                      / (MIN_NR_KEYS + 1))

typedef struct {
    __dma_aligned key_int64_t keys[MAX_NR_CHILDREN - 1];
    __dma_aligned ChildInfo children[MAX_NR_CHILDREN];
} InternalNodeBody;
typedef struct {
    __dma_aligned key_int64_t keys[MAX_NR_PAIRS];
    __dma_aligned value_ptr_t values[MAX_NR_PAIRS];
    NodePtr right;
    NodePtr left;
} LeafNodeBody;

typedef union {
    struct {
        __dma_aligned NodeHeader header;
        union {
            InternalNodeBody inl;  // also update MAX_NR_CHILDREN in common/inc/common.h
            LeafNodeBody lf;       // also update MAX_NR_PAIRS in common/inc/common.h
        } body;
    };
    char size_adjuster[SIZEOF_NODE];
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");

typedef struct {
    __dma_aligned NodeHeader header;
    union {
        struct {
            __dma_aligned key_int64_t keys[MAX_NR_CHILDREN - 1];
        } inl;
        struct {
            __dma_aligned key_int64_t keys[MAX_NR_PAIRS];
        } lf;
    } body;
} NodeHeaderAndKeys;
