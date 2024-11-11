#pragma once

#include "bit_ops_macro.h"
#include "common.h"
#include "dpu_params.h"
#include "node_ptr.h"
#include "workload_types.h"

#include <attributes.h>
#include <limits.h>


typedef struct {
    NodePtr ptr : 32 - CEIL_LOG2_UINT32(SIZEOF_NODE);
    unsigned numKeys : CEIL_LOG2_UINT32(SIZEOF_NODE);
} NodeLink;
_Static_assert(sizeof(NodeLink) == 4, "sizeof(NodeLink) == 4");
_Static_assert(_Alignof(NodeLink) == 4, "_Alignof(NodeLink) == 4");
static const NodeLink NODELINK_NULLPTR = {NODE_NULLPTR, UINT_MAX&((1u << CEIL_LOG2_UINT32(SIZEOF_NODE)) - 1u)};


#define MAX_NR_CHILDREN ((SIZEOF_NODE + sizeof(key_uint64_t)) / (sizeof(key_uint64_t) + sizeof(NodeLink)))
#define MIN_NR_CHILDREN ((MAX_NR_CHILDREN + 1) / 2)

#define MAX_NR_PAIRS ((SIZEOF_NODE - 16) / (sizeof(key_uint64_t) + sizeof(value_uint64_t)))
#define MIN_NR_PAIRS ((MAX_NR_PAIRS + 1) / 2)

typedef struct {
    __dma_aligned key_uint64_t keys[MAX_NR_CHILDREN - 1];
    __dma_aligned NodeLink children[MAX_NR_CHILDREN];
} InternalNode;
typedef struct {
    __dma_aligned key_uint64_t keys[MAX_NR_PAIRS];
    __dma_aligned value_uint64_t values[MAX_NR_PAIRS];
    __dma_aligned NodeLink right;
    __dma_aligned NodePtr left;
} LeafNode;

typedef union {
    InternalNode inl;
    LeafNode lf;  // also update MAX_NR_PAIRS in common/inc/common.h
    char size_adjuster[SIZEOF_NODE];
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");


typedef struct {
    uint32_t key_parts[2];
    NodeLink child;
} LinkLift;

typedef struct {
    union {
        __dma_aligned KVPair pairs[TASK_INIT_NR_CACHED_KVPAIRS];
        __dma_aligned LinkLift lifted[TASK_INIT_NR_CACHED_INPUT_LIFT];
    } in;
    struct {
        __dma_aligned LinkLift lifted[TASK_INIT_NR_CACHED_OUTPUT_LIFT];
        unsigned nr_cached_lift;
    } out;
    __dma_aligned Node nodes[TASK_INIT_NR_CACHED_NODES];
} InitWorkspace;

typedef struct {
    InitWorkspace init[TASK_INIT_NR_TASKLETS];
} TreeWorkspace;
