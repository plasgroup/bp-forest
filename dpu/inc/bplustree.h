#pragma once

#include "bit_ops_macro.h"
#include "common.h"
#include "dpu_params.h"
#include "node_ptr.h"
#include "workload_types.h"

#include <attributes.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>


typedef struct {
    NodePtr ptr : 32 - CEIL_LOG2_UINT32(SIZEOF_NODE);
    unsigned numKeys : CEIL_LOG2_UINT32(SIZEOF_NODE);
} NodeLink;
_Static_assert(sizeof(NodeLink) == 4, "sizeof(NodeLink) == 4");
_Static_assert(_Alignof(NodeLink) == 4, "_Alignof(NodeLink) == 4");
static const NodeLink NODELINK_NULLPTR = {NODE_NULLPTR, UINT_MAX&((1u << CEIL_LOG2_UINT32(SIZEOF_NODE)) - 1u)};


#define MAX_NR_CHILDREN ((SIZEOF_NODE + 8) / 12)  // ((SIZEOF_NODE + sizeof(key_uint64_t)) / (sizeof(key_uint64_t) + sizeof(NodeLink)))
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


// HEIGHT <= log_{MIN_NR_CHILDREN} [ (MAX_NR_NODES - 1) * (MIN_NR_CHILDREN - 1) / 2.0 + 1 ]
#define MAX_HEIGHT ((CEIL_LOG2_UINT32((MAX_NR_NODES - 1) * (MIN_NR_CHILDREN - 1) + 2) - 1) / FLOOR_LOG2_UINT32(MIN_NR_CHILDREN))


typedef struct {
    uint32_t key_parts[2];
    NodeLink child;
} LinkLift;

typedef struct {
    union {
        __dma_aligned KVPair pairs[TREE_CONSTRUCT_NR_CACHED_KVPAIRS];
        __dma_aligned LinkLift lifted[TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT];
    } in;
    struct {
        __dma_aligned LinkLift lifted[TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT];
        unsigned nr_cached_lift;
    } out;
    __dma_aligned Node node;
    __dma_aligned NodeLink first_leaf;
    __dma_aligned NodePtr last_leaf;
} InitWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned key_uint64_t qrys[TASK_GET_NR_CACHED_QRYS];
} GetWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned uint16_t lump_end_indices[TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES];
    __dma_aligned key_uint64_t delim_keys[TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS];
    __dma_aligned value_uint64_t results[TASK_RANGE_MIN_NR_CACHED_RESULTS];
} TaskletLocalRMQWorkspace;
_Static_assert((TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES * sizeof(uint16_t)) % 8 == 0, "(TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES * sizeof(uint16_t)) % 8 == 0");

typedef union {
    __dma_aligned uint16_t lump_end_indices[MAX_NR_RMQ_LUMPS + 2];
    TaskletLocalRMQWorkspace th[TASK_RANGE_MIN_NR_TASKLETS];
} RMQWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned RangeCountQuery qrys[TASK_RANGE_COUNT_NR_CACHED_QRYS];
    __dma_aligned uint64_t results[TASK_RANGE_COUNT_NR_CACHED_RESULTS];
} RCQWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned RangeCountQuery qrys[TASK_RANGE_COUNT_PREFIX_NR_CACHED_QRYS];
    __dma_aligned uint64_t results[TASK_RANGE_COUNT_PREFIX_NR_CACHED_RESULTS];
} RCPQWorkspace;


#define MAX_NR_SUMMARY_DATA (MAX_NR_NODES * (MAX_NR_CHILDREN - 1) / ((MAX_NR_CHILDREN - 1) * MIN_NR_CHILDREN + MAX_NR_CHILDREN))
_Static_assert(MAX_NR_SUMMARY_DATA <= UINT16_MAX * 4, "MAX_NR_SUMMARY_DATA <= UINT16_MAX * 4");
#define NR_SUMMARY_BLOCKS_PER_CHUNK                                       \
    (((MAX_NR_SUMMARY_DATA - (TASK_SUMMARIZE_NR_TASKLETS - 1))            \
         + 4 * (MAX_NR_SUMMARY_CHUNKS - (TASK_SUMMARIZE_NR_TASKLETS - 1)) \
         - 1)                                                             \
        / (4 * (MAX_NR_SUMMARY_CHUNKS - (TASK_SUMMARIZE_NR_TASKLETS - 1))))

typedef struct {
    __dma_aligned NodeLink children_cache[2];
    NodeLink node;
    uint8_t nr_visited_children;
} SummarizeStackElem;
typedef struct {
    __dma_aligned SummaryBlock summary[NR_SUMMARY_BLOCKS_PER_CHUNK];
    __dma_aligned NodeLink children_cache[MAX_NR_CHILDREN];
#define MAX_NR_TRAVERSAL_ROOTS ((MAX_NR_CHILDREN + 1) / 2 * 2 - MAX_NR_CHILDREN / 4 * 2)
    __dma_aligned NodeLink traversal_roots[MAX_NR_TRAVERSAL_ROOTS];
    __dma_aligned key_uint64_t delims_of_traversal_roots[MAX_NR_TRAVERSAL_ROOTS];
#undef MAX_NR_TRAVERSAL_ROOTS
    SummarizeStackElem stack[MAX_HEIGHT - 2];
} TaskletLocalSummarizeWorkspace;

typedef struct {
    TaskletLocalSummarizeWorkspace th[TASK_SUMMARIZE_NR_TASKLETS];
    uint32_t nr_allocated_bytes;
    struct {
        uint32_t nr_pairs;
        uint16_t nr_chunks;
        uint16_t chunk_end_indices[MAX_NR_SUMMARY_CHUNKS];
    } result_header __dma_aligned;
    // chunk_linked_list[me()] is head for the me()-th tasklet
    uint16_t chunk_linked_list[MAX_NR_SUMMARY_CHUNKS + TASK_SUMMARIZE_NR_TASKLETS];
    uint16_t nr_committed_blocks;
} SummarizeWorkspace;
_Static_assert(sizeof(SummaryBlock[NR_SUMMARY_BLOCKS_PER_CHUNK]) <= 2048, "sizeof(SummaryBlock[NR_SUMMARY_BLOCKS_PER_CHUNK]) <= 2048");
_Static_assert(NR_SUMMARY_BLOCKS_PER_CHUNK >= 1, "NR_SUMMARY_BLOCKS_PER_CHUNK >= 1");

typedef struct {
    __dma_aligned NodeLink children_cache[2];
    NodeLink node;
    uint16_t nr_passed_children;
} ExtractStackElem;
typedef struct {
    __dma_aligned KeyRange hot_ranges[NR_RANKS * MAX_NR_DPUS_IN_RANK];
    __dma_aligned uint32_t nr_pairs_cache[2];
    __dma_aligned Node node_cache;
    __dma_aligned KVPair kvpair;
    __aligned(8) ExtractStackElem stack[MAX_HEIGHT];
    __aligned(8) ExtractStackElem initial_stack[MAX_HEIGHT];
} ExtractWorkspace;

typedef union {
    InitWorkspace init[TREE_CONSTRUCT_NR_TASKLETS];
    SummarizeWorkspace summarize;
    ExtractWorkspace extract;
    GetWorkspace get[TASK_GET_NR_TASKLETS];
    RCQWorkspace rcq[TASK_RANGE_COUNT_NR_TASKLETS];
    RCPQWorkspace rcpq[TASK_RANGE_COUNT_PREFIX_NR_TASKLETS];
    RMQWorkspace rmq;
} TreeWorkspace;
