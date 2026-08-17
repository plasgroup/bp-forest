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


#ifdef DEBUG_OCCUPANCY
#define MAX_NR_CHILDREN ((SIZEOF_NODE + 8 - 4) / 12 / 2 * 2)  // maximum even number <= ((SIZEOF_NODE + sizeof(key_uint64_t) - sizeof(unsigned)) / (sizeof(key_uint64_t) + sizeof(NodeLink)))
#define MIN_NR_CHILDREN ((MAX_NR_CHILDREN + 1) / 2)
#else
#define MAX_NR_CHILDREN ((SIZEOF_NODE + 8) / 12 / 2 * 2)  // maximum even number <= ((SIZEOF_NODE + sizeof(key_uint64_t)) / (sizeof(key_uint64_t) + sizeof(NodeLink)))
#define MIN_NR_CHILDREN ((MAX_NR_CHILDREN + 1) / 2)
#endif

#define MAX_NR_PAIRS ((SIZEOF_NODE - 16) / (sizeof(key_uint64_t) + sizeof(value_uint64_t)))
#define MIN_NR_PAIRS ((MAX_NR_PAIRS + 1) / 2)

// Bidirectional layout: all the occupied elements of a node form one
// contiguous region in the middle, so a single DMA, sized from the occupancy
// embedded in the NodeLink, can fetch exactly the occupied part.  Every node
// fetch on the query paths does so (fetch_internal_filled/fetch_leaf_filled).
//  * The children/values array occupies the front of the node and is filled
//    backward from its end.
//  * The keys array occupies the back and is filled forward.
//  * Fixed-size fields that the same DMA should cover (the leaf's `right`
//    link) sit between the two arrays.
//  * The leaf's `left` is used only by writers that transfer the whole node
//    anyway (insertion) or its own 8-byte slot, so it sits at the tail,
//    outside the occupancy-sized fetch; `values` then starts at offset 0,
//    which keeps the fetch's address arithmetic minimal.
typedef struct {
    __dma_aligned NodeLink children[MAX_NR_CHILDREN];
    __dma_aligned key_uint64_t keys[MAX_NR_CHILDREN - 1];
#ifdef DEBUG_OCCUPANCY
    unsigned numKeys;
#endif
} InternalNode;
typedef struct {
    __dma_aligned value_uint64_t values[MAX_NR_PAIRS];
    __dma_aligned NodeLink right;
    __dma_aligned key_uint64_t keys[MAX_NR_PAIRS];
    __dma_aligned NodePtr left;
#ifdef DEBUG_OCCUPANCY
    unsigned numKeys;
#endif
} LeafNode;

typedef union {
    InternalNode inl;
    LeafNode lf;  // also update MAX_NR_PAIRS in common/inc/common.h
    char size_adjuster[SIZEOF_NODE];
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");

// The n-th child/value in logical order.
#define NthChild(inl, n) ((inl).children[MAX_NR_CHILDREN - 1 - (unsigned)(n)])
#define NthValue(lf, n) ((lf).values[MAX_NR_PAIRS - 1 - (unsigned)(n)])
// The values in logical order by negative indexing:
// RevValues(lf)[-(int32_t)n] == NthValue(lf, n).  Scan loops hoist this base
// pointer so that the per-element addressing stays one instruction (lsl_sub).
#define RevValues(lf) (&NthValue(lf, 0))


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
#ifdef DEBUG_OCCUPANCY
    unsigned numKeys_in_next_of_last_leaf;
#endif
} InitWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned key_uint64_t qrys[TASK_GET_NR_CACHED_QRYS];
} GetWorkspace;


typedef struct {
    __dma_aligned Node node_cache[2];
    __dma_aligned KVPair qrys[TASK_INSERT_NR_CACHED_QRYS];
    uint32_t idx_qry_in_cache;
    uintptr_t cursor_on_qrys;
} InsertWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned key_uint64_t qrys[TASK_DELETE_NR_CACHED_QRYS];
    __dma_aligned uint8_t results[TASK_DELETE_NR_CACHED_RESULTS];
    __dma_aligned value_uint64_t old_value;
    // DMA buffers of the min-refresh phase.  Static so that the 8-byte
    // alignment is guaranteed: the DMA engine masks the low bits of the WRAM
    // address, so a stack local, whose alignment the compiler is free to
    // weaken, must never be handed to it.
    __dma_aligned uint32_t nr_refreshes[2];
    __dma_aligned KeyRange refresh_range;
    __dma_aligned KVPair refresh_response;
    uint32_t idx_qry_in_cache;
    uintptr_t cursor_on_qrys;
    uint32_t idx_result_in_cache;
    uintptr_t cursor_on_results;
} DeleteWorkspace;
_Static_assert(TASK_DELETE_NR_CACHED_RESULTS % 8 == 0, "TASK_DELETE_NR_CACHED_RESULTS % 8 == 0");


#if SUPPORT_RANGE_MIN
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
#endif /* if SUPPORT_RANGE_MIN */


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned RangeCountQuery qrys[TASK_RANGE_COUNT_NR_CACHED_QRYS];
    __dma_aligned uint64_t results[TASK_RANGE_COUNT_NR_CACHED_RESULTS];
} RCQWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned KeyRange qrys[TASK_RANGE_MAX_NR_CACHED_QRYS];
    __dma_aligned value_uint64_t results[TASK_RANGE_MAX_NR_CACHED_RESULTS];
} RMaxQWorkspace;


typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned key_uint64_t qrys[TASK_PRED_NR_CACHED_QRYS];
    __dma_aligned KVPair results[TASK_PRED_NR_CACHED_RESULTS];
    uint32_t idx_qry_in_cache;
    uintptr_t cursor_on_qrys;
    uint32_t idx_result_in_cache;
    uintptr_t cursor_on_results;
} PredWorkspace;


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

typedef struct {
    union {
        __dma_aligned LeafNode leaf_cache;
        __dma_aligned NodeLink children_cache[2];
    };
    __dma_aligned KVPair pairs[TASK_SERIALIZE_NR_CACHED_KVPAIRS];
    __dma_aligned key_uint64_t delims[TASK_SERIALIZE_NR_CACHED_DELIMS];
    __dma_aligned uint32_t incisions[TASK_SERIALIZE_NR_CACHED_INCISIONS];
    uint32_t nr_pairs, idx_pair_in_cache;
    uintptr_t cursor_on_pairs;
    uint32_t nr_delims, idx_delim_in_cache;
    uintptr_t cursor_on_delims;
    uint32_t idx_incision_in_cache;
    uintptr_t cursor_on_incisions;
} SerializeWorkspace;

typedef struct {
    __dma_aligned NodeLink children_cache[2];
    NodeLink node;
    uint8_t nr_visited_children;
} ClearTreeStackElem;
typedef struct {
    __dma_aligned NodeLink children_cache[MAX_NR_CHILDREN];
    ClearTreeStackElem stack[MAX_HEIGHT - 2];
} ClearTreeWorkspace;

typedef union {
    InitWorkspace init[TREE_CONSTRUCT_NR_TASKLETS];
    SummarizeWorkspace summarize;
    SerializeWorkspace serialize[TASK_SERIALIZE_NR_TASKLETS];
    ClearTreeWorkspace clear;
    GetWorkspace get[TASK_GET_NR_TASKLETS];
    PredWorkspace pred[TASK_PRED_NR_TASKLETS];
    InsertWorkspace insert[TASK_INSERT_NR_TASKLETS];
    DeleteWorkspace delete[TASK_DELETE_NR_TASKLETS];
    RCQWorkspace rcq[TASK_RANGE_COUNT_NR_TASKLETS];
    RMaxQWorkspace rmaxq[TASK_RANGE_MAX_NR_TASKLETS];
#if SUPPORT_RANGE_MIN
    RMQWorkspace rmq;
#endif
} TreeWorkspace;
