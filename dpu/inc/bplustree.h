#pragma once

#include "bit_ops_macro.h"
#include "common.h"
#include "dpu_params.h"
#include "node_ptr.h"
#include "workload_types.h"

#include <attributes.h>
#include <limits.h>
#include <stdbool.h>
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
#define MAX_NR_CHILDREN (SIZEOF_NODE / 12 / 2 * 2)
#define MIN_NR_CHILDREN ((MAX_NR_CHILDREN + 1) / 2)
#else
#define MAX_NR_CHILDREN ((SIZEOF_NODE + 8) / 12 / 2 * 2)
#define MIN_NR_CHILDREN ((MAX_NR_CHILDREN + 1) / 2)
#endif

#define MAX_NR_PAIRS ((SIZEOF_NODE - 16) / (sizeof(key_uint64_t) + sizeof(value_int64_t)))
#define MIN_NR_PAIRS ((MAX_NR_PAIRS + 1) / 2)

// Bidirectional layout: the children/values array is filled backward from its
// end and the keys array forward.
typedef struct {
    __dma_aligned NodeLink children[MAX_NR_CHILDREN];
#ifdef DEBUG_OCCUPANCY
    unsigned numKeys;
#endif
    __dma_aligned key_uint64_t keys[MAX_NR_CHILDREN - 1];
} InternalNode;
typedef struct {
    __dma_aligned value_int64_t values[MAX_NR_PAIRS];
    __dma_aligned NodeLink right;
    __dma_aligned NodePtr left;
#ifdef DEBUG_OCCUPANCY
    unsigned numKeys;
#endif
    __dma_aligned key_uint64_t keys[MAX_NR_PAIRS];
} LeafNode;

typedef union {
    InternalNode inl;
    LeafNode lf;
    char size_adjuster[SIZEOF_NODE];
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");

#define NthChild(inl, n) ((inl).children[MAX_NR_CHILDREN - 1 - (unsigned)(n)])
#define NthValue(lf, n) ((lf).values[MAX_NR_PAIRS - 1 - (unsigned)(n)])
#define RevValues(lf) (&NthValue(lf, 0))


// HEIGHT <= log_{MIN_NR_CHILDREN} [ (MAX_NR_NODES - 1) * (MIN_NR_CHILDREN - 1) / 2.0 + 1 ]
#define MAX_HEIGHT ((CEIL_LOG2_UINT32((MAX_NR_NODES - 1) * (MIN_NR_CHILDREN - 1) + 2) - 1) / FLOOR_LOG2_UINT32(MIN_NR_CHILDREN))

#define MAX_INSERT_DEPTH (MAX_HEIGHT - 1)

#define TASK_INSERT_SORT_NR_PIECES (1u << TASK_INSERT_SORT_RADIX_BITS)
#define TASK_INSERT_SORT_NR_LEVELS ((KEY_WIDTH + TASK_INSERT_SORT_RADIX_BITS - 1) / TASK_INSERT_SORT_RADIX_BITS)
#define TASK_INSERT_SORT_STACK_CAPACITY (TASK_INSERT_SORT_NR_PIECES + (TASK_INSERT_SORT_NR_LEVELS - 1) * (TASK_INSERT_SORT_NR_PIECES - 1))

// make it positive, since it will be array length
#define TASK_INSERT_MAX_NR_FORK (TASK_INSERT_NR_TASKLETS == 1 ? 1 : TASK_INSERT_NR_TASKLETS / 2)


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
    key_uint64_t qry_min_key, qry_max_key;
    uint32_t shift;
    uint32_t piece_delims[TASK_INSERT_SORT_NR_PIECES + 1];
} InsertSortTopDigit;

typedef struct {
    __dma_aligned KVPair buf[TASK_INSERT_SORT_NR_TASKLETS][TASK_INSERT_SORT_RUN];
    __dma_aligned KVPair digit_buf[TASK_INSERT_SORT_NR_TASKLETS][TASK_INSERT_SORT_NR_PIECES][TASK_INSERT_SORT_DIGIT_BUF];
    uint8_t nr_in_digit_buf[TASK_INSERT_SORT_NR_TASKLETS][TASK_INSERT_SORT_NR_PIECES];
    union {
        struct {
            key_uint64_t qry_min_key[TASK_INSERT_SORT_NR_TASKLETS], qry_max_key[TASK_INSERT_SORT_NR_TASKLETS];
        };
        struct {
            uint32_t counts[TASK_INSERT_SORT_NR_TASKLETS][TASK_INSERT_SORT_NR_PIECES];
            uint16_t top_digit_split[TASK_INSERT_SORT_NR_TASKLETS + 1];
        };
    };
} InsertSortWorkspace;
_Static_assert(sizeof(KVPair) * TASK_INSERT_SORT_RUN <= 2048, "sizeof(KVPair) * TASK_INSERT_SORT_RUN <= 2048");

typedef struct {
    __dma_aligned uint32_t begin;
    uint32_t end;
    uint32_t prev_shift;
    __mram_ptr KVPair* src;
} InsertSortStackEntry;

typedef struct {
    uint8_t end_child;
    uint8_t nr_tasklets;
} TaskletAssignmentToChildren;

typedef struct {
    // input
    // @{
    uint32_t idx_qry_begin, idx_qry_end;
    uint8_t node_numKeys;
    uint8_t nr_tasklets;
    // @}

    uint32_t backet_ends[MAX_NR_CHILDREN - 1];

    uint32_t nr_backet_qrys[MAX_NR_CHILDREN];
    TaskletAssignmentToChildren assignments[TASK_INSERT_NR_TASKLETS];
    uint8_t forks[TASK_INSERT_MAX_NR_FORK];  // indices in `assignments`
    uint8_t nr_forks;
} InsertPartitioning;

typedef struct {
#define TASK_INSERT_PARTITIONING_LEADER(height) (UINT8_MAX - (height))
    // 0 if empty partition, TASK_INSERT_PARTITIONING_LEADER(height) if me() is a partitioning leader at the height
    uint8_t idx_child_end;

    uint8_t idx_child_begin;
    uint8_t idx_partitioning;
    uint8_t parent_height;
    uint32_t idx_qry_begin, idx_qry_end;
    key_uint64_t min_key;
} InsertPartition;

typedef struct {
#define TASK_INSERT_MAX_NR_PARTITIONINGS (TASK_INSERT_NR_TASKLETS == 1 ? 1 : TASK_INSERT_NR_TASKLETS - 1)
    InsertPartitioning partitionings[TASK_INSERT_MAX_NR_PARTITIONINGS];
    unsigned nr_partitionings;
    InsertPartition partitions[TASK_INSERT_NR_TASKLETS];
} InsertPartitionWorkspace;

//! @brief One level of the path an insertion took, level 0 being the leaf.
typedef struct {
    key_uint64_t max_key;
    NodeLink link;
    unsigned idx_in_parent;
} PathLevel;

typedef struct {
    __dma_aligned KVPair qrys[TASK_INSERT_NR_CACHED_QRYS];
    PathLevel path[MAX_HEIGHT + 1];
    bool leaf_loaded, leaf_dirty, parent_loaded;
    uint32_t nr_new_pairs;
} TaskletLocalInsertWorkspace;
_Static_assert(sizeof(KVPair) * TASK_INSERT_NR_CACHED_QRYS <= 2048, "sizeof(KVPair) * TASK_INSERT_NR_CACHED_QRYS <= 2048");

typedef struct {
    InsertPartitionWorkspace part;
    __dma_aligned Node node_cache[TASK_INSERT_NR_TASKLETS][4];

    NodeLink task_tree[TASK_INSERT_NR_TASKLETS];
    uint8_t task_tree_height[TASK_INSERT_NR_TASKLETS];
    key_uint64_t task_min_key[TASK_INSERT_NR_TASKLETS];
    //! The ends of each tree's chain of leaves, which the joining keeps up to
    //! date: a tree's chain stays closed until the tree is joined.
    NodeLink leftmost_leaf[TASK_INSERT_NR_TASKLETS], rightmost_leaf[TASK_INSERT_NR_TASKLETS];

    TaskletLocalInsertWorkspace th[TASK_INSERT_NR_TASKLETS];
} InsertPhysWorkspace;

//! @brief The stages run one after another in a single launch, so they share
//! the workspace.  The top digit of the sort outlives the stage that fills it,
//! since the handout reads it, so it sits outside the union.
typedef struct {
    union {
        InsertSortWorkspace sort;
        InsertPhysWorkspace phys;
    };
    InsertSortTopDigit sort_top_digit;
} InsertWorkspace;


#define TASK_DELETE_SORT_NR_PIECES (1u << TASK_DELETE_SORT_RADIX_BITS)
#define TASK_DELETE_SORT_NR_LEVELS ((KEY_WIDTH + TASK_DELETE_SORT_RADIX_BITS - 1) / TASK_DELETE_SORT_RADIX_BITS)
#define TASK_DELETE_SORT_STACK_CAPACITY (TASK_DELETE_SORT_NR_PIECES + (TASK_DELETE_SORT_NR_LEVELS - 1) * (TASK_DELETE_SORT_NR_PIECES - 1))

// make it positive, since it will be array length
#define TASK_DELETE_MAX_NR_FORK (TASK_DELETE_NR_TASKLETS == 1 ? 1 : TASK_DELETE_NR_TASKLETS / 2)
#define TASK_DELETE_MAX_NR_PARTITIONINGS (TASK_DELETE_NR_TASKLETS == 1 ? 1 : TASK_DELETE_NR_TASKLETS - 1)

//! @brief Stage 1: what one tasklet needs to turn its slice of the batch into
//! the result flags.  @sa /docs/parallel_delete.md
typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned key_uint64_t qrys[TASK_DELETE_NR_CACHED_QRYS];
    //! MRAM (or, for a height-0 tree, WRAM) addresses of the value slots the
    //! first pass found, one per query, so the second pass need not descend.
    __dma_aligned uintptr_t slot_addrs[TASK_DELETE_NR_CACHED_QRYS];
    __dma_aligned uint8_t results[TASK_DELETE_NR_CACHED_QRYS];
    __dma_aligned KeyRange refresh_range;
    __dma_aligned KVPair refresh_response;
} DeleteResultWorkspace;
_Static_assert(sizeof(key_uint64_t) * TASK_DELETE_NR_CACHED_QRYS <= 2048, "sizeof(key_uint64_t) * TASK_DELETE_NR_CACHED_QRYS <= 2048");

typedef struct {
    __dma_aligned key_uint64_t buf[TASK_DELETE_SORT_NR_TASKLETS][TASK_DELETE_SORT_RUN];
    __dma_aligned key_uint64_t digit_buf[TASK_DELETE_SORT_NR_TASKLETS][TASK_DELETE_SORT_NR_PIECES][TASK_DELETE_SORT_DIGIT_BUF];
    uint8_t nr_in_digit_buf[TASK_DELETE_SORT_NR_TASKLETS][TASK_DELETE_SORT_NR_PIECES];
    union {
        struct {
            key_uint64_t qry_min_key[TASK_DELETE_SORT_NR_TASKLETS], qry_max_key[TASK_DELETE_SORT_NR_TASKLETS];
        };
        struct {
            uint32_t counts[TASK_DELETE_SORT_NR_TASKLETS][TASK_DELETE_SORT_NR_PIECES];
            uint16_t top_digit_split[TASK_DELETE_SORT_NR_TASKLETS + 1];
        };
    };
} DeleteSortWorkspace;
_Static_assert(sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN <= 2048, "sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN <= 2048");

typedef struct {
    key_uint64_t qry_min_key, qry_max_key;
    uint32_t shift;
    uint32_t piece_delims[TASK_DELETE_SORT_NR_PIECES + 1];
} DeleteSortTopDigit;

typedef struct {
    __dma_aligned uint32_t begin;
    uint32_t end;
    uint32_t prev_shift;
    __mram_ptr key_uint64_t* src;
} DeleteSortStackEntry;

typedef struct {
    // input
    // @{
    uint32_t idx_qry_begin, idx_qry_end;
    uint8_t node_numKeys;
    uint8_t nr_tasklets;
    // @}

    uint32_t backet_ends[MAX_NR_CHILDREN - 1];

    uint32_t nr_backet_qrys[MAX_NR_CHILDREN];
    TaskletAssignmentToChildren assignments[TASK_DELETE_NR_TASKLETS];
    uint8_t forks[TASK_DELETE_MAX_NR_FORK];  // indices in `assignments`
    uint8_t nr_forks;
} DeletePartitioning;

typedef struct {
#define TASK_DELETE_PARTITIONING_LEADER(height) (UINT8_MAX - (height))
    // 0 if empty partition, TASK_DELETE_PARTITIONING_LEADER(height) if me() is a partitioning leader at the height
    uint8_t idx_child_end;

    uint8_t idx_child_begin;
    uint8_t idx_partitioning;
    uint8_t parent_height;
    uint32_t idx_qry_begin, idx_qry_end;
    key_uint64_t min_key;
} DeletePartition;

typedef struct {
    DeletePartitioning partitionings[TASK_DELETE_MAX_NR_PARTITIONINGS];
    unsigned nr_partitionings;
    DeletePartition partitions[TASK_DELETE_NR_TASKLETS];
} DeletePartitionWorkspace;

typedef struct {
    __dma_aligned key_uint64_t qrys[TASK_DELETE_NR_CACHED_QRYS];
    PathLevel path[MAX_HEIGHT + 1];
    bool leaf_loaded, leaf_dirty, parent_loaded, path_stale;
    uint32_t nr_removed_pairs;
} TaskletLocalDeleteWorkspace;

//! @brief Stage 2: the workspace of the physical deletion.
typedef struct {
    DeletePartitionWorkspace part;
    __dma_aligned Node node_cache[TASK_DELETE_NR_TASKLETS][4];

    NodeLink task_tree[TASK_DELETE_NR_TASKLETS];
    uint8_t task_tree_height[TASK_DELETE_NR_TASKLETS];
    key_uint64_t task_min_key[TASK_DELETE_NR_TASKLETS];
    //! The ends of each tree's chain of leaves, which the joining keeps up to
    //! date: a tree's chain stays closed until the tree is joined.
    NodeLink leftmost_leaf[TASK_DELETE_NR_TASKLETS], rightmost_leaf[TASK_DELETE_NR_TASKLETS];

    TaskletLocalDeleteWorkspace th[TASK_DELETE_NR_TASKLETS];
} DeletePhysWorkspace;

//! @brief Where everything TASK_DELETE reads and writes sits in MRAM: the parts
//! the host lays out (docs/dpu_task_signature.md) and, after them, the scratch
//! the DPU keeps to itself.  One tasklet works it out and the rest read it.
typedef struct {
    //! How many smallest-live-key requests each tree got, which is what says
    //! where the host-visible results end and the DPU's own scratch starts.
    __dma_aligned uint32_t nr_refreshes[2];
    __mram_ptr key_uint64_t* qrys;
    uint32_t nr_cold_qrys, nr_hot_qrys;
    uintptr_t refresh_requests;
    //! One byte per query, the two trees' sections padded apart to 8 bytes.
    uintptr_t cold_results, hot_results;
    uintptr_t counts_result, refresh_results;
    //! One value-slot address per query, handed from the first pass of stage 1
    //! to the second.  Padded to an even count so every slice stays aligned.
    uintptr_t cold_slots, hot_slots;
    //! Room for a second copy of the keys of whichever tree has more, and for
    //! one sort stack per tasklet after it.
    __mram_ptr key_uint64_t* sort_scratch;
} DeleteLayout;

//! @brief The stages run one after another in a single launch, so they share
//! the workspace.  What outlives the stage that fills it lives outside the
//! union: the MRAM layout, and the top digit of each tree's sort, which the
//! handout reads.
typedef struct {
    union {
        DeleteResultWorkspace th[TASK_DELETE_NR_TASKLETS];
        DeleteSortWorkspace sort;
        DeletePhysWorkspace phys;
    };
    DeleteLayout layout;
    DeleteSortTopDigit sort_top_digit[2];
} DeleteWorkspace;


#if SUPPORT_RANGE_MIN
typedef struct {
    __dma_aligned Node node_cache;
    __dma_aligned uint16_t lump_end_indices[TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES];
    __dma_aligned key_uint64_t delim_keys[TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS];
    __dma_aligned value_int64_t results[TASK_RANGE_MIN_NR_CACHED_RESULTS];
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
    __dma_aligned value_int64_t results[TASK_RANGE_MAX_NR_CACHED_RESULTS];
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
    InsertWorkspace insert;
    DeleteWorkspace delete;
    RCQWorkspace rcq[TASK_RANGE_COUNT_NR_TASKLETS];
    RMaxQWorkspace rmaxq[TASK_RANGE_MAX_NR_TASKLETS];
#if SUPPORT_RANGE_MIN
    RMQWorkspace rmq;
#endif
} TreeWorkspace;
