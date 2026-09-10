#include "bplustree.h"

#include "tree.h"

#include "allocator.h"
#include "bit_ops.h"
#include "bit_ops_macro.h"
#include "common.h"
#include "div_by_const.h"
#include "dpu_params.h"
#include "input_header.h"
#include "iram_overlay.h"
#include "node_ptr.h"
#include "sync.h"
#include "workload_types.h"
#include "workspace.h"

#include <attributes.h>
#include <defs.h>
#include <mram.h>
#include <mutex_pool.h>

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>


#define DEBUG_PRINT(datum) printf("th[%02d] " __FILE__ ":%d: " #datum " = %u (0x%x)\n", me(), __LINE__, datum, datum)
#define DEBUG_PRINT_L(datum) printf("th[%02d] " __FILE__ ":%d: " #datum " = %lu (0x%lx)\n", me(), __LINE__, datum, datum)


static DEFINE_DIV_BY(MAX_NR_PAIRS, BITWIDTH_UINT32(MAX_NR_PAIRS* MAX_NR_NODES), _NR_PAIRS);
static DEFINE_DIV_BY(MAX_NR_CHILDREN, NODE_PTR_WIDTH, _NR_NODES);

static DEFINE_DIV_BY(TREE_CONSTRUCT_NR_TASKLETS, NODE_PTR_WIDTH, _NR_NODES);
static DEFINE_DIV_BY(TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT, NODE_PTR_WIDTH, _NR_NODES);

#ifdef SUPPORT_INSERT
static DEFINE_DIV_BY(TASK_INSERT_SORT_NR_TASKLETS, 32, _NR_QRYS);
#endif

#ifdef SUPPORT_DELETE
static DEFINE_DIV_BY(TASK_DELETE_NR_TASKLETS, 32, _NR_QRYS);
static DEFINE_DIV_BY(TASK_DELETE_SORT_NR_TASKLETS, 32, _NR_QRYS);
#endif

#ifdef SUPPORT_GET
static DEFINE_DIV_BY(TASK_GET_NR_TASKLETS, 32, _NR_QRYS);
#endif

#ifdef SUPPORT_PRED
static DEFINE_DIV_BY(TASK_PRED_NR_TASKLETS, 32, _NR_QRYS);
#endif

#ifdef SUPPORT_RANGE_MIN
static DEFINE_DIV_BY(TASK_RANGE_MIN_NR_TASKLETS, 32, _NR_DELIMS);
#endif

#ifdef SUPPORT_RANGE_COUNT
static DEFINE_DIV_BY(TASK_RANGE_COUNT_NR_TASKLETS, 32, _NR_QRYS);
#endif

#ifdef SUPPORT_RANGE_MAX
static DEFINE_DIV_BY(TASK_RANGE_MAX_NR_TASKLETS, 32, _NR_QRYS);
#endif

Node cold_root, hot_root;
key_uint64_t cold_min_key, hot_min_key;
uint8_t cold_height, hot_height;
uint8_t cold_root_numKeys, hot_root_numKeys;
typedef struct {
    uint32_t cold, hot;
} NrPairs;
__dma_aligned NrPairs nr_pairs;

static void report_nr_pairs(__mram_ptr NrPairs* const dest)
{
    mram_write(&nr_pairs, dest, sizeof(nr_pairs));
}


static uint16_t search_for_child_index(const key_uint64_t* delim_keys, uint8_t nr_keys, key_uint64_t query)
{
    // candidate: [left_m1 + 1, right_m1 + 1)
    uint16_t left_m1 = UINT16_MAX, right_m1 = nr_keys;
    while ((uint16_t)(left_m1 + UINT16_C(1)) < right_m1) {
        const uint16_t probe_m1 = (uint16_t)(left_m1 + right_m1) / 2;
        if (delim_keys[probe_m1] > query) {
            right_m1 = probe_m1;
        } else {
            left_m1 = probe_m1;
        }
    }
    return right_m1;
}
static __attribute__((unused)) uint16_t search_for_pair_index(const key_uint64_t* keys, uint8_t nr_keys, key_uint64_t query)
{
    // candidate: [left_m1 + 1, right_m1 + 1)
    uint16_t left_m1 = UINT16_MAX, right_m1 = nr_keys;
    while ((uint16_t)(left_m1 + UINT16_C(1)) < right_m1) {
        const uint16_t probe_m1 = (uint16_t)(left_m1 + right_m1) / 2;
        if (keys[probe_m1] >= query) {
            right_m1 = probe_m1;
        } else {
            left_m1 = probe_m1;
        }
    }
    return right_m1;
}


#define NODE_DMA_BEGIN_MASK UINT32_C(0xffffff)
#define NODE_DMA_SPEC(begin, nr_bytes) ((uint32_t)((nr_bytes) / 8 - 1) << 24 | (uint32_t)(begin))

#define LEAF_FILLED_BEGIN(nr_keys) (offsetof(LeafNode, values) + sizeof(value_int64_t) * (MAX_NR_PAIRS - (nr_keys)))
#define LEAF_FILLED_END(nr_keys) (offsetof(LeafNode, keys) + sizeof(key_uint64_t) * (nr_keys))
#define LEAF_DMA_SPEC(nr_keys) NODE_DMA_SPEC(LEAF_FILLED_BEGIN(nr_keys), LEAF_FILLED_END(nr_keys) - LEAF_FILLED_BEGIN(nr_keys))
#define INTERNAL_FILLED_BEGIN(nr_keys) ((offsetof(InternalNode, children) + sizeof(NodeLink) * (MAX_NR_CHILDREN - ((nr_keys) + 1))) & ~(uintptr_t)7)
#define INTERNAL_FILLED_END(nr_keys) (offsetof(InternalNode, keys) + sizeof(key_uint64_t) * (nr_keys))
#define INTERNAL_DMA_SPEC(nr_keys) NODE_DMA_SPEC(INTERNAL_FILLED_BEGIN(nr_keys), INTERNAL_FILLED_END(nr_keys) - INTERNAL_FILLED_BEGIN(nr_keys))
_Static_assert(SIZEOF_NODE <= 2048, "a node fetch fits in one DMA");

#ifdef DPU_ON_CPU
static inline void dma_node_from_mram(const __mram_ptr void* src, void* dst, uint32_t spec)
{
    const uint32_t begin = spec & NODE_DMA_BEGIN_MASK;
    mram_read((const __mram_ptr void*)((uintptr_t)src + begin), (void*)((uintptr_t)dst + begin), ((spec >> 24) + 1) * 8);
}
static inline void dma_node_to_mram(const void* src, __mram_ptr void* dst, uint32_t spec)
{
    const uint32_t begin = spec & NODE_DMA_BEGIN_MASK;
    mram_write((const void*)((uintptr_t)src + begin), (__mram_ptr void*)((uintptr_t)dst + begin), ((spec >> 24) + 1) * 8);
}
#else
static inline void dma_node_from_mram(const __mram_ptr void* src, void* dst, uint32_t spec)
{
    __asm__ volatile("ldma %[wram], %[mram], 0"
                     :
                     : [wram] "r"((uintptr_t)dst + spec), [mram] "r"((uintptr_t)src + (spec & NODE_DMA_BEGIN_MASK))
                     : "memory");
}
static inline void dma_node_to_mram(const void* src, __mram_ptr void* dst, uint32_t spec)
{
    __asm__ volatile("sdma %[wram], %[mram], 0"
                     :
                     : [wram] "r"((uintptr_t)src + spec), [mram] "r"((uintptr_t)dst + (spec & NODE_DMA_BEGIN_MASK))
                     : "memory");
}
#endif /* DPU_ON_CPU */

#if NODE_DMA_TABLE
#define NODE_DMA_IDX(n, last) ((n) <= (last) ? (n) : 0)
#define NODE_DMA_R1(F, base) F(base)
#define NODE_DMA_R2(F, base) NODE_DMA_R1(F, base), NODE_DMA_R1(F, (base) + 1)
#define NODE_DMA_R4(F, base) NODE_DMA_R2(F, base), NODE_DMA_R2(F, (base) + 2)
#define NODE_DMA_R8(F, base) NODE_DMA_R4(F, base), NODE_DMA_R4(F, (base) + 4)
#define NODE_DMA_R16(F, base) NODE_DMA_R8(F, base), NODE_DMA_R8(F, (base) + 8)
#define NODE_DMA_R32(F, base) NODE_DMA_R16(F, base), NODE_DMA_R16(F, (base) + 16)
#define NODE_DMA_R64(F, base) NODE_DMA_R32(F, base), NODE_DMA_R32(F, (base) + 32)
#define NODE_DMA_R128(F, base) NODE_DMA_R64(F, base), NODE_DMA_R64(F, (base) + 64)
#define NODE_DMA_R256(F) NODE_DMA_R128(F, 0), NODE_DMA_R128(F, 128)
#define LEAF_DMA_ENTRY(n) [NODE_DMA_IDX(n, MAX_NR_PAIRS)] = LEAF_DMA_SPEC(NODE_DMA_IDX(n, MAX_NR_PAIRS))
#define INTERNAL_DMA_ENTRY(n) [NODE_DMA_IDX(n, MAX_NR_CHILDREN - 1)] = INTERNAL_DMA_SPEC(NODE_DMA_IDX(n, MAX_NR_CHILDREN - 1))

_Static_assert(MAX_NR_PAIRS < 256 && MAX_NR_CHILDREN <= 256, "the tables cover occupancies below 256");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winitializer-overrides"
static const uint32_t LeafDma[MAX_NR_PAIRS + 1] = {NODE_DMA_R256(LEAF_DMA_ENTRY)};
static const uint32_t InternalDma[MAX_NR_CHILDREN] = {NODE_DMA_R256(INTERNAL_DMA_ENTRY)};
#pragma clang diagnostic pop

#define LEAF_DMA_OF(nr_keys) LeafDma[nr_keys]
#define INTERNAL_DMA_OF(nr_keys) InternalDma[nr_keys]
#else
#define LEAF_DMA_OF(nr_keys) LEAF_DMA_SPEC(nr_keys)
#define INTERNAL_DMA_OF(nr_keys) INTERNAL_DMA_SPEC(nr_keys)
#endif

__attribute__((noinline)) static void fetch_leaf_filled(const __mram_ptr LeafNode* src, LeafNode* dst, unsigned numKeys)
{
    dma_node_from_mram(src, dst, LEAF_DMA_OF(numKeys));
}
__attribute__((noinline)) static void store_leaf_filled(const LeafNode* src, __mram_ptr LeafNode* dst, unsigned numKeys)
{
    dma_node_to_mram(src, dst, LEAF_DMA_OF(numKeys));
}

__attribute__((noinline)) static void fetch_internal_filled(const __mram_ptr InternalNode* src, InternalNode* dst, unsigned numKeys)
{
    dma_node_from_mram(src, dst, INTERNAL_DMA_OF(numKeys));
}
__attribute__((noinline)) static void store_internal_filled(const InternalNode* src, __mram_ptr InternalNode* dst, unsigned numKeys)
{
    dma_node_to_mram(src, dst, INTERNAL_DMA_OF(numKeys));
}


// Optimization barrier for loop-invariant addresses: at every -O level the
// compiler rematerializes the me()-scaled workspace address at each use instead
// of keeping it in a register.  The empty asm makes the value opaque.
static inline void* loop_invariant(void* p)
{
    __asm__("" : "+r"(p));
    return p;
}


#if defined(TASK_INIT_CHECK) || defined(TASK_INSERT_CHECK) || defined(TASK_DELETE_CHECK) || defined(TASK_MOVE_HOT_CHECK)
#define TASK_TREE_CHECK
#endif
OVERLAY_LOCAL(OVL_SLOT_CHECK)
__attribute__((unused)) static bool check_tree_structure(const Node* root, unsigned height, unsigned root_numKeys);
#ifdef TASK_TREE_CHECK
static void CHECK_trees(void);
#endif


//! @sa /docs/tree_initialization.md
//! @return Sum of nr. of KV pairs that [0, idx_leaf)-th leaves have
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static unsigned TREE_CONSTRUCT_idx_leaf_to_idx_pair(unsigned idx_leaf, unsigned nr_leaves, bool is_2nd_last_leaf_not_full, unsigned nr_pairs)
{
    return (idx_leaf + is_2nd_last_leaf_not_full < nr_leaves ? idx_leaf * MAX_NR_PAIRS
                                                             : (idx_leaf == nr_leaves ? nr_pairs
                                                                                      : nr_pairs - MIN_NR_PAIRS));
}
//! @sa /docs/tree_initialization.md
//! @return Sum of nr. of children that [0, idx_parent)-th parents have
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static unsigned TREE_CONSTRUCT_idx_parent_to_idx_child(unsigned idx_parent, unsigned nr_parents, bool is_2nd_last_parent_not_full, unsigned nr_children)
{
    return (idx_parent + is_2nd_last_parent_not_full < nr_parents ? idx_parent * MAX_NR_CHILDREN
                                                                  : (idx_parent == nr_parents ? nr_children
                                                                                              : nr_children - MIN_NR_CHILDREN));
}
//! @sa /docs/tree_initialization.md
//! @return max{ i | TREE_CONSTRUCT_idx_parent_to_idx_child(i, nr_parents, _) <= idx_chlid }
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static unsigned TREE_CONSTRUCT_idx_child_to_idx_parent(unsigned idx_child, unsigned nr_children, unsigned nr_parents)
{
    return (idx_child + MIN_NR_CHILDREN < nr_children ? DIV_NR_NODES_BY_MAX_NR_CHILDREN(idx_child)
                                                      : (idx_child < nr_children ? nr_parents - 1
                                                                                 : nr_parents));
}

OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void TREE_CONSTRUCT_receive_lifted_links_from_junior(unsigned nr_children_received, Node* dest_node, key_uint64_t* key_min_subtree)
{
    unsigned idx_junior = me() - 1, nr_lift_left_in_this_junior = workspace.tree.init[idx_junior].out.nr_cached_lift;
    for (unsigned idx_child_in_this_node_p1 = nr_children_received; idx_child_in_this_node_p1 > 0; idx_child_in_this_node_p1--, nr_lift_left_in_this_junior--) {
        while (nr_lift_left_in_this_junior == 0) {
            idx_junior--;
            nr_lift_left_in_this_junior = workspace.tree.init[idx_junior].out.nr_cached_lift;
        }
        const unsigned idx_child_in_this_node = idx_child_in_this_node_p1 - 1;
        const LinkLift* const p_lift = &workspace.tree.init[idx_junior].out.lifted[nr_lift_left_in_this_junior - 1];
        const key_uint64_t key = ((key_uint64_t)p_lift->key_parts[0] << 32) + p_lift->key_parts[1];
        if (idx_child_in_this_node == 0) {
            *key_min_subtree = key;
        } else {
            dest_node->inl.keys[idx_child_in_this_node - 1] = key;
        }
        NthChild(dest_node->inl, idx_child_in_this_node) = p_lift->child;
    }
}

//! @sa /docs/tree_initialization.md
//! @return number of the allocated nodes
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static unsigned construct_tree(const uintptr_t initial_pairs, const uint32_t nr_pairs,
    uint8_t* root_numKeys, Node* root, uint8_t* height, key_uint64_t* min_key, uint32_t* p_nr_pairs, NodePtr (*allocator)(unsigned))
{
    InitWorkspace* const wks = &workspace.tree.init[me()];

    if (nr_pairs == 0) {
        if (me() == 0) {
            *height = 0;
#ifdef DEBUG_OCCUPANCY
            root->lf.numKeys = 0;
#endif
            *root_numKeys = 0;
            *p_nr_pairs = 0;
        }
        return 0;
    }

    assert(nr_pairs <= (MAX_NR_PAIRS * MAX_NR_NODES));
    unsigned nr_nodes = DIV_NR_PAIRS_BY_MAX_NR_PAIRS(nr_pairs + MAX_NR_PAIRS - 1);
    const unsigned nr_leaves = nr_nodes;
    Node* node_cache = (nr_nodes == 1 ? root : &wks->node);

    const unsigned nr_leaves_per_tasklet = DIV_NR_NODES_BY_TREE_CONSTRUCT_NR_TASKLETS(nr_nodes),
                   nr_remainder_leaves = nr_nodes - nr_leaves_per_tasklet * TREE_CONSTRUCT_NR_TASKLETS,
                   nr_leaves_for_me = nr_leaves_per_tasklet + (me() < nr_remainder_leaves);
    unsigned idx_node_begin = nr_leaves_per_tasklet * me() + (me() <= nr_remainder_leaves ? me() : nr_remainder_leaves),
             idx_node_end = idx_node_begin + nr_leaves_for_me;

    const bool is_2nd_last_node_not_full = nr_nodes > 1u && MAX_NR_PAIRS * (nr_nodes - 1u) + MIN_NR_PAIRS > nr_pairs;
    unsigned idx_pair = TREE_CONSTRUCT_idx_leaf_to_idx_pair(idx_node_begin, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
    const uintptr_t pairs_for_me = initial_pairs + sizeof(KVPair) * idx_pair;
    mram_read((__mram_ptr KVPair*)pairs_for_me, &wks->in.pairs[0], sizeof(KVPair) * TREE_CONSTRUCT_NR_CACHED_KVPAIRS);
    if (me() == 0) {
        *min_key = wks->in.pairs[0].key;
        *p_nr_pairs = nr_pairs;
    }

    unsigned nr_parents = DIV_NR_NODES_BY_MAX_NR_CHILDREN(nr_nodes + MAX_NR_CHILDREN - 1),
             idx_parent_begin = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_begin, nr_nodes, nr_parents),
             idx_parent_end = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_end, nr_nodes, nr_parents);

    bool is_2nd_last_parent_not_full = nr_parents > 1u && MAX_NR_CHILDREN * (nr_parents - 1u) + MIN_NR_CHILDREN > nr_nodes;
    unsigned idx_node_begin_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_begin, nr_parents, is_2nd_last_parent_not_full, nr_nodes),
             idx_node_end_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_end, nr_parents, is_2nd_last_parent_not_full, nr_nodes);

    bool is_any_node_sent_from_junior_to_senior = idx_node_end_used_by_me < idx_node_begin;
    const unsigned idx_node_begin_sent_to_senior = (is_any_node_sent_from_junior_to_senior ? idx_node_begin : idx_node_end_used_by_me),
                   nr_nodes_not_sent = idx_node_begin_sent_to_senior - idx_node_begin;

    unsigned idx_pair_cache = 0;
    // To place (idx_node_begin_sent_to_senior)-th node in wks->out.lifted[0], where should (idx_node_begin)-th be placed?
    //     -> wks->out.lifted[idx_lift_cache_begin]
    unsigned idx_lift_cache_begin = DIV_NR_NODES_BY_TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT(nr_nodes_not_sent + TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT - 1u)
                                        * TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT
                                    - nr_nodes_not_sent,
             idx_lift_cache = idx_lift_cache_begin;

    unsigned lifted_links_offset = (idx_lift_cache_begin % 2) * 4;  // (idx_lift_cache_begin % 2 != 0 ? 4 : 0)
    uintptr_t lifted_links = pairs_for_me + lifted_links_offset;

    wks->first_leaf = NODELINK_NULLPTR;
    wks->last_leaf = NODE_NULLPTR;
    if (idx_node_begin < idx_node_end) {
        unsigned idx_node = idx_node_begin;
        unsigned idx_pair_end_for_this_node = TREE_CONSTRUCT_idx_leaf_to_idx_pair(idx_node + 1, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
        NodePtr left_node = NODE_NULLPTR;
        unsigned numKeys_in_this_node = idx_pair_end_for_this_node - idx_pair;
        NodeLink link_to_this_node = {(nr_nodes == 1 ? NODE_NULLPTR : allocator(idx_node)), numKeys_in_this_node};
        wks->first_leaf = link_to_this_node;

        for (;;) {
            for (unsigned idx_pair_in_this_node = 0; idx_pair < idx_pair_end_for_this_node; idx_pair++, idx_pair_in_this_node++) {
                if (idx_pair_cache == TREE_CONSTRUCT_NR_CACHED_KVPAIRS) {
                    mram_read((__mram_ptr KVPair*)(initial_pairs + sizeof(KVPair) * idx_pair), &wks->in.pairs[0],
                        sizeof(KVPair) * TREE_CONSTRUCT_NR_CACHED_KVPAIRS);
                    idx_pair_cache = 0;
                }
                node_cache->lf.keys[idx_pair_in_this_node] = wks->in.pairs[idx_pair_cache].key;
                NthValue(node_cache->lf, idx_pair_in_this_node) = wks->in.pairs[idx_pair_cache].value;
                idx_pair_cache++;
            }
            node_cache->lf.left = left_node;
#ifdef DEBUG_OCCUPANCY
            node_cache->lf.numKeys = numKeys_in_this_node;
#endif

            const key_uint64_t first_key = node_cache->lf.keys[0];
            wks->out.lifted[idx_lift_cache] = (LinkLift){{(uint32_t)(first_key >> 32), (uint32_t)first_key}, link_to_this_node};
            idx_lift_cache++;

            if (idx_lift_cache == TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT) {
                _Static_assert((TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                const uintptr_t off_for_alignment = (idx_lift_cache_begin % 2) * 4;
                const unsigned size = sizeof(LinkLift) * (idx_lift_cache - idx_lift_cache_begin) + off_for_alignment;
                mram_write((void*)((uintptr_t)(&wks->out.lifted[idx_lift_cache_begin]) - off_for_alignment),
                    (__mram_ptr void*)(lifted_links + sizeof(LinkLift) * (idx_node + 1 - idx_node_begin) - size),
                    size);
                idx_lift_cache_begin = idx_lift_cache = 0;
            }

            left_node = link_to_this_node.ptr;
            const unsigned numKeys_in_left_node = numKeys_in_this_node;

            idx_node++;
            if (idx_node == idx_node_end) {
                node_cache->lf.right = NODELINK_NULLPTR;
                wks->last_leaf = left_node;
                if (node_cache != root) {
                    store_leaf_filled(&node_cache->lf, &Deref(link_to_this_node.ptr).lf, numKeys_in_this_node);
                }
                break;
            }

            idx_pair_end_for_this_node = TREE_CONSTRUCT_idx_leaf_to_idx_pair(idx_node + 1, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
            numKeys_in_this_node = idx_pair_end_for_this_node - idx_pair;
            link_to_this_node = (NodeLink){allocator(idx_node), numKeys_in_this_node};

            node_cache->lf.right = link_to_this_node;
            store_leaf_filled(&node_cache->lf, &Deref(left_node).lf, numKeys_in_left_node);
        }
    }


    uint8_t tmp_height = 0;
    NodePtr nr_nodes_in_lower = nr_nodes;
    for (;; tmp_height++, nr_nodes_in_lower += nr_nodes) {
        _Static_assert(TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT >= MAX_NR_CHILDREN - 1, "TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT >= MAX_NR_CHILDREN - 1");
        wks->out.nr_cached_lift = idx_lift_cache;

        if (nr_nodes == 1) {
            if (me() < nr_leaves) {
                if (me() != 0) {
#ifdef DEBUG_OCCUPANCY
                    workspace.tree.init[me() - 1].numKeys_in_next_of_last_leaf = wks->first_leaf.numKeys;
#endif
                    mram_write(&workspace.tree.init[me() - 1].last_leaf, &Deref(wks->first_leaf.ptr).lf.left, 8);
                }
                if (me() + 1 != TREE_CONSTRUCT_NR_TASKLETS && me() + 1 < nr_leaves) {
                    mram_write(&workspace.tree.init[me() + 1].first_leaf, &Deref(wks->last_leaf).lf.right, 8);
                }
            }
            if (idx_node_begin != idx_node_end) {
                *height = tmp_height;
                *root_numKeys = wks->out.lifted[TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT - 1].child.numKeys;
#ifdef DEBUG_OCCUPANCY
                if (tmp_height == 0) {
                    root->lf.numKeys = *root_numKeys;
                } else {
                    root->inl.numKeys = *root_numKeys;
                }
#endif
            }
            return nr_nodes_in_lower;
        }

        const bool is_2nd_last_node_not_full = is_2nd_last_parent_not_full;
        const unsigned nr_children = nr_nodes,
                       idx_child_begin = idx_node_begin_used_by_me,
                       idx_child_end = idx_node_end_used_by_me,
                       idx_child_begin_from_me = idx_node_begin,
                       idx_child_end_from_me = idx_node_end;
        const bool is_any_child_sent_from_junior_to_senior = is_any_node_sent_from_junior_to_senior;

        nr_nodes = nr_parents;
        idx_node_begin = idx_parent_begin;
        idx_node_end = idx_parent_end;

        nr_parents = DIV_NR_NODES_BY_MAX_NR_CHILDREN(nr_nodes + MAX_NR_CHILDREN - 1);
        idx_parent_begin = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_begin, nr_nodes, nr_parents);
        idx_parent_end = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_end, nr_nodes, nr_parents);

        is_2nd_last_parent_not_full = nr_parents > 1u && MAX_NR_CHILDREN * (nr_parents - 1u) + MIN_NR_CHILDREN > nr_nodes;
        idx_node_begin_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_begin, nr_parents, is_2nd_last_parent_not_full, nr_nodes);
        idx_node_end_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_end, nr_parents, is_2nd_last_parent_not_full, nr_nodes);

        is_any_node_sent_from_junior_to_senior = idx_node_end_used_by_me < idx_node_begin;
        idx_lift_cache = 0;

        if (is_any_child_sent_from_junior_to_senior) {
            wait_for_prev_ready();
            notify_next_of_readiness();
            wait_for_next_ready();
            notify_prev_of_readiness();
            continue;

        } else {
            if (idx_child_end != idx_child_end_from_me) {
                notify_next_of_readiness();
            }

            if (nr_nodes == 1) {
                node_cache = root;
            }

            key_uint64_t key_min_subtree;

            const unsigned nr_children_received = idx_child_begin_from_me - idx_child_begin;
            if (nr_children_received != 0) {
                wait_for_prev_ready();
                TREE_CONSTRUCT_receive_lifted_links_from_junior(nr_children_received, &node_cache[0], &key_min_subtree);
                notify_prev_of_readiness();
            }
            if (idx_child_end != idx_child_end_from_me) {
                wait_for_next_ready();
            }

            if (idx_node_begin < idx_node_end) {
                unsigned idx_child = idx_child_begin_from_me;

                const unsigned idx_node_begin_sent_to_senior = (is_any_node_sent_from_junior_to_senior ? idx_node_begin : idx_node_end_used_by_me),
                               nr_nodes_not_sent = idx_node_begin_sent_to_senior - idx_node_begin;

                unsigned idx_child_cache = (lifted_links_offset != 0);
                // To place (idx_node_begin_sent_to_senior)-th node in wks->out.lifted[0], where should (idx_node_begin)-th be placed?
                //     -> wks->out.lifted[idx_lift_cache_begin]
                unsigned idx_lift_cache_begin = DIV_NR_NODES_BY_TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT(nr_nodes_not_sent + TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT - 1u)
                                                    * TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT
                                                - nr_nodes_not_sent;
                idx_lift_cache = idx_lift_cache_begin;

                const uintptr_t incoming_links = lifted_links;
                {  // Fetch the links to children before rewriting lifted_links_offset
                    _Static_assert((TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                    mram_read((__mram_ptr void*)(incoming_links - lifted_links_offset),
                        (void*)((uintptr_t)(&wks->in.lifted[idx_child_cache]) - lifted_links_offset),
                        sizeof(LinkLift) * (TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT - idx_child_cache) + lifted_links_offset);
                }
                lifted_links_offset = (idx_lift_cache_begin % 2) * 4;
                lifted_links = pairs_for_me + lifted_links_offset;

                unsigned idx_child_in_this_node = nr_children_received;

                for (unsigned idx_node = idx_node_begin; idx_node < idx_node_end; idx_node++) {
                    const unsigned idx_child_end_for_this_node = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_node + 1, nr_nodes, is_2nd_last_node_not_full, nr_children);

                    for (; idx_child < idx_child_end_for_this_node; idx_child++, idx_child_in_this_node++) {
                        if (idx_child_cache == TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT) {
                            _Static_assert((TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                            mram_read((__mram_ptr void*)(incoming_links + sizeof(LinkLift) * (idx_child - idx_child_begin_from_me)),
                                &wks->in.lifted[0],
                                sizeof(LinkLift) * TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT);
                            idx_child_cache = 0;
                        }
                        const LinkLift* const p_lift = &wks->in.lifted[idx_child_cache];
                        const key_uint64_t key = ((key_uint64_t)p_lift->key_parts[0] << 32) + p_lift->key_parts[1];
                        if (idx_child_in_this_node == 0) {
                            key_min_subtree = key;
                        } else {
                            node_cache->inl.keys[idx_child_in_this_node - 1] = key;
                        }
                        NthChild(node_cache->inl, idx_child_in_this_node) = p_lift->child;
                        idx_child_cache++;
                    }

                    const unsigned numKeys = idx_child_in_this_node - 1;
#ifdef DEBUG_OCCUPANCY
                    node_cache->inl.numKeys = numKeys;
#endif
                    const NodeLink link_to_this_node = {(nr_nodes == 1 ? NODE_NULLPTR : allocator(nr_nodes_in_lower + idx_node)), numKeys};
                    idx_child_in_this_node = 0;

                    wks->out.lifted[idx_lift_cache] = (LinkLift){{(uint32_t)(key_min_subtree >> 32), (uint32_t)key_min_subtree}, link_to_this_node};
                    idx_lift_cache++;
                    if (idx_lift_cache == TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT) {
                        _Static_assert((TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                        const uintptr_t off_for_alignment = (idx_lift_cache_begin % 2) * 4;
                        const unsigned size = sizeof(LinkLift) * (idx_lift_cache - idx_lift_cache_begin) + off_for_alignment;
                        mram_write((void*)((uintptr_t)(&wks->out.lifted[idx_lift_cache_begin]) - off_for_alignment),
                            (__mram_ptr void*)(lifted_links + sizeof(LinkLift) * (idx_node + 1 - idx_node_begin) - size),
                            size);
                        idx_lift_cache_begin = idx_lift_cache = 0;
                    }

                    if (node_cache == root) {
                        break;
                    }
                    store_internal_filled(&node_cache->inl, &Deref(link_to_this_node.ptr).inl, numKeys);
                }
            }
        }
    }
}


static void TREE_CONSTRUCT_barrier(void)
{
    if (me() != 0) {
        wait_for_prev_ready();
    }
    if (me() != TREE_CONSTRUCT_NR_TASKLETS - 1) {
        notify_next_of_readiness();
        wait_for_next_ready();
    }
    if (me() != 0) {
        notify_prev_of_readiness();
    }
}

OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static NodePtr INIT_cold_allocator(unsigned idx_node)
{
    return idx_node;
}
unsigned node_idx_shift;
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static NodePtr INIT_hot_allocator(unsigned idx_node)
{
    return idx_node + node_idx_shift;
}
OVERLAY_TASK_STATIC(OVL_SLOT_RESHARD, INIT_construct_phase, (void), ())
{
    _Static_assert(TREE_CONSTRUCT_NR_TASKLETS > 0, "TREE_CONSTRUCT_NR_TASKLETS > 0");
    if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
        static const uintptr_t cold_pairs = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

        const unsigned nr_cold_nodes = construct_tree(
            cold_pairs, input_header.init.nr_cold_pairs,
            &cold_root_numKeys, &cold_root, &cold_height, &cold_min_key, &nr_pairs.cold, INIT_cold_allocator);

        if (me() == 0) {
            node_idx_shift = nr_cold_nodes;
        }
        TREE_CONSTRUCT_barrier();

        const uintptr_t hot_pairs = cold_pairs + sizeof(KVPair) * input_header.init.nr_cold_pairs;
        const unsigned nr_hot_nodes = construct_tree(
            hot_pairs, input_header.init.nr_hot_pairs,
            &hot_root_numKeys, &hot_root, &hot_height, &hot_min_key, &nr_pairs.hot, INIT_hot_allocator);

        TREE_CONSTRUCT_barrier();

        _Static_assert(TASK_INIT_ALLOC_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS, "TASK_INIT_ALLOC_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS");
        Allocator_init(nr_cold_nodes + nr_hot_nodes);
    }
}
void task_init(void)
{
    INIT_construct_phase();
#ifdef TASK_INIT_CHECK
    CHECK_trees();
#endif
}


#if SUPPORT_INSERT
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_wait_for_all_prev(const unsigned nr_tasklets)
{
    if (me() != 0) {
        wait_for_prev_ready();
    }
    if (me() != nr_tasklets - 1) {
        notify_next_of_readiness();
    }
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_wait_for_all_next(const unsigned nr_tasklets)
{
    if (me() != nr_tasklets - 1) {
        wait_for_next_ready();
    }
    if (me() != 0) {
        notify_prev_of_readiness();
    }
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_sort_barrier(void)
{
    INSERT_wait_for_all_next(TASK_INSERT_SORT_NR_TASKLETS);
    INSERT_wait_for_all_prev(TASK_INSERT_SORT_NR_TASKLETS);
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_barrier(void)
{
    INSERT_wait_for_all_next(TASK_INSERT_NR_TASKLETS);
    INSERT_wait_for_all_prev(TASK_INSERT_NR_TASKLETS);
}

/* ---------------------------------------------------------------------- *
 *  Sorting a batch by key.  @sa /docs/parallel_batch_update.md
 * ---------------------------------------------------------------------- */

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned ISORT_digit(const key_uint64_t key, const unsigned shift)
{
    return (unsigned)(key >> shift) & (TASK_INSERT_SORT_NR_PIECES - 1);
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned ISORT_first_shift(const key_uint64_t min_key, const key_uint64_t max_key)
{
    const unsigned width = KEY_WIDTH - countl_zero_uint64(min_key ^ max_key);
    return (width > TASK_INSERT_SORT_RADIX_BITS ? width - TASK_INSERT_SORT_RADIX_BITS : 0);
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_min_max(key_uint64_t* const p_min, key_uint64_t* const p_max, KVPair (*const buf)[TASK_INSERT_SORT_RUN],
    __mram_ptr const KVPair* const qrys, const uint32_t begin, const uint32_t end)
{
    key_uint64_t min_key = KEY_MAX, max_key = KEY_MIN;

    for (uint32_t i = begin; i < end;) {
        __mram_ptr const KVPair* const to_read = qrys + i;

        uint32_t n = end - i;
        if (n >= TASK_INSERT_SORT_RUN) {
            n = TASK_INSERT_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * TASK_INSERT_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * n);
        }

        for (uint32_t j = 0; j < n; j++) {
            if ((*buf)[j].key < min_key) {
                min_key = (*buf)[j].key;
            }
            if ((*buf)[j].key > max_key) {
                max_key = (*buf)[j].key;
            }
        }
        i += n;
    }
    *p_min = min_key;
    *p_max = max_key;
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_count_digits(uint32_t (*const counts_me)[TASK_INSERT_SORT_NR_PIECES], KVPair (*const buf)[TASK_INSERT_SORT_RUN],
    __mram_ptr const KVPair* const src, const uint32_t begin, const uint32_t end, const unsigned shift)
{
    for (unsigned d = 0; d < TASK_INSERT_SORT_NR_PIECES; d++) {
        (*counts_me)[d] = 0;
    }
    for (uint32_t i = begin; i < end;) {
        __mram_ptr const KVPair* const to_read = src + i;

        uint32_t n = end - i;
        if (n >= TASK_INSERT_SORT_RUN) {
            n = TASK_INSERT_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * TASK_INSERT_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * n);
        }

        for (uint32_t j = 0; j < n; j++) {
            (*counts_me)[ISORT_digit((*buf)[j].key, shift)]++;
        }
        i += n;
    }
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_scatter_by_digit(uint32_t (*const offsets_me)[TASK_INSERT_SORT_NR_PIECES], KVPair (*const buf)[TASK_INSERT_SORT_RUN],
    KVPair (*const digit_buf)[TASK_INSERT_SORT_NR_PIECES][TASK_INSERT_SORT_DIGIT_BUF],
    uint8_t (*const nr_in_digit_buf)[TASK_INSERT_SORT_NR_PIECES],
    __mram_ptr const KVPair* const src, __mram_ptr KVPair* const dst,
    const uint32_t begin, const uint32_t end, const unsigned shift)
{
    for (unsigned d = 0; d < TASK_INSERT_SORT_NR_PIECES; d++) {
        (*nr_in_digit_buf)[d] = 0;
    }

    for (uint32_t i = begin; i < end;) {
        __mram_ptr const KVPair* const to_read = src + i;

        uint32_t n = end - i;
        if (n >= TASK_INSERT_SORT_RUN) {
            n = TASK_INSERT_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * TASK_INSERT_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * n);
        }

        for (uint32_t j = 0; j < n; j++) {
            const KVPair pair = (*buf)[j];

            const unsigned d = ISORT_digit(pair.key, shift);
            const unsigned nr_buffered = (*nr_in_digit_buf)[d];
            KVPair(*const out_buf)[TASK_INSERT_SORT_DIGIT_BUF] = &(*digit_buf)[d];

            if (nr_buffered >= TASK_INSERT_SORT_DIGIT_BUF) {
                mram_write(&(*out_buf)[0], dst + (*offsets_me)[d], sizeof(KVPair) * TASK_INSERT_SORT_DIGIT_BUF);
                (*offsets_me)[d] += TASK_INSERT_SORT_DIGIT_BUF;

                (*out_buf)[0] = pair;
                (*nr_in_digit_buf)[d] = 1;
            } else {
                (*out_buf)[nr_buffered] = pair;
                (*nr_in_digit_buf)[d] = (uint8_t)(nr_buffered + 1);
            }
        }
        i += n;
    }
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_flush_digit_buf(uint32_t (*const offsets_me)[TASK_INSERT_SORT_NR_PIECES],
    KVPair (*const digit_buf)[TASK_INSERT_SORT_NR_PIECES][TASK_INSERT_SORT_DIGIT_BUF],
    uint8_t (*const nr_in_digit_buf)[TASK_INSERT_SORT_NR_PIECES], __mram_ptr KVPair* const dst)
{
    for (unsigned d = 0; d < TASK_INSERT_SORT_NR_PIECES; d++) {
        const unsigned nr_buffered = (*nr_in_digit_buf)[d];
        if (nr_buffered != 0) {
            mram_write(&(*digit_buf)[d][0], dst + (*offsets_me)[d], sizeof(KVPair) * nr_buffered);
            (*offsets_me)[d] += nr_buffered;
        }
    }
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_sort_short_piece_impl(KVPair* const buf, const uint32_t n,
    __mram_ptr KVPair* const dst)
{
    for (uint32_t i = 1; i < n; i++) {
        const KVPair qry = buf[i];
        uint32_t j = i;
        if (buf[j - 1].key > qry.key) {
            do {
                buf[j] = buf[j - 1];
                j--;
            } while (j != 0 && buf[j - 1].key > qry.key);
            buf[j] = qry;
        }
    }

    mram_write(&buf[0], dst, sizeof(KVPair) * n);
}
//! @pre `[begin, end)` fits in `buf`: at most TASK_INSERT_SORT_RUN queries.
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_sort_short_piece(KVPair (*const buf)[TASK_INSERT_SORT_RUN], __mram_ptr const KVPair* const src,
    __mram_ptr KVPair* const dst, const uint32_t begin, const uint32_t end)
{
    const uint32_t n = end - begin;
    mram_read(src + begin, &(*buf)[0], sizeof(KVPair) * n);

    ISORT_sort_short_piece_impl(&(*buf)[0], n, dst + begin);
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_move_piece(KVPair (*const buf)[TASK_INSERT_SORT_RUN], __mram_ptr const KVPair* const src,
    __mram_ptr KVPair* const dst, const uint32_t begin, const uint32_t end)
{
    if (src == dst) {
        return;
    }
    for (uint32_t i = begin; i < end;) {
        __mram_ptr const KVPair* const to_read = src + i;
        __mram_ptr KVPair* const to_write = dst + i;

        uint32_t n = end - i;
        if (end - i >= TASK_INSERT_SORT_RUN) {
            n = TASK_INSERT_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * TASK_INSERT_SORT_RUN);
            mram_write(&(*buf)[0], to_write, sizeof(KVPair) * TASK_INSERT_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(KVPair) * n);
            mram_write(&(*buf)[0], to_write, sizeof(KVPair) * n);
        }

        i += n;
    }
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_plan_top_digit(const uint32_t nr_qrys)
{
    InsertSortTopDigit* const top_digit = &workspace.tree.insert.sort_top_digit;
    InsertSortWorkspace* const wks = &workspace.tree.insert.sort;
    uint32_t(*const piece_delims)[TASK_INSERT_SORT_NR_PIECES + 1] = &top_digit->piece_delims;

    for (unsigned d = me(); d < TASK_INSERT_SORT_NR_PIECES; d += TASK_INSERT_SORT_NR_TASKLETS) {
        uint32_t total = 0;
        for (unsigned t = 0; t < TASK_INSERT_SORT_NR_TASKLETS; t++) {
            total += wks->counts[t][d];
        }
        (*piece_delims)[d + 1] = total;
    }
    INSERT_wait_for_all_next(TASK_INSERT_SORT_NR_TASKLETS);

    if (me() == 0) {
        (*piece_delims)[0] = 0;
        for (unsigned d = 1; d <= TASK_INSERT_SORT_NR_PIECES; d++) {
            (*piece_delims)[d] += (*piece_delims)[d - 1];
        }

        uint32_t threshold = nr_qrys;
        unsigned d = 0;
        wks->top_digit_split[0] = 0;
        for (unsigned t = 1; t < TASK_INSERT_SORT_NR_TASKLETS; t++, threshold += nr_qrys) {
            while ((*piece_delims)[d + 1] * TASK_INSERT_SORT_NR_TASKLETS < threshold) {
                d++;
            }
            wks->top_digit_split[t] = (uint16_t)d;
        }
        wks->top_digit_split[TASK_INSERT_SORT_NR_TASKLETS] = TASK_INSERT_SORT_NR_PIECES;
    }
    INSERT_wait_for_all_prev(TASK_INSERT_SORT_NR_TASKLETS);

    for (unsigned d = me(); d < TASK_INSERT_SORT_NR_PIECES; d += TASK_INSERT_SORT_NR_TASKLETS) {
        uint32_t acc = (*piece_delims)[d];
        for (unsigned t = 0; t < TASK_INSERT_SORT_NR_TASKLETS; t++) {
            const uint32_t c = wks->counts[t][d];
            wks->counts[t][d] = acc;
            acc += c;
        }
    }
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_finish_pieces(__mram_ptr KVPair* const qrys, __mram_ptr KVPair* const scratch,
    __mram_ptr InsertSortStackEntry (*const stack)[TASK_INSERT_SORT_STACK_CAPACITY],
    const unsigned first_shift)
{
    InsertSortTopDigit* const top_digit = &workspace.tree.insert.sort_top_digit;
    InsertSortWorkspace* const wks = &workspace.tree.insert.sort;
    KVPair(*const buf)[TASK_INSERT_SORT_RUN] = &wks->buf[me()];
    uint32_t(*const piece_delims)[TASK_INSERT_SORT_NR_PIECES + 1] = &top_digit->piece_delims;
    uint32_t(*const counts_me)[TASK_INSERT_SORT_NR_PIECES] = &wks->counts[me()];

    uint32_t nr_stacked = 0;

    for (unsigned d = wks->top_digit_split[me()]; d < wks->top_digit_split[me() + 1]; d++) {
        if ((*piece_delims)[d] != (*piece_delims)[d + 1]) {
            InsertSortStackEntry entry;
            entry.begin = (*piece_delims)[d];
            entry.end = (*piece_delims)[d + 1];
            entry.prev_shift = first_shift;
            entry.src = scratch;
            mram_write(&entry, &(*stack)[nr_stacked++], sizeof(InsertSortStackEntry));
        }
    }

    while (nr_stacked != 0) {
        InsertSortStackEntry entry;
        mram_read(&(*stack)[--nr_stacked], &entry, sizeof(InsertSortStackEntry));
        const uint32_t begin = entry.begin, end = entry.end, prev_shift = entry.prev_shift;
        __mram_ptr KVPair* const src = entry.src;
        __mram_ptr KVPair* const dst = (src == qrys ? scratch : qrys);

        if (prev_shift == 0) {
            ISORT_move_piece(buf, src, qrys, begin, end);
            continue;
        }
        if (end - begin <= TASK_INSERT_SORT_RUN) {
            ISORT_sort_short_piece(buf, src, qrys, begin, end);
            continue;
        }
        uint32_t shift = prev_shift >= TASK_INSERT_SORT_RADIX_BITS ? prev_shift - TASK_INSERT_SORT_RADIX_BITS : 0;

        ISORT_count_digits(counts_me, buf, src, begin, end, shift);
        uint32_t acc = begin;
        for (unsigned d = 0; d < TASK_INSERT_SORT_NR_PIECES; d++) {
            const uint32_t c = (*counts_me)[d];
            (*counts_me)[d] = acc;
            acc += c;
        }
        KVPair(*const digit_buf)[TASK_INSERT_SORT_NR_PIECES][TASK_INSERT_SORT_DIGIT_BUF] = &wks->digit_buf[me()];
        uint8_t(*const nr_in_digit_buf)[TASK_INSERT_SORT_NR_PIECES] = &wks->nr_in_digit_buf[me()];
        ISORT_scatter_by_digit(counts_me, buf, digit_buf, nr_in_digit_buf,
            src, dst, begin, end, shift);

        uint32_t piece_begin = begin;
        for (unsigned d = 0; d < TASK_INSERT_SORT_NR_PIECES; d++) {
            KVPair(*const out_buf)[TASK_INSERT_SORT_DIGIT_BUF] = &(*digit_buf)[d];
            const unsigned nr_buffered = (*nr_in_digit_buf)[d];

            const uint32_t offset = (*counts_me)[d];
            const uint32_t piece_end = offset + nr_buffered;

            if (offset == piece_begin) {
                if (nr_buffered == 0) {
                    continue;
                } else if (nr_buffered > TASK_INSERT_SORT_DIGIT_BUF) {
                    __builtin_unreachable();
                } else {
                    ISORT_sort_short_piece_impl(&(*out_buf)[0], nr_buffered, qrys + offset);
                }
            } else {
                if (nr_buffered != 0) {
                    mram_write(&(*out_buf)[0], dst + offset, sizeof(KVPair) * nr_buffered);
                }

                InsertSortStackEntry entry;
                entry.begin = piece_begin;
                entry.end = piece_end;
                entry.prev_shift = shift;
                entry.src = dst;
                mram_write(&entry, &(*stack)[nr_stacked++], sizeof(InsertSortStackEntry));
            }
            piece_begin = piece_end;
        }
    }
}

#ifdef TASK_INSERT_SORT_CHECK
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static bool ISORT_check_sorted(__mram_ptr const KVPair* const qrys, const uint32_t nr_qrys)
{
    KVPair(*const buf)[TASK_INSERT_SORT_RUN] = &workspace.tree.insert.sort.buf[me()];
    bool success = true;
    key_uint64_t previous = KEY_MIN;

    for (uint32_t i = 0; i < nr_qrys;) {
        const uint32_t n = (nr_qrys - i < TASK_INSERT_SORT_RUN ? nr_qrys - i : TASK_INSERT_SORT_RUN);
        mram_read(qrys + i, (*buf), sizeof(KVPair) * n);
        for (uint32_t j = 0; j < n; j++) {
            if ((*buf)[j].key < previous) {
                success = false;
                printf("qrys[%u].key == %lu < %lu == qrys[%u].key\n", i + j, (*buf)[j].key, previous, i + j - 1);
            }
            previous = (*buf)[j].key;
        }
        i += n;
    }
    return success;
}
#endif

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void ISORT_execute_in_parallel(__mram_ptr KVPair* const qrys, const uint32_t nr_qrys,
    __mram_ptr KVPair* const scratch)
{
    InsertSortTopDigit* const top_digit = &workspace.tree.insert.sort_top_digit;
    InsertSortWorkspace* const wks = &workspace.tree.insert.sort;
    KVPair(*const buf)[TASK_INSERT_SORT_RUN] = &wks->buf[me()];

    if (nr_qrys <= 1) {
        return;
    }
    if (nr_qrys <= TASK_INSERT_SORT_RUN) {
        if (me() == 0) {
            ISORT_sort_short_piece(buf, qrys, qrys, 0, nr_qrys);
        }
        INSERT_sort_barrier();
        return;
    }

    const uint32_t nr_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_INSERT_SORT_NR_TASKLETS(nr_qrys),
                   nr_remainder_qrys = nr_qrys - nr_qrys_per_tasklet * TASK_INSERT_SORT_NR_TASKLETS,
                   nr_qrys_for_me = nr_qrys_per_tasklet + (me() < nr_remainder_qrys);
    const uint32_t my_qry_begin = nr_qrys_per_tasklet * me() + (me() < nr_remainder_qrys ? me() : nr_remainder_qrys),
                   my_qry_end = my_qry_begin + nr_qrys_for_me;

    ISORT_min_max(&wks->qry_min_key[me()], &wks->qry_max_key[me()], buf, qrys, my_qry_begin, my_qry_end);
    INSERT_wait_for_all_next(TASK_INSERT_SORT_NR_TASKLETS);

    if (me() == 0) {
        key_uint64_t min_key = KEY_MAX, max_key = KEY_MIN;
        for (unsigned t = 0; t < TASK_INSERT_SORT_NR_TASKLETS; t++) {
            if (wks->qry_min_key[t] < min_key) {
                min_key = wks->qry_min_key[t];
            }
            if (wks->qry_max_key[t] > max_key) {
                max_key = wks->qry_max_key[t];
            }
        }
        top_digit->qry_min_key = min_key;
        top_digit->qry_max_key = max_key;
        top_digit->shift = ISORT_first_shift(min_key, max_key);
    }
    INSERT_wait_for_all_prev(TASK_INSERT_SORT_NR_TASKLETS);
    const unsigned first_shift = top_digit->shift;

    uint32_t(*const counts_me)[TASK_INSERT_SORT_NR_PIECES] = &wks->counts[me()];
    ISORT_count_digits(counts_me, buf, qrys, my_qry_begin, my_qry_end, first_shift);
    INSERT_sort_barrier();

    ISORT_plan_top_digit(nr_qrys);
    INSERT_sort_barrier();

    KVPair(*const digit_buf)[TASK_INSERT_SORT_NR_PIECES][TASK_INSERT_SORT_DIGIT_BUF] = &wks->digit_buf[me()];
    uint8_t(*const nr_in_digit_buf)[TASK_INSERT_SORT_NR_PIECES] = &wks->nr_in_digit_buf[me()];

    ISORT_scatter_by_digit(counts_me, buf, digit_buf, nr_in_digit_buf,
        qrys, scratch, my_qry_begin, my_qry_end, first_shift);
    ISORT_flush_digit_buf(counts_me, digit_buf, nr_in_digit_buf, scratch);
    INSERT_sort_barrier();

    ISORT_finish_pieces(qrys, scratch, (__mram_ptr InsertSortStackEntry(*)[TASK_INSERT_SORT_STACK_CAPACITY])(scratch + nr_qrys) + me(), first_shift);
    INSERT_sort_barrier();
}
#endif /* SUPPORT_INSERT */


OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_write_child_link(const NodePtr ptr, const uint16_t j, const NodeLink link)
{
    __dma_aligned NodeLink link_pair[2];
    mram_read(&NthChild(Deref(ptr).inl, j / 2 * 2 + 1), &link_pair[0], sizeof(NodeLink) * 2);
    link_pair[1 - j % 2] = link;
    mram_write(&link_pair[0], &NthChild(Deref(ptr).inl, j / 2 * 2 + 1), sizeof(NodeLink) * 2);
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_store_child_link(InternalNode* const node, const NodePtr ptr, const uint16_t j, const NodeLink link)
{
    NthChild(*node, j) = link;
    mram_write(&NthChild(*node, j / 2 * 2 + 1), &NthChild(Deref(ptr).inl, j / 2 * 2 + 1), sizeof(NodeLink) * 2);
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_write_left_of_leaf(const NodeLink leaf, const NodePtr left)
{
    struct {
        __dma_aligned NodePtr left;
#ifdef DEBUG_OCCUPANCY
        unsigned numKeys;
#endif
    } leaf_left;
    leaf_left.left = left;
#ifdef DEBUG_OCCUPANCY
    leaf_left.numKeys = leaf.numKeys;
#endif
    mram_write(&leaf_left, &Deref(leaf.ptr).lf.left, 8);
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_write_right_of_leaf(const NodePtr leaf, const NodeLink right)
{
    __dma_aligned NodeLink link_pair[2];
    link_pair[0] = right;
    mram_write(&link_pair[0], &Deref(leaf).lf.right, 8);
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static NodeLink INSERT_combined_child(const InternalNode* const node, const unsigned p, const NodeLink child, const unsigned i)
{
    return (i < p ? NthChild(*node, i) : i == p ? child
                                                : NthChild(*node, i - 1));
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static key_uint64_t INSERT_combined_key(const InternalNode* const node, const unsigned q, const key_uint64_t key, const unsigned i)
{
    return (i < q ? node->keys[i] : i == q ? key
                                           : node->keys[i - 1]);
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_leave_leaf(TaskletLocalInsertWorkspace* const restrict wks, NodeLink* const restrict p_root, const unsigned height)
{
    if (!wks->leaf_dirty) {
        return;
    }
    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];

    const Node* const leaf = &(*node_cache)[0];
    const NodeLink link = wks->path[0].link;

    store_leaf_filled(&leaf->lf, &Deref(link.ptr).lf, link.numKeys);
    if (height == 0) {
        *p_root = link;
    } else if (wks->parent_loaded) {
        INSERT_store_child_link(&(*node_cache)[1].inl, wks->path[1].link.ptr, wks->path[0].idx_in_parent, link);
    } else {
        INSERT_write_child_link(wks->path[1].link.ptr, wks->path[0].idx_in_parent, link);
    }
    if (leaf->lf.left != NODE_NULLPTR) {
        INSERT_write_right_of_leaf(leaf->lf.left, link);
    }
    wks->leaf_dirty = false;
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_descend(TaskletLocalInsertWorkspace* const restrict wks, NodeLink* const restrict p_root, const unsigned height, const key_uint64_t key)
{
    INSERT_leave_leaf(wks, p_root, height);

    unsigned d = 0;
    if (key > wks->path[0].max_key) {
        d = height;
        for (unsigned i = 1; i < height; i++) {
            if (key <= wks->path[i].max_key) {
                d = i;
                break;
            }
        }
    }

    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];

    Node* const node = &(*node_cache)[1];
    for (; d > 0; d--) {
        const NodeLink link = wks->path[d].link;
        fetch_internal_filled(&Deref(link.ptr).inl, &node->inl, link.numKeys);
        wks->parent_loaded = (d == 1);

        const uint16_t idx = search_for_child_index(&node->inl.keys[0], (uint8_t)link.numKeys, key);
        wks->path[d - 1].link = NthChild(node->inl, idx);
        wks->path[d - 1].idx_in_parent = idx;
        wks->path[d - 1].max_key = (idx == link.numKeys ? wks->path[d].max_key : node->inl.keys[idx] - 1);
    }

    fetch_leaf_filled(&Deref(wks->path[0].link.ptr).lf, &(*node_cache)[0].lf, wks->path[0].link.numKeys);
    wks->leaf_loaded = true;
}

//! @brief Puts `right` into the tree next to `left`, the child at index `idx`
//! of the node at level `d` of the path that has just been split in two.  `key`
//! is the delimiter between them and `follow_right` says which of the two the
//! path below goes through.
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_insert_child(TaskletLocalInsertWorkspace* const wks, NodeLink* const p_root, unsigned* const p_height,
    unsigned d, unsigned idx, NodeLink left, NodeLink right, key_uint64_t key, bool follow_right)
{
    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];
    Node* const node = &(*node_cache)[1];
    Node* const sibling = &(*node_cache)[2];

    for (;;) {
        if (d > *p_height) {
            node->inl.keys[0] = key;
            NthChild(node->inl, 0) = left;
            NthChild(node->inl, 1) = right;
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = 1;
#endif
            const NodePtr ptr = Allocate_node();
            store_internal_filled(&node->inl, &Deref(ptr).inl, 1);

            wks->path[d].link = (NodeLink){ptr, 1};
            wks->path[d].max_key = KEY_MAX;
            wks->path[d].idx_in_parent = 0;
            wks->path[d - 1].idx_in_parent = (follow_right ? 1 : 0);
            *p_root = wks->path[d].link;
            *p_height = (uint8_t)d;
            wks->parent_loaded = (d == 1);
            return;
        }

        const unsigned n = (unsigned)wks->path[d].link.numKeys + 1;
        if (d != 1 || !wks->parent_loaded) {
            fetch_internal_filled(&Deref(wks->path[d].link.ptr).inl, &node->inl, wks->path[d].link.numKeys);
        }
        // The child that split is still linked with the keys it had before.
        NthChild(node->inl, idx) = left;

        if (n < MAX_NR_CHILDREN) {
            for (unsigned i = n - 1; i-- > idx;) {
                NthChild(node->inl, i + 2) = NthChild(node->inl, i + 1);
                node->inl.keys[i + 1] = node->inl.keys[i];
            }
            NthChild(node->inl, idx + 1u) = right;
            node->inl.keys[idx] = key;
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = n;
#endif
            store_internal_filled(&node->inl, &Deref(wks->path[d].link.ptr).inl, n);

            wks->path[d].link.numKeys = n;
            wks->path[d - 1].idx_in_parent = (uint16_t)(idx + follow_right);
            if (d == *p_height) {
                *p_root = wks->path[d].link;
            } else {
                INSERT_write_child_link(wks->path[d + 1].link.ptr, wks->path[d].idx_in_parent, wks->path[d].link);
            }
            wks->parent_loaded = (d == 1);
            return;
        }

        _Static_assert(MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN, "MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN");
        const unsigned nr_left = MAX_NR_CHILDREN - MIN_NR_CHILDREN;
        const unsigned p = idx + 1u, q = idx;
        const key_uint64_t promoted = INSERT_combined_key(&node->inl, q, key, nr_left - 1);

        for (unsigned i = nr_left; i <= MAX_NR_CHILDREN; i++) {
            NthChild(sibling->inl, i - nr_left) = INSERT_combined_child(&node->inl, p, right, i);
            if (i != nr_left) {
                sibling->inl.keys[i - nr_left - 1] = INSERT_combined_key(&node->inl, q, key, i - 1);
            }
        }
        for (unsigned i = nr_left; i-- > 0;) {
            if (i != 0) {
                node->inl.keys[i - 1] = INSERT_combined_key(&node->inl, q, key, i - 1);
            }
            NthChild(node->inl, i) = INSERT_combined_child(&node->inl, p, right, i);
        }
#ifdef DEBUG_OCCUPANCY
        node->inl.numKeys = nr_left - 1;
        sibling->inl.numKeys = MAX_NR_CHILDREN - nr_left;
#endif
        const NodePtr sibling_ptr = Allocate_node();
        store_internal_filled(&node->inl, &Deref(wks->path[d].link.ptr).inl, nr_left - 1);
        store_internal_filled(&sibling->inl, &Deref(sibling_ptr).inl, MAX_NR_CHILDREN - nr_left);

        const NodeLink new_left = {wks->path[d].link.ptr, nr_left - 1},
                       new_right = {sibling_ptr, MAX_NR_CHILDREN - nr_left};
        const unsigned pos = idx + follow_right;
        const uint16_t idx_of_node = wks->path[d].idx_in_parent;
        follow_right = (pos >= nr_left);
        if (follow_right) {
            wks->path[d].link = new_right;
            wks->path[d - 1].idx_in_parent = (uint16_t)(pos - nr_left);
        } else {
            wks->path[d].max_key = promoted - 1;
            wks->path[d].link = new_left;
            wks->path[d - 1].idx_in_parent = (uint16_t)pos;
        }

        d++;
        idx = idx_of_node;
        left = new_left;
        right = new_right;
        key = promoted;
    }
}

//! @return Whether the number of live pairs grew.
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static bool INSERT_execute(TaskletLocalInsertWorkspace* const restrict wks, NodeLink* const restrict p_root, unsigned* const restrict p_height, const KVPair* const restrict qry)
{
    if (qry->key > wks->path[0].max_key || !wks->leaf_loaded) {
        INSERT_descend(wks, p_root, *p_height, qry->key);
    }

    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];
    Node* const leaf = &(*node_cache)[0];
    const unsigned nr_pairs_in_leaf = wks->path[0].link.numKeys;
    const uint16_t idx_pair = search_for_pair_index(&leaf->lf.keys[0], (uint8_t)nr_pairs_in_leaf, qry->key);

    if (idx_pair < nr_pairs_in_leaf && leaf->lf.keys[idx_pair] == qry->key) {
        NthValue(leaf->lf, idx_pair) = qry->value;
        wks->leaf_dirty = true;
        return false;
    }

    if (nr_pairs_in_leaf < MAX_NR_PAIRS) {
        for (unsigned i = nr_pairs_in_leaf; i > idx_pair; i--) {
            leaf->lf.keys[i] = leaf->lf.keys[i - 1];
            NthValue(leaf->lf, i) = NthValue(leaf->lf, i - 1);
        }
        leaf->lf.keys[idx_pair] = qry->key;
        NthValue(leaf->lf, idx_pair) = qry->value;
#ifdef DEBUG_OCCUPANCY
        leaf->lf.numKeys = nr_pairs_in_leaf + 1;
#endif
        wks->path[0].link.numKeys = nr_pairs_in_leaf + 1;
        wks->leaf_dirty = true;
        return true;
    }

    Node* const sibling = &(*node_cache)[2];
    const NodePtr sibling_ptr = Allocate_node();
    const unsigned nr_left = MAX_NR_PAIRS - MIN_NR_PAIRS + 1, nr_right = MIN_NR_PAIRS;
    const NodeLink left_link = {wks->path[0].link.ptr, nr_left}, right_link = {sibling_ptr, nr_right};
    const key_uint64_t mid = (idx_pair == nr_left  ? qry->key
                              : idx_pair < nr_left ? leaf->lf.keys[nr_left - 1]
                                                   : leaf->lf.keys[nr_left]);

    sibling->lf.right = leaf->lf.right;
    leaf->lf.right = right_link;
    sibling->lf.left = left_link.ptr;
#ifdef DEBUG_OCCUPANCY
    leaf->lf.numKeys = nr_left;
    sibling->lf.numKeys = nr_right;
#endif
    if (idx_pair <= nr_left - 1) {
        for (unsigned i = 0; i < nr_right; i++) {
            sibling->lf.keys[i] = leaf->lf.keys[i + nr_left - 1];
            NthValue(sibling->lf, i) = NthValue(leaf->lf, i + nr_left - 1);
        }
        for (unsigned i = nr_left - 1; i > idx_pair; i--) {
            leaf->lf.keys[i] = leaf->lf.keys[i - 1];
            NthValue(leaf->lf, i) = NthValue(leaf->lf, i - 1);
        }
        leaf->lf.keys[idx_pair] = qry->key;
        NthValue(leaf->lf, idx_pair) = qry->value;
    } else {
        for (unsigned i = nr_left; i < idx_pair; i++) {
            sibling->lf.keys[i - nr_left] = leaf->lf.keys[i];
            NthValue(sibling->lf, i - nr_left) = NthValue(leaf->lf, i);
        }
        sibling->lf.keys[idx_pair - nr_left] = qry->key;
        NthValue(sibling->lf, idx_pair - nr_left) = qry->value;
        for (unsigned i = idx_pair; i < MAX_NR_PAIRS; i++) {
            sibling->lf.keys[i - (nr_left - 1)] = leaf->lf.keys[i];
            NthValue(sibling->lf, i - (nr_left - 1)) = NthValue(leaf->lf, i);
        }
    }
    if (leaf->lf.left != NODE_NULLPTR) {
        INSERT_write_right_of_leaf(leaf->lf.left, left_link);
    }
    if (!(sibling->lf.right.ptr == NODELINK_NULLPTR.ptr && sibling->lf.right.numKeys == NODELINK_NULLPTR.numKeys)) {
        INSERT_write_left_of_leaf(sibling->lf.right, sibling_ptr);
    }
    store_leaf_filled(&leaf->lf, &Deref(left_link.ptr).lf, nr_left);
    store_leaf_filled(&sibling->lf, &Deref(sibling_ptr).lf, nr_right);
    wks->leaf_dirty = false;

    const bool follow_left = (idx_pair < nr_left);
    wks->leaf_loaded = follow_left;
    if (follow_left) {
        wks->path[0].max_key = mid - 1;
        wks->path[0].link = left_link;
    } else {
        wks->path[0].link = right_link;
    }
    INSERT_insert_child(wks, p_root, p_height, 1, wks->path[0].idx_in_parent,
        left_link, right_link, mid, !follow_left);

    return true;
}

//! @pre The batch is in key order.
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_execute_batch(TaskletLocalInsertWorkspace* const restrict wks, NodeLink* const restrict p_root, unsigned* const restrict p_height,
    uint32_t* const restrict p_nr_new_pairs, __mram_ptr KVPair* const restrict qrys, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    __mram_ptr KVPair* cursor = qrys + idx_qry_begin;

    uint32_t nr_new_pairs = 0;
    for (uint32_t nr_left = idx_qry_end - idx_qry_begin; nr_left != 0;) {
        uint32_t n = TASK_INSERT_NR_CACHED_QRYS;
        if (nr_left >= TASK_INSERT_NR_CACHED_QRYS) {
            mram_read(cursor, wks->qrys, sizeof(KVPair) * TASK_INSERT_NR_CACHED_QRYS);
        } else {
            n = nr_left;
            mram_read(cursor, wks->qrys, sizeof(KVPair) * n);
        }
        cursor += n;

        for (uint32_t i = 0; i < n; i++) {
            nr_new_pairs += INSERT_execute(wks, p_root, p_height, &wks->qrys[i]);
        }
        nr_left -= n;
    }
    INSERT_leave_leaf(wks, p_root, *p_height);

    *p_nr_new_pairs = nr_new_pairs;
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_execute_batch_wram_root(TaskletLocalInsertWorkspace* const wks, Node* const p_root, uint8_t* const p_height,
    uint8_t* const p_root_numKeys, uint32_t* const p_nr_pairs,
    __mram_ptr KVPair* const qrys, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    unsigned height = *p_height;

    const NodePtr ptr = Allocate_node();
    NodeLink link = {ptr, *p_root_numKeys};

    wks->path[height].link = link;
    wks->path[height].max_key = KEY_MAX;

    if (height == 0) {
        store_leaf_filled(&p_root->lf, &Deref(ptr).lf, *p_root_numKeys);
    } else {
        store_internal_filled(&p_root->inl, &Deref(ptr).inl, *p_root_numKeys);

        NodeLink first_child = NthChild(p_root->inl, 0);
        key_uint64_t max_key_of_first_child = p_root->inl.keys[0] - 1;
        for (unsigned h = height - 1;; h--) {
            wks->path[h].link = first_child;
            wks->path[h].idx_in_parent = 0;
            wks->path[h].max_key = max_key_of_first_child;

            if (h == 0) {
                break;
            }

            fetch_internal_filled(&Deref(first_child.ptr).inl, &p_root->inl, 1);
            first_child = NthChild(p_root->inl, 0);
            max_key_of_first_child = p_root->inl.keys[0] - 1;
        }
    }
    wks->parent_loaded = false;
    wks->leaf_loaded = false;
    wks->leaf_dirty = false;

    uint32_t nr_new_pairs = 0;
    INSERT_execute_batch(wks, &link, &height, &nr_new_pairs, qrys, idx_qry_begin, idx_qry_end);
    *p_nr_pairs += nr_new_pairs;

    if (height == 0) {
        fetch_leaf_filled(&Deref(link.ptr).lf, &p_root->lf, link.numKeys);
    } else {
        fetch_internal_filled(&Deref(link.ptr).inl, &p_root->inl, link.numKeys);
    }
    *p_height = (uint8_t)height;
    *p_root_numKeys = (uint8_t)link.numKeys;
    Free_node(link.ptr);
}

#if SUPPORT_INSERT
/* ---------------------------------------------------------------------- *
 *  Splitting a batch among the tasklets by key range.
 *  @sa /docs/insert_handout.md
 * ---------------------------------------------------------------------- */

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static uint32_t INSERT_lower_bound(__mram_ptr const KVPair* const qrys, uint32_t begin, uint32_t end,
    const key_uint64_t key)
{
    while (begin < end) {
        const uint32_t mid = (begin + end) / 2;
        __dma_aligned key_uint64_t probe;
        mram_read(&qrys[mid].key, &probe, sizeof(key_uint64_t));
        if (probe < key) {
            begin = mid + 1;
        } else {
            end = mid;
        }
    }
    return begin;
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned INSERT_find_bucket_begins(__mram_ptr const KVPair* const qrys, uint32_t begin, uint32_t end,
    const key_uint64_t (*const keys)[MAX_NR_CHILDREN - 1], const unsigned nr_keys,
    uint32_t (*const restrict out)[MAX_NR_CHILDREN - 1], const unsigned idx_key_offset)
{
    const InsertSortTopDigit* const top_digit = &workspace.tree.insert.sort_top_digit;
    const key_uint64_t qry_min_key = top_digit->qry_min_key, qry_max_key = top_digit->qry_max_key;
    const uint32_t shift = top_digit->shift;
    const uint32_t(*const piece_delims)[TASK_INSERT_SORT_NR_PIECES + 1] = &top_digit->piece_delims;

    unsigned idx_key = idx_key_offset;
    for (; idx_key < nr_keys; idx_key += TASK_INSERT_NR_TASKLETS) {
        const key_uint64_t key = (*keys)[idx_key];

        uint32_t floor_pos;
        if (key <= qry_min_key) {
            floor_pos = begin;
        } else if (key > qry_max_key) {
            floor_pos = end;
        } else {
            const unsigned digit = ISORT_digit(key, shift);
            const uint32_t piece_begin = (*piece_delims)[digit], piece_end = (*piece_delims)[digit + 1];
            if (begin < piece_begin) {
                begin = piece_begin;
            }
            const uint32_t tmp_end = piece_end < end ? piece_end : end;
            floor_pos = INSERT_lower_bound(qrys, begin, tmp_end, key);
        }

        begin = floor_pos;
        (*out)[idx_key] = floor_pos;
    }
    return idx_key - nr_keys;
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_nr_backet_qrys(const uint32_t (*const backet_ends)[MAX_NR_CHILDREN - 1], const unsigned nr_keys,
    uint32_t (*const nr_qrys)[MAX_NR_CHILDREN], const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    uint32_t prev_backet_end = (*backet_ends)[0];
    (*nr_qrys)[0] = prev_backet_end - idx_qry_begin;
    for (unsigned idx_child = 1; idx_child < nr_keys; idx_child++) {
        const uint32_t backet_end = (*backet_ends)[idx_child];
        (*nr_qrys)[idx_child] = backet_end - prev_backet_end;
        prev_backet_end = backet_end;
    }
    (*nr_qrys)[nr_keys] = idx_qry_end - prev_backet_end;
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned INSERT_nr_sharers(const uint32_t total, const uint32_t limit)
{
    unsigned k = 1;
    for (uint32_t covered = limit; covered < total; covered += limit) {
        k++;
    }
    return k;
}
//! @return `need(limit)` (/docs/insert_handout.md §3).
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned INSERT_nr_tasklets_needed(const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN], const unsigned nr_children,
    const uint32_t nqrys_limit, const bool is_child_leaf)
{
    const uint32_t* nr_qrys_cursor = &(*nr_qrys)[0];
    const uint32_t* const end_nr_qrys = nr_qrys_cursor + nr_children;
    unsigned result = 0;

    uint32_t next_nr_qrys = *nr_qrys_cursor;
    for (;;) {
        nr_qrys_cursor++;

        if (next_nr_qrys > nqrys_limit) {
            if (is_child_leaf) {
                return UINT_MAX;
            }
            result += INSERT_nr_sharers(next_nr_qrys, nqrys_limit);

            if (nr_qrys_cursor >= end_nr_qrys) {
                return result;
            }
            next_nr_qrys = *nr_qrys_cursor;

        } else {
            result++;

            uint32_t sum_nr_qrys = next_nr_qrys;
            for (;;) {
                if (nr_qrys_cursor >= end_nr_qrys) {
                    return result;
                } else {
                    next_nr_qrys = *nr_qrys_cursor;

                    sum_nr_qrys += next_nr_qrys;
                    if (sum_nr_qrys <= nqrys_limit) {
                        nr_qrys_cursor++;
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned INSERT_min_max_nqrys_per_tasklet(const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN],
    const unsigned nr_children, const uint32_t total_nr_qrys,
    const unsigned nr_tasklets, const bool is_child_leaf,
    unsigned* const restrict p_nr_assigned_tasklets)
{
    uint32_t lo = (total_nr_qrys + nr_tasklets - 1) / nr_tasklets, hi = total_nr_qrys;
    unsigned nr_tasklets_at_hi = 1;

    while (lo < hi) {
        const uint32_t mid = (lo + hi) / 2;
        const unsigned nr_tasklets_at_mid = INSERT_nr_tasklets_needed(nr_qrys, nr_children, mid, is_child_leaf);
        if (nr_tasklets_at_mid <= nr_tasklets) {
            hi = mid;
            nr_tasklets_at_hi = nr_tasklets_at_mid;
        } else {
            lo = mid + 1;
        }
    }
    *p_nr_assigned_tasklets = nr_tasklets_at_hi;
    return hi;
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_tasklet_assignments(const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN], const unsigned nr_children,
    const uint32_t nqrys_limit,
    TaskletAssignmentToChildren (*const assignments)[TASK_INSERT_NR_TASKLETS], unsigned* const restrict p_nr_assignments,
    uint8_t (*const forks)[TASK_INSERT_MAX_NR_FORK], unsigned* const restrict p_nr_forks)
{
    unsigned idx_child = 0, nr_assignments = 0, nr_forks = 0;

    uint32_t next_nr_qrys = (*nr_qrys)[idx_child];
    for (;;) {
        idx_child++;

        if (next_nr_qrys > nqrys_limit) {
            (*forks)[nr_forks] = nr_assignments;
            nr_forks++;

            TaskletAssignmentToChildren assignment;
            assignment.end_child = idx_child;
            assignment.nr_tasklets = INSERT_nr_sharers(next_nr_qrys, nqrys_limit);
            (*assignments)[nr_assignments] = assignment;
            nr_assignments++;

            if (idx_child >= nr_children) {
                goto end_of_func;
            }
            next_nr_qrys = (*nr_qrys)[idx_child];

        } else {
            TaskletAssignmentToChildren assignment;
            assignment.nr_tasklets = 1;

            uint32_t sum_nr_qrys = next_nr_qrys;
            for (;;) {
                if (idx_child >= nr_children) {
                    assignment.end_child = idx_child;
                    (*assignments)[nr_assignments] = assignment;
                    nr_assignments++;

                    goto end_of_func;

                } else {
                    next_nr_qrys = (*nr_qrys)[idx_child];

                    sum_nr_qrys += next_nr_qrys;
                    if (sum_nr_qrys <= nqrys_limit) {
                        idx_child++;
                    } else {
                        assignment.end_child = idx_child;
                        (*assignments)[nr_assignments] = assignment;
                        nr_assignments++;
                        break;
                    }
                }
            }
        }
    }

end_of_func:
    *p_nr_assignments = nr_assignments;
    *p_nr_forks = nr_forks;
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_assign_leftover_tasklets(unsigned nr_leftover_tasklets, const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN],
    TaskletAssignmentToChildren (*const assignments)[TASK_INSERT_NR_TASKLETS],
    const uint8_t (*const forks)[TASK_INSERT_MAX_NR_FORK], const unsigned nr_forks)
{
    if (nr_forks > 0) {
        for (; nr_leftover_tasklets > 0; nr_leftover_tasklets--) {
            unsigned idx_busiest = (*forks)[0];
            uint32_t busiest_nqrys = (*nr_qrys)[(*assignments)[idx_busiest].end_child - 1];
            uint8_t busiest_nr_tasklets = (*assignments)[idx_busiest].nr_tasklets;

            for (unsigned idx_fork = 1; idx_fork < nr_forks; idx_fork++) {
                const unsigned idx_assignment = (*forks)[idx_fork];
                const TaskletAssignmentToChildren* const assignment = &(*assignments)[idx_assignment];
                const uint32_t nqrys = (*nr_qrys)[assignment->end_child - 1];
                const uint8_t nr_tasklets = assignment->nr_tasklets;

                if (nqrys * busiest_nr_tasklets > busiest_nqrys * nr_tasklets) {
                    idx_busiest = idx_assignment;
                    busiest_nqrys = nqrys;
                    busiest_nr_tasklets = nr_tasklets;
                }
            }

            (*assignments)[idx_busiest].nr_tasklets++;
        }
    }
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_store_partitioning_results(const unsigned idx_partitioning, InsertPartitioning* const partitioning,
    const unsigned tasklet_begin,
    const Node* const node, const unsigned node_height, key_uint64_t node_min_key,
    const unsigned nr_assignments)
{
    const TaskletAssignmentToChildren(*const assignments)[TASK_INSERT_NR_TASKLETS] = &partitioning->assignments;
    const uint32_t(*const backet_ends)[MAX_NR_CHILDREN - 1] = &partitioning->backet_ends;
    uint32_t idx_qry_begin = partitioning->idx_qry_begin;

    InsertPartitionWorkspace* const part_wks = &workspace.tree.insert.phys.part;
    InsertPartition(*const partitions)[TASK_INSERT_NR_TASKLETS] = &part_wks->partitions;

    unsigned tasklet = tasklet_begin, begin_child = 0;
    for (unsigned idx_assignment = 0;;) {
        const TaskletAssignmentToChildren* const assignment = &(*assignments)[idx_assignment];
        const unsigned end_child = assignment->end_child, n = assignment->nr_tasklets,
                       end_child_m1 = end_child - 1;

        idx_assignment++;
        const bool last_assignment = (idx_assignment >= nr_assignments);

        const uint32_t idx_qry_end = last_assignment ? partitioning->idx_qry_end : (*backet_ends)[end_child_m1];

        InsertPartition* const partition = &(*partitions)[tasklet];
        partition->min_key = node_min_key;

        if (n == 1) {
            partition->idx_child_end = end_child;
            partition->idx_child_begin = begin_child;
            partition->idx_partitioning = idx_partitioning;
            partition->parent_height = node_height;
            partition->idx_qry_begin = idx_qry_begin;
            partition->idx_qry_end = idx_qry_end;

        } else {
            acquire_lock();
            const unsigned slot = part_wks->nr_partitionings;
            part_wks->nr_partitionings++;
            release_lock();

            InsertPartitioning* const new_partitioning = &part_wks->partitionings[slot];

            partition->idx_child_end = TASK_INSERT_PARTITIONING_LEADER(node_height - 1);
            partition->idx_partitioning = slot;

            new_partitioning->idx_qry_begin = idx_qry_begin;
            new_partitioning->idx_qry_end = idx_qry_end;

            const NodeLink link = NthChild(node->inl, end_child_m1);
            const unsigned numKeys = new_partitioning->node_numKeys = link.numKeys;
            fetch_internal_filled(&Deref(link.ptr).inl, &workspace.tree.insert.phys.node_cache[slot][0].inl, numKeys);
            Free_node(link.ptr);

            new_partitioning->nr_tasklets = n;
        }

        if (last_assignment) {
            break;
        }

        tasklet += n;
        begin_child = end_child;
        node_min_key = node->inl.keys[end_child_m1];
        idx_qry_begin = idx_qry_end;
    }
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned INSERT_plan_partitioning_impl(InsertPartitioning* const partitioning,
    const uint32_t idx_qry_begin, const uint32_t idx_qry_end,
    const unsigned nr_keys, const unsigned node_height,
    const unsigned nr_tasklets)
{
    const uint32_t(*const backet_ends)[MAX_NR_CHILDREN - 1] = &partitioning->backet_ends;
    uint32_t(*const nr_qrys)[MAX_NR_CHILDREN] = &partitioning->nr_backet_qrys;
    TaskletAssignmentToChildren(*const restrict assignments)[TASK_INSERT_NR_TASKLETS] = &partitioning->assignments;
    uint8_t(*const restrict forks)[TASK_INSERT_MAX_NR_FORK] = &partitioning->forks;

    const unsigned nr_children = nr_keys + 1;

    INSERT_nr_backet_qrys(backet_ends, nr_keys, nr_qrys, idx_qry_begin, idx_qry_end);

    unsigned nr_assigned_tasklets;
    const unsigned nqrys_per_tasklet = INSERT_min_max_nqrys_per_tasklet(nr_qrys, nr_children, idx_qry_end - idx_qry_begin, nr_tasklets, node_height == 1, &nr_assigned_tasklets);

    unsigned nr_assignments;
    unsigned nr_forks;
    INSERT_tasklet_assignments(nr_qrys, nr_children, nqrys_per_tasklet,
        assignments, &nr_assignments,
        forks, &nr_forks);
    partitioning->nr_forks = nr_forks;

    INSERT_assign_leftover_tasklets(nr_tasklets - nr_assigned_tasklets, nr_qrys, assignments, forks, nr_forks);

    return nr_assignments;
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static unsigned INSERT_plan_partitioning(InsertPartitioning* const partitioning, const unsigned node_height)
{
    return INSERT_plan_partitioning_impl(partitioning,
        partitioning->idx_qry_begin, partitioning->idx_qry_end,
        partitioning->node_numKeys, node_height,
        partitioning->nr_tasklets);
}
#endif /* SUPPORT_INSERT */


#if SUPPORT_INSERT
/* ---------------------------------------------------------------------- *
 *  TASK_INSERT: the update and the rebuild.  @sa /docs/parallel_batch_update.md
 * ---------------------------------------------------------------------- */

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static NodeLink INSERT_mram_child(const NodePtr ptr, const uint16_t j)
{
    __dma_aligned NodeLink link_pair[2];
    mram_read(&NthChild(Deref(ptr).inl, j / 2 * 2 + 1), &link_pair[0], sizeof(NodeLink) * 2);
    return link_pair[1 - j % 2];
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static NodeLink INSERT_edge_leaf_under(const NodeLink subtree, const uint8_t height, const bool rightmost)
{
    NodeLink link = subtree;
    for (uint8_t h = height; h > 0; h--) {
        link = INSERT_mram_child(link.ptr, (uint16_t)(rightmost ? link.numKeys : 0));
    }
    return link;
}
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_build_local_tree(TaskletLocalInsertWorkspace* const wks,
    const InsertPartition* const partition,
    Node* const restrict orig_root, NodeLink* const restrict p_root, unsigned* const restrict p_height)
{
    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];
    Node* const restrict inl_cache = &(*node_cache)[1];

    const unsigned idx_partitioning = partition->idx_partitioning;
    const InternalNode* const parent = (idx_partitioning == TASK_INSERT_MAX_NR_PARTITIONINGS - 1 ? &orig_root->inl : &workspace.tree.insert.phys.node_cache[idx_partitioning][0].inl);

    const unsigned parent_height = partition->parent_height, child_height = parent_height - 1;
    const unsigned idx_child_begin = partition->idx_child_begin, idx_child_end = partition->idx_child_end,
                   nr_keys = idx_child_end - idx_child_begin - 1;

    NodeLink first_child = NthChild(*parent, idx_child_begin);
    key_uint64_t max_key_of_first_child;
    NodeLink last_child;

    if (nr_keys == 0) {
        max_key_of_first_child = KEY_MAX;
        *p_root = last_child = first_child;
        *p_height = child_height;
    } else {
        max_key_of_first_child = parent->keys[idx_child_begin] - 1;

        NthChild(inl_cache->inl, 0) = first_child;
        {
            unsigned idx_child = idx_child_begin + 1;
            do {
                inl_cache->inl.keys[idx_child - idx_child_begin - 1] = parent->keys[idx_child - 1];
                last_child = NthChild(inl_cache->inl, idx_child - idx_child_begin) = NthChild(*parent, idx_child);

                idx_child++;
            } while (idx_child < idx_child_end);
        }
#ifdef DEBUG_OCCUPANCY
        inl_cache->inl.numKeys = nr_keys;
#endif

        const NodePtr ptr = Allocate_node();
        store_internal_filled(&inl_cache->inl, &Deref(ptr).inl, nr_keys);

        const NodeLink root = (NodeLink){ptr, nr_keys};
        wks->path[parent_height].link = root;
        wks->path[parent_height].max_key = KEY_MAX;

        *p_root = root;
        *p_height = parent_height;
    }

    for (unsigned h = child_height; h > 0; h--) {
        last_child = INSERT_mram_child(last_child.ptr, last_child.numKeys);
    }
    INSERT_write_right_of_leaf(last_child.ptr, NODELINK_NULLPTR);

    for (unsigned h = child_height;; h--) {
        wks->path[h].link = first_child;
        wks->path[h].idx_in_parent = 0;
        wks->path[h].max_key = max_key_of_first_child;

        if (h == 0) {
            break;
        }

        fetch_internal_filled(&Deref(first_child.ptr).inl, &inl_cache->inl, 1);
        first_child = NthChild(inl_cache->inl, 0);
        max_key_of_first_child = inl_cache->inl.keys[0] - 1;
    }

    INSERT_write_left_of_leaf(first_child, NODE_NULLPTR);

    wks->parent_loaded = false;
    wks->leaf_loaded = false;
    wks->leaf_dirty = false;
}

//! @brief Where a join inserts: the nodes of the receiving tree from the one
//! that takes the moved children (level 0) up to its root (level `top`).
typedef struct {
    NodeLink path[MAX_HEIGHT + 1];
    uint8_t top;
    bool at_tail;
} JoinPath;

//! @brief Inserts `child` at position `p` of the node at level `d` of `jp`, with
//! `key` as the delimiter before it, or after it when it goes to the head.  Both
//! node caches are used, so the caller must have written back what it holds in
//! them; `jp` is left pointing at the nodes the insertion side now runs through.
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void IJOIN_insert_child(JoinPath* const jp,
    unsigned d, unsigned p, NodeLink child, key_uint64_t key)
{
    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];

    // The new link to the level below: only a split leaves this node's copy of
    // it stale.
    bool has_fix = false;
    NodeLink fix = NODELINK_NULLPTR;

    for (;;) {
        Node* const node = &(*node_cache)[0];
        Node* const sibling = &(*node_cache)[1];
        const unsigned n = (unsigned)jp->path[d].numKeys + 1;
        const unsigned q = (p == 0 ? 0 : p - 1);

        fetch_internal_filled(&Deref(jp->path[d].ptr).inl, &node->inl, jp->path[d].numKeys);
        if (has_fix) {
            NthChild(node->inl, jp->at_tail ? n - 1 : 0) = fix;
        }

        if (n < MAX_NR_CHILDREN) {
            for (unsigned i = n; i-- > p;) {
                NthChild(node->inl, i + 1) = NthChild(node->inl, i);
            }
            for (unsigned i = n - 1; i-- > q;) {
                node->inl.keys[i + 1] = node->inl.keys[i];
            }
            NthChild(node->inl, p) = child;
            node->inl.keys[q] = key;
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = n;
#endif
            store_internal_filled(&node->inl, &Deref(jp->path[d].ptr).inl, n);
            jp->path[d].numKeys = n;
            if (d != jp->top) {
                INSERT_write_child_link(jp->path[d + 1].ptr,
                    (uint16_t)(jp->at_tail ? jp->path[d + 1].numKeys : 0), jp->path[d]);
            }
            return;
        }

        _Static_assert(MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN, "MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN");
        const unsigned nr_left = MAX_NR_CHILDREN - MIN_NR_CHILDREN;
        const key_uint64_t promoted = INSERT_combined_key(&node->inl, q, key, nr_left - 1);

        for (unsigned i = nr_left; i <= MAX_NR_CHILDREN; i++) {
            NthChild(sibling->inl, i - nr_left) = INSERT_combined_child(&node->inl, p, child, i);
            if (i != nr_left) {
                sibling->inl.keys[i - nr_left - 1] = INSERT_combined_key(&node->inl, q, key, i - 1);
            }
        }
        for (unsigned i = nr_left; i-- > 0;) {
            if (i != 0) {
                node->inl.keys[i - 1] = INSERT_combined_key(&node->inl, q, key, i - 1);
            }
            NthChild(node->inl, i) = INSERT_combined_child(&node->inl, p, child, i);
        }
#ifdef DEBUG_OCCUPANCY
        node->inl.numKeys = nr_left - 1;
        sibling->inl.numKeys = MAX_NR_CHILDREN - nr_left;
#endif
        const NodePtr sibling_ptr = Allocate_node();
        store_internal_filled(&node->inl, &Deref(jp->path[d].ptr).inl, nr_left - 1);
        store_internal_filled(&sibling->inl, &Deref(sibling_ptr).inl, MAX_NR_CHILDREN - nr_left);

        const NodeLink left = {jp->path[d].ptr, nr_left - 1},
                       right = {sibling_ptr, MAX_NR_CHILDREN - nr_left};
        jp->path[d] = (jp->at_tail ? right : left);

        if (d == jp->top) {
            node->inl.keys[0] = promoted;
            NthChild(node->inl, 0) = left;
            NthChild(node->inl, 1) = right;
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = 1;
#endif
            const NodePtr root_ptr = Allocate_node();
            store_internal_filled(&node->inl, &Deref(root_ptr).inl, 1);
            jp->path[++jp->top] = (NodeLink){root_ptr, 1};
            return;
        }

        d++;
        p = (jp->at_tail ? (unsigned)jp->path[d].numKeys + 1 : 1);
        child = right;
        key = promoted;
        has_fix = true;
        fix = left;
    }
}

//! @param[in,out] p_root, p_height  The left tree, and the tree that results.
//! @param mid  The boundary: the smallest key the right tree's range covers.
OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void IJOIN_trees(NodeLink* const p_root, uint8_t* const p_height,
    const NodeLink right_root, const uint8_t right_height, const key_uint64_t mid)
{
    Node(*const node_cache)[4] = &workspace.tree.insert.phys.node_cache[me()];
    Node* const node = &(*node_cache)[0];

    if (*p_height == 0 && right_height == 0) {
        node->inl.keys[0] = mid;
        NthChild(node->inl, 0) = *p_root;
        NthChild(node->inl, 1) = right_root;
#ifdef DEBUG_OCCUPANCY
        node->inl.numKeys = 1;
#endif
        const NodePtr ptr = Allocate_node();
        store_internal_filled(&node->inl, &Deref(ptr).inl, 1);
        *p_root = (NodeLink){ptr, 1};
        *p_height = 1;
        return;
    }

    const bool at_tail = (*p_height >= right_height);
    const NodeLink donor = (at_tail ? right_root : *p_root), recv = (at_tail ? *p_root : right_root);
    const uint8_t donor_height = (at_tail ? right_height : *p_height),
                  recv_height = (at_tail ? *p_height : right_height);
    const unsigned nr_moved = (donor_height == 0 ? 1u : (unsigned)donor.numKeys + 1);
    const uint8_t bottom_height = (donor_height == 0 ? 1 : donor_height);

    JoinPath jp;
    jp.at_tail = at_tail;
    jp.top = (uint8_t)(recv_height - bottom_height);
    jp.path[jp.top] = recv;
    for (unsigned i = jp.top; i-- > 0;) {
        jp.path[i] = INSERT_mram_child(jp.path[i + 1].ptr,
            (uint16_t)(at_tail ? jp.path[i + 1].numKeys : 0));
    }

    InternalNode* const donor_node = &(*node_cache)[2].inl;
    if (donor_height != 0) {
        fetch_internal_filled(&Deref(donor.ptr).inl, donor_node, donor.numKeys);
    }
    fetch_internal_filled(&Deref(jp.path[0].ptr).inl, &node->inl, jp.path[0].numKeys);
    unsigned nr_children = (unsigned)jp.path[0].numKeys + 1;

    for (unsigned k = 0; k < nr_moved; k++) {
        // Nearest the boundary first, since each one goes in at the boundary end.
        const unsigned j = (at_tail ? k : nr_moved - 1 - k);
        NodeLink child;
        key_uint64_t key;
        if (donor_height == 0) {
            child = donor;
            key = mid;
        } else if (at_tail) {
            child = NthChild(*donor_node, j);
            key = (j == 0 ? mid : donor_node->keys[j - 1]);
        } else {
            child = NthChild(*donor_node, j);
            key = (j + 1 == nr_moved ? mid : donor_node->keys[j]);
        }

        if (nr_children < MAX_NR_CHILDREN) {
            if (at_tail) {
                node->inl.keys[nr_children - 1] = key;
                NthChild(node->inl, nr_children) = child;
            } else {
                for (unsigned i = nr_children; i-- > 0;) {
                    NthChild(node->inl, i + 1) = NthChild(node->inl, i);
                    if (i != 0) {
                        node->inl.keys[i] = node->inl.keys[i - 1];
                    }
                }
                node->inl.keys[0] = key;
                NthChild(node->inl, 0) = child;
            }
            nr_children++;

        } else {
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = nr_children - 1;
#endif
            store_internal_filled(&node->inl, &Deref(jp.path[0].ptr).inl, nr_children - 1);
            jp.path[0].numKeys = nr_children - 1;
            IJOIN_insert_child(&jp, 0, (at_tail ? nr_children : 0), child, key);
            fetch_internal_filled(&Deref(jp.path[0].ptr).inl, &node->inl, jp.path[0].numKeys);
            nr_children = (unsigned)jp.path[0].numKeys + 1;
        }
    }

#ifdef DEBUG_OCCUPANCY
    node->inl.numKeys = nr_children - 1;
#endif
    store_internal_filled(&node->inl, &Deref(jp.path[0].ptr).inl, nr_children - 1);
    jp.path[0].numKeys = nr_children - 1;
    if (jp.top != 0) {
        INSERT_write_child_link(jp.path[1].ptr,
            (uint16_t)(at_tail ? jp.path[1].numKeys : 0), jp.path[0]);
    }
    if (donor_height != 0) {
        Free_node(donor.ptr);
    }

    *p_root = jp.path[jp.top];
    *p_height = (uint8_t)(bottom_height + jp.top);
}

OVERLAY_LOCAL(OVL_SLOT_INSERT)
static void INSERT_execute_in_parallel(Node* const root, uint8_t* const p_height, uint8_t* const p_root_numKeys, uint32_t* const p_nr_pairs,
    __mram_ptr KVPair* const qrys, const uint32_t nr_qrys, __mram_ptr KVPair* const scratch)
{
    InsertPhysWorkspace* const wks = &workspace.tree.insert.phys;
    InsertPartitionWorkspace* const part_wks = &wks->part;

    if (nr_qrys == 0) {
        return;
    }

    if (me() < TASK_INSERT_SORT_NR_TASKLETS) {
        ISORT_execute_in_parallel(qrys, nr_qrys, scratch);
#ifdef TASK_INSERT_SORT_CHECK
        if (me() == 0) {
            // The log is only read when a DPU faults, so a bad sort has to fault.
            const bool sorted = ISORT_check_sorted(qrys, nr_qrys);
            assert(sorted);
            (void)sorted;
        }
        INSERT_sort_barrier();
#endif
    }
    if (me() >= TASK_INSERT_NR_TASKLETS) {
        return;
    }
    TaskletLocalInsertWorkspace* const wks_me = &wks->th[me()];

    uint8_t height = *p_height;
    INSERT_barrier();
    if (height == 0 || nr_qrys <= TASK_INSERT_SORT_RUN) {
        if (me() == 0) {
            INSERT_execute_batch_wram_root(wks_me, root, p_height, p_root_numKeys, p_nr_pairs, qrys, 0, nr_qrys);
        }
        INSERT_barrier();
        return;
    }

    const unsigned orig_root_numKeys = *p_root_numKeys;
    InsertPartitioning* const root_partitioning = &part_wks->partitionings[TASK_INSERT_MAX_NR_PARTITIONINGS - 1];

    INSERT_find_bucket_begins(qrys, 0, nr_qrys, &root->inl.keys, orig_root_numKeys, &root_partitioning->backet_ends, me());

    InsertPartition* const my_partition = &part_wks->partitions[me()];
    my_partition->idx_child_end = 0;

    INSERT_wait_for_all_next(TASK_INSERT_NR_TASKLETS);

    if (me() == 0) {
        const unsigned nr_assignments = INSERT_plan_partitioning_impl(root_partitioning,
            0, nr_qrys,
            orig_root_numKeys, height,
            TASK_INSERT_NR_TASKLETS);

        part_wks->nr_partitionings = 0;
        root_partitioning->idx_qry_begin = 0;
        root_partitioning->idx_qry_end = nr_qrys;
        root_partitioning->node_numKeys = orig_root_numKeys;
        INSERT_store_partitioning_results(TASK_INSERT_MAX_NR_PARTITIONINGS - 1, root_partitioning, 0, root, height, KEY_MIN, nr_assignments);
    }

    INSERT_wait_for_all_prev(TASK_INSERT_NR_TASKLETS);

    for (unsigned idx_partitioning = 0;;) {
        const unsigned idx_partitioning_end = part_wks->nr_partitionings;
        if (idx_partitioning == idx_partitioning_end) {
            break;
        }

        height--;

        unsigned idx_key = me();
        for (; idx_partitioning < idx_partitioning_end; idx_partitioning++) {
            InsertPartitioning* const partitioning = &part_wks->partitionings[idx_partitioning];
            Node* const node = &workspace.tree.insert.phys.node_cache[idx_partitioning][0];

            idx_key = INSERT_find_bucket_begins(qrys, partitioning->idx_qry_begin, partitioning->idx_qry_end,
                &node->inl.keys, partitioning->node_numKeys,
                &partitioning->backet_ends, idx_key);
        }

        INSERT_barrier();

        if (my_partition->idx_child_end == TASK_INSERT_PARTITIONING_LEADER(height)) {
            const unsigned idx_partitioning = my_partition->idx_partitioning;
            InsertPartitioning* const partitioning = &part_wks->partitionings[idx_partitioning];
            Node* const node = &workspace.tree.insert.phys.node_cache[idx_partitioning][0];

            const unsigned nr_assignments = INSERT_plan_partitioning(partitioning, height);

            INSERT_store_partitioning_results(idx_partitioning, partitioning, me(), node, height, my_partition->min_key, nr_assignments);
        }

        INSERT_barrier();
    }

    if (my_partition->idx_child_end == 0) {
        wks_me->nr_new_pairs = 0;
        wks->task_tree[me()] = NODELINK_NULLPTR;

        INSERT_barrier();

    } else {
        NodeLink subtree_root;
        unsigned subtree_height;
        INSERT_build_local_tree(wks_me, my_partition, root, &subtree_root, &subtree_height);

        wks->task_min_key[me()] = my_partition->min_key;

        INSERT_barrier();

        INSERT_execute_batch(wks_me, &subtree_root, &subtree_height, &wks_me->nr_new_pairs,
            qrys, my_partition->idx_qry_begin, my_partition->idx_qry_end);

        wks->task_tree[me()] = subtree_root;
        wks->task_tree_height[me()] = subtree_height;

        wks->leftmost_leaf[me()] = INSERT_edge_leaf_under(subtree_root, subtree_height, false);
        wks->rightmost_leaf[me()] = INSERT_edge_leaf_under(subtree_root, subtree_height, true);
    }

    for (unsigned step = 1; step < TASK_INSERT_NR_TASKLETS; step *= 2) {
        INSERT_barrier();
        const unsigned other = me() + step;
        if ((me() & (step * 2 - 1)) == 0 && other < TASK_INSERT_NR_TASKLETS && wks->task_tree[other].ptr != NODE_NULLPTR) {
            if (wks->task_tree[me()].ptr == NODE_NULLPTR) {
                wks->task_tree[me()] = wks->task_tree[other];
                wks->task_tree_height[me()] = wks->task_tree_height[other];
                wks->task_min_key[me()] = wks->task_min_key[other];
                wks->leftmost_leaf[me()] = wks->leftmost_leaf[other];
            } else {
                // The place the two chains of leaves meet, and the only leaf
                // link either tree is missing.
                INSERT_write_right_of_leaf(wks->rightmost_leaf[me()].ptr, wks->leftmost_leaf[other]);
                INSERT_write_left_of_leaf(wks->leftmost_leaf[other], wks->rightmost_leaf[me()].ptr);

                IJOIN_trees(&wks->task_tree[me()], &wks->task_tree_height[me()],
                    wks->task_tree[other], wks->task_tree_height[other], wks->task_min_key[other]);
            }
            wks->rightmost_leaf[me()] = wks->rightmost_leaf[other];
        }
    }

    if (me() == 0) {
        for (unsigned t = 0; t < TASK_INSERT_NR_TASKLETS; t++) {
            *p_nr_pairs += workspace.tree.insert.phys.th[t].nr_new_pairs;
        }
        const NodeLink joined = wks->task_tree[0];
        fetch_internal_filled(&Deref(joined.ptr).inl, &root->inl, joined.numKeys);
        *p_root_numKeys = (uint8_t)joined.numKeys;
        *p_height = wks->task_tree_height[0];
        Free_node(joined.ptr);
    }
    INSERT_barrier();
}

OVERLAY_TASK_STATIC(OVL_SLOT_INSERT, INSERT_batch_phase, (void), ())
{
    const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;
    __mram_ptr KVPair* const qrys = (__mram_ptr KVPair*)((uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader));
    __mram_ptr NrPairs* const result = (__mram_ptr NrPairs*)((uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset);
    __mram_ptr KVPair* const scratch = (__mram_ptr KVPair*)(result + 1);

    INSERT_execute_in_parallel(&cold_root, &cold_height, &cold_root_numKeys, &nr_pairs.cold,
        qrys, nr_cold_qrys, scratch);
#if TASK_INSERT_NR_TASKLETS != TASK_INSERT_SORT_NR_TASKLETS
    if (me() < TASK_INSERT_SORT_NR_TASKLETS) {
        INSERT_sort_barrier();
    }
#endif
    INSERT_execute_in_parallel(&hot_root, &hot_height, &hot_root_numKeys, &nr_pairs.hot,
        qrys + nr_cold_qrys, nr_hot_qrys, scratch);

    if (me() == 0) {
        report_nr_pairs(result);
    }
}
void task_insert(void)
{
    INSERT_batch_phase();
#ifdef TASK_INSERT_CHECK
    CHECK_trees();
#endif
}
#endif


#if SUPPORT_DELETE
/* ---------------------------------------------------------------------- *
 *  TASK_DELETE.  @sa /docs/parallel_delete.md
 * ---------------------------------------------------------------------- */

// Serializes the read-modify-write of a value slot when the same key is deleted
// by two tasklets (duplicate keys within one batch).
MUTEX_POOL_INIT(DELETE_value_mutexes, 16);

// The marks the first stage writes into a value slot, all below the range a
// user may store.  `DELETE_CLAIM(t)` says that tasklet `t` is to report the
// deletion; the tombstone is what the reporting tasklet leaves behind.
#define DELETE_TOMBSTONE NOT_FOUND_VALUE
#define DELETE_CLAIM(t) ((value_int64_t)(NOT_FOUND_VALUE + 1 + (int64_t)(t)))
_Static_assert(DELETE_CLAIM(TASK_DELETE_NR_TASKLETS - 1) < VALUE_MIN, "the marks stay out of the range a user may store");

//! No pair with this key, so the second pass has nothing to read.
#define DELETE_NO_SLOT UINTPTR_MAX
//! The bytes one value-slot address per query takes, rounded up so that the
//! slice of the array a tasklet owns starts and ends on an 8-byte boundary.
#define DELETE_SLOT_BYTES(nr_qrys) (((nr_qrys) * sizeof(uintptr_t) + 7u) / 8u * 8u)

//! @pre Called by one tasklet, before a barrier that the rest go through.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_layout(void)
{
    DeleteLayout* const out = &workspace.tree.delete.layout;
    const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;
    out->qrys = (__mram_ptr key_uint64_t*)((uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader));
    out->nr_cold_qrys = nr_cold_qrys;
    out->nr_hot_qrys = nr_hot_qrys;

    const uintptr_t nr_refreshes_addr = (uintptr_t)out->qrys + sizeof(key_uint64_t) * (nr_cold_qrys + nr_hot_qrys);
    mram_read((__mram_ptr void*)nr_refreshes_addr, &out->nr_refreshes[0], sizeof(uint32_t[2]));
    out->refresh_requests = nr_refreshes_addr + sizeof(uint32_t[2]);

    out->cold_results = (uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset;
    out->hot_results = out->cold_results + (nr_cold_qrys + 7u) / 8u * 8u;
    out->counts_result = out->hot_results + (nr_hot_qrys + 7u) / 8u * 8u;
    out->refresh_results = out->counts_result + sizeof(nr_pairs);

    out->cold_slots = out->refresh_results + sizeof(KVPair) * (out->nr_refreshes[0] + out->nr_refreshes[1]);
    out->hot_slots = out->cold_slots + DELETE_SLOT_BYTES(nr_cold_qrys);
    out->sort_scratch = (__mram_ptr key_uint64_t*)(out->hot_slots + DELETE_SLOT_BYTES(nr_hot_qrys));
}

// A function belongs to one overlay slot, so each half of the task needs its
// own copy of the chain barrier.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_wait_for_all_prev(const unsigned nr_tasklets)
{
    if (me() != 0) {
        wait_for_prev_ready();
    }
    if (me() != nr_tasklets - 1) {
        notify_next_of_readiness();
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_wait_for_all_next(const unsigned nr_tasklets)
{
    if (me() != nr_tasklets - 1) {
        wait_for_next_ready();
    }
    if (me() != 0) {
        notify_prev_of_readiness();
    }
}
//! Over the tasklets that have a slice of the result array.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_result_barrier(void)
{
    DELETE_wait_for_all_next(TASK_DELETE_NR_TASKLETS);
    DELETE_wait_for_all_prev(TASK_DELETE_NR_TASKLETS);
}
//! Over the tasklets that sort the batch.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_sort_barrier(void)
{
    DELETE_wait_for_all_next(TASK_DELETE_SORT_NR_TASKLETS);
    DELETE_wait_for_all_prev(TASK_DELETE_SORT_NR_TASKLETS);
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_tree_wait_for_all_prev(void)
{
    if (me() != 0) {
        wait_for_prev_ready();
    }
    if (me() != TASK_DELETE_NR_TASKLETS - 1) {
        notify_next_of_readiness();
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_tree_wait_for_all_next(void)
{
    if (me() != TASK_DELETE_NR_TASKLETS - 1) {
        wait_for_next_ready();
    }
    if (me() != 0) {
        notify_prev_of_readiness();
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_tree_barrier(void)
{
    DELETE_tree_wait_for_all_next();
    DELETE_tree_wait_for_all_prev();
}


/* ---------------------------------------------------------------------- *
 *  Stage 1: the result array.
 * ---------------------------------------------------------------------- */

//! @brief Whether the mark already in the slot outranks `my_claim`, i.e. its
//! owner is to report the deletion.  A tombstone outranks everyone: the pair it
//! stands for is gone already.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static bool DELETE_yields_to(const value_int64_t value, const value_int64_t my_claim)
{
    return value < my_claim;
}

//! @brief Marks the value of `key` as the one this tasklet is to report as
//! deleted, unless a tasklet with a smaller number has marked it already.
//! @return Where this tasklet's mark went, or DELETE_NO_SLOT if it left none.
//! The address stays good until the tree is edited, which is what lets the
//! second pass answer without descending again.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static uintptr_t DELETE_claim_one(DeleteResultWorkspace* const wks, Node* const root,
    const uint8_t height, const uint8_t root_numKeys, const key_uint64_t key, const value_int64_t my_claim)
{
    if (height == 0) {
        const uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, key);
        if (!(idx_pair < root_numKeys && root->lf.keys[idx_pair] == key)) {
            return DELETE_NO_SLOT;
        }

        value_int64_t* const p_value = &NthValue(root->lf, idx_pair);
        uintptr_t slot = DELETE_NO_SLOT;
        mutex_pool_lock(&DELETE_value_mutexes, (uint16_t)key);
        if (!DELETE_yields_to(*p_value, my_claim)) {
            *p_value = my_claim;
            slot = (uintptr_t)p_value;
        }
        mutex_pool_unlock(&DELETE_value_mutexes, (uint16_t)key);
        return slot;
    }

    NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, key));
    for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
        fetch_internal_filled(&Deref(link.ptr).inl, &wks->node_cache.inl, link.numKeys);
        const uint16_t idx_child = search_for_child_index(&wks->node_cache.inl.keys[0], link.numKeys, key);
        link = NthChild(wks->node_cache.inl, idx_child);
    }
    fetch_leaf_filled(&Deref(link.ptr).lf, &wks->node_cache.lf, link.numKeys);
    const uint16_t idx_pair = search_for_pair_index(&wks->node_cache.lf.keys[0], link.numKeys, key);
    if (!(idx_pair < link.numKeys && wks->node_cache.lf.keys[idx_pair] == key)) {
        return DELETE_NO_SLOT;
    }

    // The cached value predates the lock, so a mark it does not show may be on
    // the slot already.  A mark it does show settles the matter on its own:
    // marks only ever move to smaller-numbered tasklets, so a tasklet that has
    // once given way stays out of the running.
    if (DELETE_yields_to(NthValue(wks->node_cache.lf, idx_pair), my_claim)) {
        return DELETE_NO_SLOT;
    }

    __mram_ptr value_int64_t* const p_value = &NthValue(Deref(link.ptr).lf, idx_pair);
    __dma_aligned value_int64_t value;
    uintptr_t slot = DELETE_NO_SLOT;
    mutex_pool_lock(&DELETE_value_mutexes, (uint16_t)key);
    mram_read(p_value, &value, sizeof(value_int64_t));
    if (!DELETE_yields_to(value, my_claim)) {
        value = my_claim;
        mram_write(&value, p_value, sizeof(value_int64_t));
        slot = (uintptr_t)p_value;
    }
    mutex_pool_unlock(&DELETE_value_mutexes, (uint16_t)key);
    return slot;
}

//! @brief Turns this tasklet's own mark on `slot` into a tombstone.
//! @return Whether the mark was still there, which is what the query reports.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static uint8_t DELETE_report_one(const uint8_t height, const uintptr_t slot, const value_int64_t my_claim)
{
    if (slot == DELETE_NO_SLOT) {
        return 0;
    }

    if (height == 0) {
        value_int64_t* const p_value = (value_int64_t*)slot;
        if (*p_value != my_claim) {
            return 0;
        }
        *p_value = DELETE_TOMBSTONE;
        return 1;
    }

    __mram_ptr value_int64_t* const p_value = (__mram_ptr value_int64_t*)slot;
    __dma_aligned value_int64_t value;
    mram_read(p_value, &value, sizeof(value_int64_t));
    if (value != my_claim) {
        return 0;
    }
    value = DELETE_TOMBSTONE;
    mram_write(&value, p_value, sizeof(value_int64_t));
    return 1;
}

//! @brief Marks a slice of the batch and records where each mark went.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_claim_pass(Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uint32_t idx_qry_begin, const uint32_t idx_qry_end, const uintptr_t slots)
{
    DeleteResultWorkspace* const wks_me = loop_invariant(&workspace.tree.delete.th[me()]);
    __mram_ptr const key_uint64_t* cursor_on_qrys
        = (__mram_ptr const key_uint64_t*)((uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader)) + idx_qry_begin;
    uintptr_t cursor_on_slots = slots;
    const value_int64_t my_claim = DELETE_CLAIM(me());
    uint32_t nr_left = idx_qry_end - idx_qry_begin;

    for (; nr_left >= TASK_DELETE_NR_CACHED_QRYS; nr_left -= TASK_DELETE_NR_CACHED_QRYS) {
        mram_read(cursor_on_qrys, wks_me->qrys, sizeof(key_uint64_t) * TASK_DELETE_NR_CACHED_QRYS);
        cursor_on_qrys += TASK_DELETE_NR_CACHED_QRYS;

        for (uint32_t i = 0; i < TASK_DELETE_NR_CACHED_QRYS; i++) {
            wks_me->slot_addrs[i] = DELETE_claim_one(wks_me, root, height, root_numKeys, wks_me->qrys[i], my_claim);
        }

        mram_write(wks_me->slot_addrs, (__mram_ptr void*)cursor_on_slots, DELETE_SLOT_BYTES(TASK_DELETE_NR_CACHED_QRYS));
        cursor_on_slots += DELETE_SLOT_BYTES(TASK_DELETE_NR_CACHED_QRYS);
    }

    if (nr_left != 0) {
        mram_read(cursor_on_qrys, wks_me->qrys, sizeof(key_uint64_t) * nr_left);

        for (uint32_t i = 0; i < nr_left; i++) {
            wks_me->slot_addrs[i] = DELETE_claim_one(wks_me, root, height, root_numKeys, wks_me->qrys[i], my_claim);
        }

        // Rounding up may spill one address past this tasklet's slice, into the
        // padding that keeps every slice 8-byte aligned.
        mram_write(wks_me->slot_addrs, (__mram_ptr void*)cursor_on_slots, DELETE_SLOT_BYTES(nr_left));
    }
}

//! @brief Answers a slice of the batch from the marks the first pass left.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DELETE_report_pass(const uint8_t height,
    const uint32_t idx_qry_begin, const uint32_t idx_qry_end, const uintptr_t slots, const uintptr_t results)
{
    DeleteResultWorkspace* const wks_me = loop_invariant(&workspace.tree.delete.th[me()]);
    uintptr_t cursor_on_slots = slots, cursor_on_results = results;
    const value_int64_t my_claim = DELETE_CLAIM(me());
    uint32_t nr_left = idx_qry_end - idx_qry_begin;

    for (; nr_left >= TASK_DELETE_NR_CACHED_QRYS; nr_left -= TASK_DELETE_NR_CACHED_QRYS) {
        mram_read((__mram_ptr void*)cursor_on_slots, wks_me->slot_addrs, DELETE_SLOT_BYTES(TASK_DELETE_NR_CACHED_QRYS));
        cursor_on_slots += DELETE_SLOT_BYTES(TASK_DELETE_NR_CACHED_QRYS);

        for (uint32_t i = 0; i < TASK_DELETE_NR_CACHED_QRYS; i++) {
            wks_me->results[i] = DELETE_report_one(height, wks_me->slot_addrs[i], my_claim);
        }

        mram_write(wks_me->results, (__mram_ptr void*)cursor_on_results, TASK_DELETE_NR_CACHED_QRYS);
        cursor_on_results += TASK_DELETE_NR_CACHED_QRYS;
    }

    if (nr_left != 0) {
        mram_read((__mram_ptr void*)cursor_on_slots, wks_me->slot_addrs, DELETE_SLOT_BYTES(nr_left));

        for (uint32_t i = 0; i < nr_left; i++) {
            wks_me->results[i] = DELETE_report_one(height, wks_me->slot_addrs[i], my_claim);
        }

        // Rounding up may spill past this tasklet's slice, but only the last
        // block of the last slice can have a non-8 remainder, and sections are
        // padded to 8.
        mram_write(wks_me->results, (__mram_ptr void*)cursor_on_results, (nr_left + 7u) / 8u * 8u);
    }
}


/* ---------------------------------------------------------------------- *
 *  Stage 2, sorting the keys.  A copy of TASK_INSERT's batch sort
 *  (/docs/parallel_batch_update.md) over bare keys instead of pairs.
 * ---------------------------------------------------------------------- */

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static unsigned DSORT_digit(const key_uint64_t key, const unsigned shift)
{
    return (unsigned)(key >> shift) & (TASK_DELETE_SORT_NR_PIECES - 1);
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static unsigned DSORT_first_shift(const key_uint64_t min_key, const key_uint64_t max_key)
{
    const unsigned width = KEY_WIDTH - countl_zero_uint64(min_key ^ max_key);
    return (width > TASK_DELETE_SORT_RADIX_BITS ? width - TASK_DELETE_SORT_RADIX_BITS : 0);
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_min_max(key_uint64_t* const p_min, key_uint64_t* const p_max, key_uint64_t (*const buf)[TASK_DELETE_SORT_RUN],
    __mram_ptr const key_uint64_t* const qrys, const uint32_t begin, const uint32_t end)
{
    key_uint64_t min_key = KEY_MAX, max_key = KEY_MIN;

    for (uint32_t i = begin; i < end;) {
        __mram_ptr const key_uint64_t* const to_read = qrys + i;

        uint32_t n = end - i;
        if (n >= TASK_DELETE_SORT_RUN) {
            n = TASK_DELETE_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * n);
        }

        for (uint32_t j = 0; j < n; j++) {
            if ((*buf)[j] < min_key) {
                min_key = (*buf)[j];
            }
            if ((*buf)[j] > max_key) {
                max_key = (*buf)[j];
            }
        }
        i += n;
    }
    *p_min = min_key;
    *p_max = max_key;
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_count_digits(uint32_t (*const counts_me)[TASK_DELETE_SORT_NR_PIECES], key_uint64_t (*const buf)[TASK_DELETE_SORT_RUN],
    __mram_ptr const key_uint64_t* const src, const uint32_t begin, const uint32_t end, const unsigned shift)
{
    for (unsigned d = 0; d < TASK_DELETE_SORT_NR_PIECES; d++) {
        (*counts_me)[d] = 0;
    }
    for (uint32_t i = begin; i < end;) {
        __mram_ptr const key_uint64_t* const to_read = src + i;

        uint32_t n = end - i;
        if (n >= TASK_DELETE_SORT_RUN) {
            n = TASK_DELETE_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * n);
        }

        for (uint32_t j = 0; j < n; j++) {
            (*counts_me)[DSORT_digit((*buf)[j], shift)]++;
        }
        i += n;
    }
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_scatter_by_digit(uint32_t (*const offsets_me)[TASK_DELETE_SORT_NR_PIECES], key_uint64_t (*const buf)[TASK_DELETE_SORT_RUN],
    key_uint64_t (*const digit_buf)[TASK_DELETE_SORT_NR_PIECES][TASK_DELETE_SORT_DIGIT_BUF],
    uint8_t (*const nr_in_digit_buf)[TASK_DELETE_SORT_NR_PIECES],
    __mram_ptr const key_uint64_t* const src, __mram_ptr key_uint64_t* const dst,
    const uint32_t begin, const uint32_t end, const unsigned shift)
{
    for (unsigned d = 0; d < TASK_DELETE_SORT_NR_PIECES; d++) {
        (*nr_in_digit_buf)[d] = 0;
    }

    for (uint32_t i = begin; i < end;) {
        __mram_ptr const key_uint64_t* const to_read = src + i;

        uint32_t n = end - i;
        if (n >= TASK_DELETE_SORT_RUN) {
            n = TASK_DELETE_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * n);
        }

        for (uint32_t j = 0; j < n; j++) {
            const key_uint64_t key = (*buf)[j];

            const unsigned d = DSORT_digit(key, shift);
            const unsigned nr_buffered = (*nr_in_digit_buf)[d];
            key_uint64_t(*const out_buf)[TASK_DELETE_SORT_DIGIT_BUF] = &(*digit_buf)[d];

            if (nr_buffered >= TASK_DELETE_SORT_DIGIT_BUF) {
                mram_write(&(*out_buf)[0], dst + (*offsets_me)[d], sizeof(key_uint64_t) * TASK_DELETE_SORT_DIGIT_BUF);
                (*offsets_me)[d] += TASK_DELETE_SORT_DIGIT_BUF;

                (*out_buf)[0] = key;
                (*nr_in_digit_buf)[d] = 1;
            } else {
                (*out_buf)[nr_buffered] = key;
                (*nr_in_digit_buf)[d] = (uint8_t)(nr_buffered + 1);
            }
        }
        i += n;
    }
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_flush_digit_buf(uint32_t (*const offsets_me)[TASK_DELETE_SORT_NR_PIECES],
    key_uint64_t (*const digit_buf)[TASK_DELETE_SORT_NR_PIECES][TASK_DELETE_SORT_DIGIT_BUF],
    uint8_t (*const nr_in_digit_buf)[TASK_DELETE_SORT_NR_PIECES], __mram_ptr key_uint64_t* const dst)
{
    for (unsigned d = 0; d < TASK_DELETE_SORT_NR_PIECES; d++) {
        const unsigned nr_buffered = (*nr_in_digit_buf)[d];
        if (nr_buffered != 0) {
            mram_write(&(*digit_buf)[d][0], dst + (*offsets_me)[d], sizeof(key_uint64_t) * nr_buffered);
            (*offsets_me)[d] += nr_buffered;
        }
    }
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_sort_short_piece_impl(key_uint64_t* const buf, const uint32_t n,
    __mram_ptr key_uint64_t* const dst)
{
    for (uint32_t i = 1; i < n; i++) {
        const key_uint64_t key = buf[i];
        uint32_t j = i;
        if (buf[j - 1] > key) {
            do {
                buf[j] = buf[j - 1];
                j--;
            } while (j != 0 && buf[j - 1] > key);
            buf[j] = key;
        }
    }

    mram_write(&buf[0], dst, sizeof(key_uint64_t) * n);
}
//! @pre `[begin, end)` fits in `buf`: at most TASK_DELETE_SORT_RUN queries.
OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_sort_short_piece(key_uint64_t (*const buf)[TASK_DELETE_SORT_RUN], __mram_ptr const key_uint64_t* const src,
    __mram_ptr key_uint64_t* const dst, const uint32_t begin, const uint32_t end)
{
    const uint32_t n = end - begin;
    mram_read(src + begin, &(*buf)[0], sizeof(key_uint64_t) * n);

    DSORT_sort_short_piece_impl(&(*buf)[0], n, dst + begin);
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_move_piece(key_uint64_t (*const buf)[TASK_DELETE_SORT_RUN], __mram_ptr const key_uint64_t* const src,
    __mram_ptr key_uint64_t* const dst, const uint32_t begin, const uint32_t end)
{
    if (src == dst) {
        return;
    }
    for (uint32_t i = begin; i < end;) {
        __mram_ptr const key_uint64_t* const to_read = src + i;
        __mram_ptr key_uint64_t* const to_write = dst + i;

        uint32_t n = end - i;
        if (end - i >= TASK_DELETE_SORT_RUN) {
            n = TASK_DELETE_SORT_RUN;
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN);
            mram_write(&(*buf)[0], to_write, sizeof(key_uint64_t) * TASK_DELETE_SORT_RUN);
        } else {
            mram_read(to_read, &(*buf)[0], sizeof(key_uint64_t) * n);
            mram_write(&(*buf)[0], to_write, sizeof(key_uint64_t) * n);
        }

        i += n;
    }
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_plan_top_digit(DeleteSortTopDigit* const top_digit, const uint32_t nr_qrys)
{
    DeleteSortWorkspace* const wks = &workspace.tree.delete.sort;
    uint32_t(*const piece_delims)[TASK_DELETE_SORT_NR_PIECES + 1] = &top_digit->piece_delims;

    for (unsigned d = me(); d < TASK_DELETE_SORT_NR_PIECES; d += TASK_DELETE_SORT_NR_TASKLETS) {
        uint32_t total = 0;
        for (unsigned t = 0; t < TASK_DELETE_SORT_NR_TASKLETS; t++) {
            total += wks->counts[t][d];
        }
        (*piece_delims)[d + 1] = total;
    }
    DELETE_wait_for_all_next(TASK_DELETE_SORT_NR_TASKLETS);

    if (me() == 0) {
        (*piece_delims)[0] = 0;
        for (unsigned d = 1; d <= TASK_DELETE_SORT_NR_PIECES; d++) {
            (*piece_delims)[d] += (*piece_delims)[d - 1];
        }

        uint32_t threshold = nr_qrys;
        unsigned d = 0;
        wks->top_digit_split[0] = 0;
        for (unsigned t = 1; t < TASK_DELETE_SORT_NR_TASKLETS; t++, threshold += nr_qrys) {
            while ((*piece_delims)[d + 1] * TASK_DELETE_SORT_NR_TASKLETS < threshold) {
                d++;
            }
            wks->top_digit_split[t] = (uint16_t)d;
        }
        wks->top_digit_split[TASK_DELETE_SORT_NR_TASKLETS] = TASK_DELETE_SORT_NR_PIECES;
    }
    DELETE_wait_for_all_prev(TASK_DELETE_SORT_NR_TASKLETS);

    for (unsigned d = me(); d < TASK_DELETE_SORT_NR_PIECES; d += TASK_DELETE_SORT_NR_TASKLETS) {
        uint32_t acc = (*piece_delims)[d];
        for (unsigned t = 0; t < TASK_DELETE_SORT_NR_TASKLETS; t++) {
            const uint32_t c = wks->counts[t][d];
            wks->counts[t][d] = acc;
            acc += c;
        }
    }
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_finish_pieces(const DeleteSortTopDigit* const top_digit,
    __mram_ptr key_uint64_t* const qrys, __mram_ptr key_uint64_t* const scratch,
    __mram_ptr DeleteSortStackEntry (*const stack)[TASK_DELETE_SORT_STACK_CAPACITY],
    const unsigned first_shift)
{
    DeleteSortWorkspace* const wks = &workspace.tree.delete.sort;
    key_uint64_t(*const buf)[TASK_DELETE_SORT_RUN] = &wks->buf[me()];
    const uint32_t(*const piece_delims)[TASK_DELETE_SORT_NR_PIECES + 1] = &top_digit->piece_delims;
    uint32_t(*const counts_me)[TASK_DELETE_SORT_NR_PIECES] = &wks->counts[me()];

    uint32_t nr_stacked = 0;

    for (unsigned d = wks->top_digit_split[me()]; d < wks->top_digit_split[me() + 1]; d++) {
        if ((*piece_delims)[d] != (*piece_delims)[d + 1]) {
            DeleteSortStackEntry entry;
            entry.begin = (*piece_delims)[d];
            entry.end = (*piece_delims)[d + 1];
            entry.prev_shift = first_shift;
            entry.src = scratch;
            mram_write(&entry, &(*stack)[nr_stacked++], sizeof(DeleteSortStackEntry));
        }
    }

    while (nr_stacked != 0) {
        DeleteSortStackEntry entry;
        mram_read(&(*stack)[--nr_stacked], &entry, sizeof(DeleteSortStackEntry));
        const uint32_t begin = entry.begin, end = entry.end, prev_shift = entry.prev_shift;
        __mram_ptr key_uint64_t* const src = entry.src;
        __mram_ptr key_uint64_t* const dst = (src == qrys ? scratch : qrys);

        if (prev_shift == 0) {
            DSORT_move_piece(buf, src, qrys, begin, end);
            continue;
        }
        if (end - begin <= TASK_DELETE_SORT_RUN) {
            DSORT_sort_short_piece(buf, src, qrys, begin, end);
            continue;
        }
        uint32_t shift = prev_shift >= TASK_DELETE_SORT_RADIX_BITS ? prev_shift - TASK_DELETE_SORT_RADIX_BITS : 0;

        DSORT_count_digits(counts_me, buf, src, begin, end, shift);
        uint32_t acc = begin;
        for (unsigned d = 0; d < TASK_DELETE_SORT_NR_PIECES; d++) {
            const uint32_t c = (*counts_me)[d];
            (*counts_me)[d] = acc;
            acc += c;
        }
        key_uint64_t(*const digit_buf)[TASK_DELETE_SORT_NR_PIECES][TASK_DELETE_SORT_DIGIT_BUF] = &wks->digit_buf[me()];
        uint8_t(*const nr_in_digit_buf)[TASK_DELETE_SORT_NR_PIECES] = &wks->nr_in_digit_buf[me()];
        DSORT_scatter_by_digit(counts_me, buf, digit_buf, nr_in_digit_buf,
            src, dst, begin, end, shift);

        uint32_t piece_begin = begin;
        for (unsigned d = 0; d < TASK_DELETE_SORT_NR_PIECES; d++) {
            key_uint64_t(*const out_buf)[TASK_DELETE_SORT_DIGIT_BUF] = &(*digit_buf)[d];
            const unsigned nr_buffered = (*nr_in_digit_buf)[d];

            const uint32_t offset = (*counts_me)[d];
            const uint32_t piece_end = offset + nr_buffered;

            if (offset == piece_begin) {
                if (nr_buffered == 0) {
                    continue;
                } else if (nr_buffered > TASK_DELETE_SORT_DIGIT_BUF) {
                    __builtin_unreachable();
                } else {
                    DSORT_sort_short_piece_impl(&(*out_buf)[0], nr_buffered, qrys + offset);
                }
            } else {
                if (nr_buffered != 0) {
                    mram_write(&(*out_buf)[0], dst + offset, sizeof(key_uint64_t) * nr_buffered);
                }

                DeleteSortStackEntry entry;
                entry.begin = piece_begin;
                entry.end = piece_end;
                entry.prev_shift = shift;
                entry.src = dst;
                mram_write(&entry, &(*stack)[nr_stacked++], sizeof(DeleteSortStackEntry));
            }
            piece_begin = piece_end;
        }
    }
}

OVERLAY_LOCAL(OVL_SLOT_DELETE)
static void DSORT_execute_in_parallel(DeleteSortTopDigit* const top_digit,
    __mram_ptr key_uint64_t* const qrys, const uint32_t nr_qrys, __mram_ptr key_uint64_t* const scratch)
{
    DeleteSortWorkspace* const wks = &workspace.tree.delete.sort;
    key_uint64_t(*const buf)[TASK_DELETE_SORT_RUN] = &wks->buf[me()];

    if (nr_qrys <= 1) {
        return;
    }
    if (nr_qrys <= TASK_DELETE_SORT_RUN) {
        if (me() == 0) {
            DSORT_sort_short_piece(buf, qrys, qrys, 0, nr_qrys);
        }
        DELETE_sort_barrier();
        return;
    }

    const uint32_t nr_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_DELETE_SORT_NR_TASKLETS(nr_qrys),
                   nr_remainder_qrys = nr_qrys - nr_qrys_per_tasklet * TASK_DELETE_SORT_NR_TASKLETS,
                   nr_qrys_for_me = nr_qrys_per_tasklet + (me() < nr_remainder_qrys);
    const uint32_t my_qry_begin = nr_qrys_per_tasklet * me() + (me() < nr_remainder_qrys ? me() : nr_remainder_qrys),
                   my_qry_end = my_qry_begin + nr_qrys_for_me;

    DSORT_min_max(&wks->qry_min_key[me()], &wks->qry_max_key[me()], buf, qrys, my_qry_begin, my_qry_end);
    DELETE_wait_for_all_next(TASK_DELETE_SORT_NR_TASKLETS);

    if (me() == 0) {
        key_uint64_t min_key = KEY_MAX, max_key = KEY_MIN;
        for (unsigned t = 0; t < TASK_DELETE_SORT_NR_TASKLETS; t++) {
            if (wks->qry_min_key[t] < min_key) {
                min_key = wks->qry_min_key[t];
            }
            if (wks->qry_max_key[t] > max_key) {
                max_key = wks->qry_max_key[t];
            }
        }
        top_digit->qry_min_key = min_key;
        top_digit->qry_max_key = max_key;
        top_digit->shift = DSORT_first_shift(min_key, max_key);
    }
    DELETE_wait_for_all_prev(TASK_DELETE_SORT_NR_TASKLETS);
    const unsigned first_shift = top_digit->shift;

    uint32_t(*const counts_me)[TASK_DELETE_SORT_NR_PIECES] = &wks->counts[me()];
    DSORT_count_digits(counts_me, buf, qrys, my_qry_begin, my_qry_end, first_shift);
    DELETE_sort_barrier();

    DSORT_plan_top_digit(top_digit, nr_qrys);
    DELETE_sort_barrier();

    key_uint64_t(*const digit_buf)[TASK_DELETE_SORT_NR_PIECES][TASK_DELETE_SORT_DIGIT_BUF] = &wks->digit_buf[me()];
    uint8_t(*const nr_in_digit_buf)[TASK_DELETE_SORT_NR_PIECES] = &wks->nr_in_digit_buf[me()];

    DSORT_scatter_by_digit(counts_me, buf, digit_buf, nr_in_digit_buf,
        qrys, scratch, my_qry_begin, my_qry_end, first_shift);
    DSORT_flush_digit_buf(counts_me, digit_buf, nr_in_digit_buf, scratch);
    DELETE_sort_barrier();

    DSORT_finish_pieces(top_digit, qrys, scratch, (__mram_ptr DeleteSortStackEntry(*)[TASK_DELETE_SORT_STACK_CAPACITY])(scratch + nr_qrys) + me(), first_shift);
    DELETE_sort_barrier();
}


/* ---------------------------------------------------------------------- *
 *  Stage 2, splitting the batch among the tasklets by key range.
 *  A copy of TASK_INSERT's handout (/docs/insert_handout.md).
 * ---------------------------------------------------------------------- */

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static uint32_t DELETE_lower_bound(__mram_ptr const key_uint64_t* const qrys, uint32_t begin, uint32_t end,
    const key_uint64_t key)
{
    while (begin < end) {
        const uint32_t mid = (begin + end) / 2;
        __dma_aligned key_uint64_t probe;
        mram_read(&qrys[mid], &probe, sizeof(key_uint64_t));
        if (probe < key) {
            begin = mid + 1;
        } else {
            end = mid;
        }
    }
    return begin;
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_find_bucket_begins(const DeleteSortTopDigit* const top_digit,
    __mram_ptr const key_uint64_t* const qrys, uint32_t begin, uint32_t end,
    const key_uint64_t (*const keys)[MAX_NR_CHILDREN - 1], const unsigned nr_keys,
    uint32_t (*const restrict out)[MAX_NR_CHILDREN - 1], const unsigned idx_key_offset)
{
    const key_uint64_t qry_min_key = top_digit->qry_min_key, qry_max_key = top_digit->qry_max_key;
    const uint32_t shift = top_digit->shift;
    const uint32_t(*const piece_delims)[TASK_DELETE_SORT_NR_PIECES + 1] = &top_digit->piece_delims;

    unsigned idx_key = idx_key_offset;
    for (; idx_key < nr_keys; idx_key += TASK_DELETE_NR_TASKLETS) {
        const key_uint64_t key = (*keys)[idx_key];

        uint32_t floor_pos;
        if (key <= qry_min_key) {
            floor_pos = begin;
        } else if (key > qry_max_key) {
            floor_pos = end;
        } else {
            const unsigned digit = DSORT_digit(key, shift);
            const uint32_t piece_begin = (*piece_delims)[digit], piece_end = (*piece_delims)[digit + 1];
            if (begin < piece_begin) {
                begin = piece_begin;
            }
            const uint32_t tmp_end = piece_end < end ? piece_end : end;
            floor_pos = DELETE_lower_bound(qrys, begin, tmp_end, key);
        }

        begin = floor_pos;
        (*out)[idx_key] = floor_pos;
    }
    return idx_key - nr_keys;
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_nr_backet_qrys(const uint32_t (*const backet_ends)[MAX_NR_CHILDREN - 1], const unsigned nr_keys,
    uint32_t (*const nr_qrys)[MAX_NR_CHILDREN], const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    uint32_t prev_backet_end = (*backet_ends)[0];
    (*nr_qrys)[0] = prev_backet_end - idx_qry_begin;
    for (unsigned idx_child = 1; idx_child < nr_keys; idx_child++) {
        const uint32_t backet_end = (*backet_ends)[idx_child];
        (*nr_qrys)[idx_child] = backet_end - prev_backet_end;
        prev_backet_end = backet_end;
    }
    (*nr_qrys)[nr_keys] = idx_qry_end - prev_backet_end;
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_nr_sharers(const uint32_t total, const uint32_t limit)
{
    unsigned k = 1;
    for (uint32_t covered = limit; covered < total; covered += limit) {
        k++;
    }
    return k;
}
//! @return `need(limit)` (/docs/insert_handout.md §3).
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_nr_tasklets_needed(const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN], const unsigned nr_children,
    const uint32_t nqrys_limit, const bool is_child_leaf)
{
    const uint32_t* nr_qrys_cursor = &(*nr_qrys)[0];
    const uint32_t* const end_nr_qrys = nr_qrys_cursor + nr_children;
    unsigned result = 0;

    uint32_t next_nr_qrys = *nr_qrys_cursor;
    for (;;) {
        nr_qrys_cursor++;

        if (next_nr_qrys > nqrys_limit) {
            if (is_child_leaf) {
                return UINT_MAX;
            }
            result += DELETE_nr_sharers(next_nr_qrys, nqrys_limit);

            if (nr_qrys_cursor >= end_nr_qrys) {
                return result;
            }
            next_nr_qrys = *nr_qrys_cursor;

        } else {
            result++;

            uint32_t sum_nr_qrys = next_nr_qrys;
            for (;;) {
                if (nr_qrys_cursor >= end_nr_qrys) {
                    return result;
                } else {
                    next_nr_qrys = *nr_qrys_cursor;

                    sum_nr_qrys += next_nr_qrys;
                    if (sum_nr_qrys <= nqrys_limit) {
                        nr_qrys_cursor++;
                    } else {
                        break;
                    }
                }
            }
        }
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_min_max_nqrys_per_tasklet(const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN],
    const unsigned nr_children, const uint32_t total_nr_qrys,
    const unsigned nr_tasklets, const bool is_child_leaf,
    unsigned* const restrict p_nr_assigned_tasklets)
{
    uint32_t lo = (total_nr_qrys + nr_tasklets - 1) / nr_tasklets, hi = total_nr_qrys;
    unsigned nr_tasklets_at_hi = 1;

    while (lo < hi) {
        const uint32_t mid = (lo + hi) / 2;
        const unsigned nr_tasklets_at_mid = DELETE_nr_tasklets_needed(nr_qrys, nr_children, mid, is_child_leaf);
        if (nr_tasklets_at_mid <= nr_tasklets) {
            hi = mid;
            nr_tasklets_at_hi = nr_tasklets_at_mid;
        } else {
            lo = mid + 1;
        }
    }
    *p_nr_assigned_tasklets = nr_tasklets_at_hi;
    return hi;
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_tasklet_assignments(const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN], const unsigned nr_children,
    const uint32_t nqrys_limit,
    TaskletAssignmentToChildren (*const assignments)[TASK_DELETE_NR_TASKLETS], unsigned* const restrict p_nr_assignments,
    uint8_t (*const forks)[TASK_DELETE_MAX_NR_FORK], unsigned* const restrict p_nr_forks)
{
    unsigned idx_child = 0, nr_assignments = 0, nr_forks = 0;

    uint32_t next_nr_qrys = (*nr_qrys)[idx_child];
    for (;;) {
        idx_child++;

        if (next_nr_qrys > nqrys_limit) {
            (*forks)[nr_forks] = nr_assignments;
            nr_forks++;

            TaskletAssignmentToChildren assignment;
            assignment.end_child = idx_child;
            assignment.nr_tasklets = DELETE_nr_sharers(next_nr_qrys, nqrys_limit);
            (*assignments)[nr_assignments] = assignment;
            nr_assignments++;

            if (idx_child >= nr_children) {
                goto end_of_func;
            }
            next_nr_qrys = (*nr_qrys)[idx_child];

        } else {
            TaskletAssignmentToChildren assignment;
            assignment.nr_tasklets = 1;

            uint32_t sum_nr_qrys = next_nr_qrys;
            for (;;) {
                if (idx_child >= nr_children) {
                    assignment.end_child = idx_child;
                    (*assignments)[nr_assignments] = assignment;
                    nr_assignments++;

                    goto end_of_func;

                } else {
                    next_nr_qrys = (*nr_qrys)[idx_child];

                    sum_nr_qrys += next_nr_qrys;
                    if (sum_nr_qrys <= nqrys_limit) {
                        idx_child++;
                    } else {
                        assignment.end_child = idx_child;
                        (*assignments)[nr_assignments] = assignment;
                        nr_assignments++;
                        break;
                    }
                }
            }
        }
    }

end_of_func:
    *p_nr_assignments = nr_assignments;
    *p_nr_forks = nr_forks;
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_assign_leftover_tasklets(unsigned nr_leftover_tasklets, const uint32_t (*const nr_qrys)[MAX_NR_CHILDREN],
    TaskletAssignmentToChildren (*const assignments)[TASK_DELETE_NR_TASKLETS],
    const uint8_t (*const forks)[TASK_DELETE_MAX_NR_FORK], const unsigned nr_forks)
{
    if (nr_forks > 0) {
        for (; nr_leftover_tasklets > 0; nr_leftover_tasklets--) {
            unsigned idx_busiest = (*forks)[0];
            uint32_t busiest_nqrys = (*nr_qrys)[(*assignments)[idx_busiest].end_child - 1];
            uint8_t busiest_nr_tasklets = (*assignments)[idx_busiest].nr_tasklets;

            for (unsigned idx_fork = 1; idx_fork < nr_forks; idx_fork++) {
                const unsigned idx_assignment = (*forks)[idx_fork];
                const TaskletAssignmentToChildren* const assignment = &(*assignments)[idx_assignment];
                const uint32_t nqrys = (*nr_qrys)[assignment->end_child - 1];
                const uint8_t nr_tasklets = assignment->nr_tasklets;

                if (nqrys * busiest_nr_tasklets > busiest_nqrys * nr_tasklets) {
                    idx_busiest = idx_assignment;
                    busiest_nqrys = nqrys;
                    busiest_nr_tasklets = nr_tasklets;
                }
            }

            (*assignments)[idx_busiest].nr_tasklets++;
        }
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_store_partitioning_results(const unsigned idx_partitioning, DeletePartitioning* const partitioning,
    const unsigned tasklet_begin,
    const Node* const node, const unsigned node_height, key_uint64_t node_min_key,
    const unsigned nr_assignments)
{
    const TaskletAssignmentToChildren(*const assignments)[TASK_DELETE_NR_TASKLETS] = &partitioning->assignments;
    const uint32_t(*const backet_ends)[MAX_NR_CHILDREN - 1] = &partitioning->backet_ends;
    uint32_t idx_qry_begin = partitioning->idx_qry_begin;

    DeletePartitionWorkspace* const part_wks = &workspace.tree.delete.phys.part;
    DeletePartition(*const partitions)[TASK_DELETE_NR_TASKLETS] = &part_wks->partitions;

    unsigned tasklet = tasklet_begin, begin_child = 0;
    for (unsigned idx_assignment = 0;;) {
        const TaskletAssignmentToChildren* const assignment = &(*assignments)[idx_assignment];
        const unsigned end_child = assignment->end_child, n = assignment->nr_tasklets,
                       end_child_m1 = end_child - 1;

        idx_assignment++;
        const bool last_assignment = (idx_assignment >= nr_assignments);

        const uint32_t idx_qry_end = last_assignment ? partitioning->idx_qry_end : (*backet_ends)[end_child_m1];

        DeletePartition* const partition = &(*partitions)[tasklet];
        partition->min_key = node_min_key;

        if (n == 1) {
            partition->idx_child_end = end_child;
            partition->idx_child_begin = begin_child;
            partition->idx_partitioning = idx_partitioning;
            partition->parent_height = node_height;
            partition->idx_qry_begin = idx_qry_begin;
            partition->idx_qry_end = idx_qry_end;

        } else {
            acquire_lock();
            const unsigned slot = part_wks->nr_partitionings;
            part_wks->nr_partitionings++;
            release_lock();

            DeletePartitioning* const new_partitioning = &part_wks->partitionings[slot];

            partition->idx_child_end = TASK_DELETE_PARTITIONING_LEADER(node_height - 1);
            partition->idx_partitioning = slot;

            new_partitioning->idx_qry_begin = idx_qry_begin;
            new_partitioning->idx_qry_end = idx_qry_end;

            const NodeLink link = NthChild(node->inl, end_child_m1);
            const unsigned numKeys = new_partitioning->node_numKeys = link.numKeys;
            fetch_internal_filled(&Deref(link.ptr).inl, &workspace.tree.delete.phys.node_cache[slot][0].inl, numKeys);
            Free_node(link.ptr);

            new_partitioning->nr_tasklets = n;
        }

        if (last_assignment) {
            break;
        }

        tasklet += n;
        begin_child = end_child;
        node_min_key = node->inl.keys[end_child_m1];
        idx_qry_begin = idx_qry_end;
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_plan_partitioning_impl(DeletePartitioning* const partitioning,
    const uint32_t idx_qry_begin, const uint32_t idx_qry_end,
    const unsigned nr_keys, const unsigned node_height,
    const unsigned nr_tasklets)
{
    const uint32_t(*const backet_ends)[MAX_NR_CHILDREN - 1] = &partitioning->backet_ends;
    uint32_t(*const nr_qrys)[MAX_NR_CHILDREN] = &partitioning->nr_backet_qrys;
    TaskletAssignmentToChildren(*const restrict assignments)[TASK_DELETE_NR_TASKLETS] = &partitioning->assignments;
    uint8_t(*const restrict forks)[TASK_DELETE_MAX_NR_FORK] = &partitioning->forks;

    const unsigned nr_children = nr_keys + 1;

    DELETE_nr_backet_qrys(backet_ends, nr_keys, nr_qrys, idx_qry_begin, idx_qry_end);

    unsigned nr_assigned_tasklets;
    const unsigned nqrys_per_tasklet = DELETE_min_max_nqrys_per_tasklet(nr_qrys, nr_children, idx_qry_end - idx_qry_begin, nr_tasklets, node_height == 1, &nr_assigned_tasklets);

    unsigned nr_assignments;
    unsigned nr_forks;
    DELETE_tasklet_assignments(nr_qrys, nr_children, nqrys_per_tasklet,
        assignments, &nr_assignments,
        forks, &nr_forks);
    partitioning->nr_forks = nr_forks;

    DELETE_assign_leftover_tasklets(nr_tasklets - nr_assigned_tasklets, nr_qrys, assignments, forks, nr_forks);

    return nr_assignments;
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_plan_partitioning(DeletePartitioning* const partitioning, const unsigned node_height)
{
    return DELETE_plan_partitioning_impl(partitioning,
        partitioning->idx_qry_begin, partitioning->idx_qry_end,
        partitioning->node_numKeys, node_height,
        partitioning->nr_tasklets);
}


/* ---------------------------------------------------------------------- *
 *  Stage 2, removing the pairs.
 * ---------------------------------------------------------------------- */

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_write_child_link(const NodePtr ptr, const uint16_t j, const NodeLink link)
{
    __dma_aligned NodeLink link_pair[2];
    mram_read(&NthChild(Deref(ptr).inl, j / 2 * 2 + 1), &link_pair[0], sizeof(NodeLink) * 2);
    link_pair[1 - j % 2] = link;
    mram_write(&link_pair[0], &NthChild(Deref(ptr).inl, j / 2 * 2 + 1), sizeof(NodeLink) * 2);
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_store_child_link(InternalNode* const node, const NodePtr ptr, const uint16_t j, const NodeLink link)
{
    NthChild(*node, j) = link;
    mram_write(&NthChild(*node, j / 2 * 2 + 1), &NthChild(Deref(ptr).inl, j / 2 * 2 + 1), sizeof(NodeLink) * 2);
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_write_left_of_leaf(const NodeLink leaf, const NodePtr left)
{
    struct {
        __dma_aligned NodePtr left;
#ifdef DEBUG_OCCUPANCY
        unsigned numKeys;
#endif
    } leaf_left;
    leaf_left.left = left;
#ifdef DEBUG_OCCUPANCY
    leaf_left.numKeys = leaf.numKeys;
#endif
    mram_write(&leaf_left, &Deref(leaf.ptr).lf.left, 8);
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_write_right_of_leaf(const NodePtr leaf, const NodeLink right)
{
    __dma_aligned NodeLink link_pair[2];
    link_pair[0] = right;
    mram_write(&link_pair[0], &Deref(leaf).lf.right, 8);
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static NodeLink DELETE_combined_child(const InternalNode* const node, const unsigned p, const NodeLink child, const unsigned i)
{
    return (i < p ? NthChild(*node, i) : i == p ? child
                                                : NthChild(*node, i - 1));
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static key_uint64_t DELETE_combined_key(const InternalNode* const node, const unsigned q, const key_uint64_t key, const unsigned i)
{
    return (i < q ? node->keys[i] : i == q ? key
                                           : node->keys[i - 1]);
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static NodeLink DELETE_mram_child(const NodePtr ptr, const uint16_t j)
{
    __dma_aligned NodeLink link_pair[2];
    mram_read(&NthChild(Deref(ptr).inl, j / 2 * 2 + 1), &link_pair[0], sizeof(NodeLink) * 2);
    return link_pair[1 - j % 2];
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static NodeLink DELETE_edge_leaf_under(const NodeLink subtree, const uint8_t height, const bool rightmost)
{
    NodeLink link = subtree;
    for (uint8_t h = height; h > 0; h--) {
        link = DELETE_mram_child(link.ptr, (uint16_t)(rightmost ? link.numKeys : 0));
    }
    return link;
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static bool DELETE_is_null_link(const NodeLink link)
{
    return link.ptr == NODELINK_NULLPTR.ptr && link.numKeys == NODELINK_NULLPTR.numKeys;
}

//! @brief Spreads the pairs of two adjacent leaves over one leaf if they fit,
//! and evenly over both if they do not.  Both leaves are written back, as are
//! the sibling links that name them.
//! @return The number of pairs the left leaf ends up with, or `nr_total` when
//! the two merged into it.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static unsigned DELETE_combine_leaves(LeafNode* const left, LeafNode* const right,
    const NodePtr left_ptr, const NodePtr right_ptr, const unsigned nr_left, const unsigned nr_right)
{
    const unsigned nr_total = nr_left + nr_right;

    if (nr_total <= MAX_NR_PAIRS) {
        for (unsigned i = 0; i < nr_right; i++) {
            left->keys[nr_left + i] = right->keys[i];
            NthValue(*left, nr_left + i) = NthValue(*right, i);
        }
        const NodeLink right_of_right = right->right;
        left->right = right_of_right;
#ifdef DEBUG_OCCUPANCY
        left->numKeys = nr_total;
#endif
        store_leaf_filled(left, &Deref(left_ptr).lf, nr_total);
        Free_node(right_ptr);

        const NodeLink left_link = {left_ptr, nr_total};
        if (left->left != NODE_NULLPTR) {
            DELETE_write_right_of_leaf(left->left, left_link);
        }
        if (!DELETE_is_null_link(right_of_right)) {
            DELETE_write_left_of_leaf(right_of_right, left_ptr);
        }
        return nr_total;
    }

    const unsigned new_nr_left = nr_total / 2, new_nr_right = nr_total - new_nr_left;
    if (new_nr_left < nr_left) {
        const unsigned k = nr_left - new_nr_left;
        for (unsigned i = nr_right; i-- > 0;) {
            right->keys[i + k] = right->keys[i];
            NthValue(*right, i + k) = NthValue(*right, i);
        }
        for (unsigned i = 0; i < k; i++) {
            right->keys[i] = left->keys[new_nr_left + i];
            NthValue(*right, i) = NthValue(*left, new_nr_left + i);
        }
    } else if (new_nr_left > nr_left) {
        const unsigned k = new_nr_left - nr_left;
        for (unsigned i = 0; i < k; i++) {
            left->keys[nr_left + i] = right->keys[i];
            NthValue(*left, nr_left + i) = NthValue(*right, i);
        }
        for (unsigned i = 0; i + k < nr_right; i++) {
            right->keys[i] = right->keys[i + k];
            NthValue(*right, i) = NthValue(*right, i + k);
        }
    }
    const NodeLink left_link = {left_ptr, new_nr_left}, right_link = {right_ptr, new_nr_right};
    left->right = right_link;
    right->left = left_ptr;
#ifdef DEBUG_OCCUPANCY
    left->numKeys = new_nr_left;
    right->numKeys = new_nr_right;
#endif
    store_leaf_filled(left, &Deref(left_ptr).lf, new_nr_left);
    store_leaf_filled(right, &Deref(right_ptr).lf, new_nr_right);
    if (left->left != NODE_NULLPTR) {
        DELETE_write_right_of_leaf(left->left, left_link);
    }
    return new_nr_left;
}

//! @brief Takes the node at level `d` back up to the minimum, and keeps going
//! while doing so leaves its parent short.  `nk` is the node's key count after
//! it lost a child.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_rebalance_internal(TaskletLocalDeleteWorkspace* const wks, NodeLink* const p_root, unsigned* const p_height,
    unsigned d, unsigned nk)
{
    Node(*const node_cache)[4] = &workspace.tree.delete.phys.node_cache[me()];

    for (;;) {
        wks->path[d].link.numKeys = nk;

        if (d == *p_height) {
            if (nk == 0) {
                const NodeLink child = DELETE_mram_child(wks->path[d].link.ptr, 0);
                Free_node(wks->path[d].link.ptr);
                *p_root = child;
                *p_height = d - 1;
            } else {
                *p_root = wks->path[d].link;
            }
            return;
        }
        if (nk + 1 >= MIN_NR_CHILDREN) {
            DELETE_write_child_link(wks->path[d + 1].link.ptr, (uint16_t)wks->path[d].idx_in_parent, wks->path[d].link);
            return;
        }

        InternalNode* const node = &(*node_cache)[0].inl;
        InternalNode* const parent = &(*node_cache)[1].inl;
        InternalNode* const sibling = &(*node_cache)[2].inl;

        fetch_internal_filled(&Deref(wks->path[d].link.ptr).inl, node, nk);
        const NodeLink parent_link = wks->path[d + 1].link;
        const unsigned parent_nk = parent_link.numKeys;
        fetch_internal_filled(&Deref(parent_link.ptr).inl, parent, parent_nk);

        const unsigned idx = wks->path[d].idx_in_parent;
        const bool take_left = (idx != 0);
        const unsigned idx_left = take_left ? idx - 1 : idx;
        const NodeLink sibling_link = NthChild(*parent, take_left ? idx - 1 : idx + 1);
        fetch_internal_filled(&Deref(sibling_link.ptr).inl, sibling, sibling_link.numKeys);

        InternalNode* const left = take_left ? sibling : node;
        InternalNode* const right = take_left ? node : sibling;
        const NodePtr left_ptr = take_left ? sibling_link.ptr : wks->path[d].link.ptr;
        const NodePtr right_ptr = take_left ? wks->path[d].link.ptr : sibling_link.ptr;
        const unsigned nr_left = (take_left ? sibling_link.numKeys : nk) + 1,
                       nr_right = (take_left ? nk : sibling_link.numKeys) + 1,
                       nr_total = nr_left + nr_right;
        const key_uint64_t delim = parent->keys[idx_left];

        if (nr_total <= MAX_NR_CHILDREN) {
            left->keys[nr_left - 1] = delim;
            for (unsigned i = 0; i < nr_right; i++) {
                NthChild(*left, nr_left + i) = NthChild(*right, i);
                if (i + 1 != nr_right) {
                    left->keys[nr_left + i] = right->keys[i];
                }
            }
#ifdef DEBUG_OCCUPANCY
            left->numKeys = nr_total - 1;
#endif
            store_internal_filled(left, &Deref(left_ptr).inl, nr_total - 1);
            Free_node(right_ptr);

            NthChild(*parent, idx_left) = (NodeLink){left_ptr, nr_total - 1};
            for (unsigned i = idx_left; i + 1 < parent_nk; i++) {
                parent->keys[i] = parent->keys[i + 1];
            }
            for (unsigned i = idx_left + 1; i < parent_nk; i++) {
                NthChild(*parent, i) = NthChild(*parent, i + 1);
            }
#ifdef DEBUG_OCCUPANCY
            parent->numKeys = parent_nk - 1;
#endif
            store_internal_filled(parent, &Deref(parent_link.ptr).inl, parent_nk - 1);

            d++;
            nk = parent_nk - 1;
            continue;
        }

        const unsigned new_nr_left = nr_total / 2;
        key_uint64_t new_delim;
        if (new_nr_left < nr_left) {
            const unsigned k = nr_left - new_nr_left;
            for (unsigned i = nr_right; i-- > 0;) {
                NthChild(*right, i + k) = NthChild(*right, i);
            }
            for (unsigned i = nr_right - 1; i-- > 0;) {
                right->keys[i + k] = right->keys[i];
            }
            for (unsigned i = 0; i < k; i++) {
                NthChild(*right, i) = NthChild(*left, new_nr_left + i);
                if (i + 1 != k) {
                    right->keys[i] = left->keys[new_nr_left + i];
                }
            }
            right->keys[k - 1] = delim;
            new_delim = left->keys[new_nr_left - 1];
        } else if (new_nr_left == nr_left) {
            new_delim = delim;
        } else {
            const unsigned k = new_nr_left - nr_left;
            left->keys[nr_left - 1] = delim;
            for (unsigned i = 0; i < k; i++) {
                NthChild(*left, nr_left + i) = NthChild(*right, i);
                if (i + 1 != k) {
                    left->keys[nr_left + i] = right->keys[i];
                }
            }
            new_delim = right->keys[k - 1];
            for (unsigned i = 0; i + k < nr_right; i++) {
                NthChild(*right, i) = NthChild(*right, i + k);
            }
            for (unsigned i = 0; i + k + 1 < nr_right; i++) {
                right->keys[i] = right->keys[i + k];
            }
        }
        const unsigned new_nr_right = nr_total - new_nr_left;
#ifdef DEBUG_OCCUPANCY
        left->numKeys = new_nr_left - 1;
        right->numKeys = new_nr_right - 1;
#endif
        store_internal_filled(left, &Deref(left_ptr).inl, new_nr_left - 1);
        store_internal_filled(right, &Deref(right_ptr).inl, new_nr_right - 1);

        parent->keys[idx_left] = new_delim;
        NthChild(*parent, idx_left) = (NodeLink){left_ptr, new_nr_left - 1};
        NthChild(*parent, idx_left + 1) = (NodeLink){right_ptr, new_nr_right - 1};
        store_internal_filled(parent, &Deref(parent_link.ptr).inl, parent_nk);
        return;
    }
}

//! @brief Takes the leaf held in the node cache back up to the minimum, and
//! propagates upwards if that merges it away.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_rebalance_leaf(TaskletLocalDeleteWorkspace* const wks, NodeLink* const p_root, unsigned* const p_height)
{
    Node(*const node_cache)[4] = &workspace.tree.delete.phys.node_cache[me()];
    LeafNode* const leaf = &(*node_cache)[0].lf;
    InternalNode* const parent = &(*node_cache)[1].inl;
    LeafNode* const sibling = &(*node_cache)[2].lf;

    const NodeLink parent_link = wks->path[1].link;
    const unsigned parent_nk = parent_link.numKeys;
    if (!wks->parent_loaded) {
        fetch_internal_filled(&Deref(parent_link.ptr).inl, parent, parent_nk);
    }

    const unsigned idx = wks->path[0].idx_in_parent;
    const bool take_left = (idx != 0);
    const unsigned idx_left = take_left ? idx - 1 : idx;
    const NodeLink sibling_link = NthChild(*parent, take_left ? idx - 1 : idx + 1);
    fetch_leaf_filled(&Deref(sibling_link.ptr).lf, sibling, sibling_link.numKeys);

    LeafNode* const left = take_left ? sibling : leaf;
    LeafNode* const right = take_left ? leaf : sibling;
    const NodePtr left_ptr = take_left ? sibling_link.ptr : wks->path[0].link.ptr;
    const NodePtr right_ptr = take_left ? wks->path[0].link.ptr : sibling_link.ptr;
    const unsigned nr_left_in = take_left ? sibling_link.numKeys : wks->path[0].link.numKeys,
                   nr_right_in = take_left ? wks->path[0].link.numKeys : sibling_link.numKeys,
                   nr_total = nr_left_in + nr_right_in;

    const unsigned nr_left = DELETE_combine_leaves(left, right, left_ptr, right_ptr, nr_left_in, nr_right_in);

    if (nr_left != nr_total) {
        parent->keys[idx_left] = right->keys[0];
        NthChild(*parent, idx_left) = (NodeLink){left_ptr, nr_left};
        NthChild(*parent, idx_left + 1) = (NodeLink){right_ptr, nr_total - nr_left};
        store_internal_filled(parent, &Deref(parent_link.ptr).inl, parent_nk);
        return;
    }

    // The two leaves became one, so the parent loses a child and may itself
    // fall short.
    NthChild(*parent, idx_left) = (NodeLink){left_ptr, nr_total};
    for (unsigned i = idx_left; i + 1 < parent_nk; i++) {
        parent->keys[i] = parent->keys[i + 1];
    }
    for (unsigned i = idx_left + 1; i < parent_nk; i++) {
        NthChild(*parent, i) = NthChild(*parent, i + 1);
    }
#ifdef DEBUG_OCCUPANCY
    parent->numKeys = parent_nk - 1;
#endif
    store_internal_filled(parent, &Deref(parent_link.ptr).inl, parent_nk - 1);

    DELETE_rebalance_internal(wks, p_root, p_height, 1, parent_nk - 1);
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_leave_leaf(TaskletLocalDeleteWorkspace* const wks, NodeLink* const p_root, unsigned* const p_height)
{
    if (!wks->leaf_dirty) {
        return;
    }
    wks->leaf_dirty = false;

    LeafNode* const leaf = &workspace.tree.delete.phys.node_cache[me()][0].lf;
    const NodeLink link = wks->path[0].link;

    if (*p_height == 0) {
        if (link.numKeys == 0) {
            Free_node(link.ptr);
            *p_root = NODELINK_NULLPTR;
            wks->leaf_loaded = false;
            wks->path_stale = true;
        } else {
            store_leaf_filled(leaf, &Deref(link.ptr).lf, link.numKeys);
            *p_root = link;
        }
        return;
    }

    if (link.numKeys >= MIN_NR_PAIRS) {
        store_leaf_filled(leaf, &Deref(link.ptr).lf, link.numKeys);
        if (wks->parent_loaded) {
            DELETE_store_child_link(&workspace.tree.delete.phys.node_cache[me()][1].inl, wks->path[1].link.ptr, (uint16_t)wks->path[0].idx_in_parent, link);
        } else {
            DELETE_write_child_link(wks->path[1].link.ptr, (uint16_t)wks->path[0].idx_in_parent, link);
        }
        if (leaf->left != NODE_NULLPTR) {
            DELETE_write_right_of_leaf(leaf->left, link);
        }
        return;
    }

    DELETE_rebalance_leaf(wks, p_root, p_height);
    wks->leaf_loaded = false;
    wks->parent_loaded = false;
    wks->path_stale = true;
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_descend(TaskletLocalDeleteWorkspace* const restrict wks, NodeLink* const restrict p_root, unsigned* const restrict p_height, const key_uint64_t key)
{
    DELETE_leave_leaf(wks, p_root, p_height);
    if (p_root->ptr == NODE_NULLPTR) {
        return;  // the last pair is gone, and `leaf_loaded` says so
    }

    const unsigned height = *p_height;
    unsigned d;
    if (wks->path_stale) {
        wks->path_stale = false;
        wks->path[height].link = *p_root;
        wks->path[height].max_key = KEY_MAX;
        d = height;
    } else if (key > wks->path[0].max_key) {
        d = height;
        for (unsigned i = 1; i < height; i++) {
            if (key <= wks->path[i].max_key) {
                d = i;
                break;
            }
        }
    } else {
        d = 0;
    }

    Node(*const node_cache)[4] = &workspace.tree.delete.phys.node_cache[me()];

    Node* const node = &(*node_cache)[1];
    for (; d > 0; d--) {
        const NodeLink link = wks->path[d].link;
        fetch_internal_filled(&Deref(link.ptr).inl, &node->inl, link.numKeys);
        wks->parent_loaded = (d == 1);

        const uint16_t idx = search_for_child_index(&node->inl.keys[0], (uint8_t)link.numKeys, key);
        wks->path[d - 1].link = NthChild(node->inl, idx);
        wks->path[d - 1].idx_in_parent = idx;
        wks->path[d - 1].max_key = (idx == link.numKeys ? wks->path[d].max_key : node->inl.keys[idx] - 1);
    }

    fetch_leaf_filled(&Deref(wks->path[0].link.ptr).lf, &(*node_cache)[0].lf, wks->path[0].link.numKeys);
    wks->leaf_loaded = true;
}

//! @return Whether a pair was removed.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static bool DELETE_execute(TaskletLocalDeleteWorkspace* const restrict wks, NodeLink* const restrict p_root, unsigned* const restrict p_height, const key_uint64_t key)
{
    if (wks->path_stale || key > wks->path[0].max_key || !wks->leaf_loaded) {
        DELETE_descend(wks, p_root, p_height, key);
        if (!wks->leaf_loaded) {
            return false;
        }
    }

    LeafNode* const leaf = &workspace.tree.delete.phys.node_cache[me()][0].lf;
    const unsigned nr_pairs_in_leaf = wks->path[0].link.numKeys;
    const uint16_t idx_pair = search_for_pair_index(&leaf->keys[0], (uint8_t)nr_pairs_in_leaf, key);

    if (!(idx_pair < nr_pairs_in_leaf && leaf->keys[idx_pair] == key)) {
        return false;
    }
    for (unsigned i = idx_pair; i + 1 < nr_pairs_in_leaf; i++) {
        leaf->keys[i] = leaf->keys[i + 1];
        NthValue(*leaf, i) = NthValue(*leaf, i + 1);
    }
#ifdef DEBUG_OCCUPANCY
    leaf->numKeys = nr_pairs_in_leaf - 1;
#endif
    wks->path[0].link.numKeys = nr_pairs_in_leaf - 1;
    wks->leaf_dirty = true;
    return true;
}

//! @pre The batch is in key order.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
__attribute__((noinline)) static void DELETE_execute_batch(TaskletLocalDeleteWorkspace* const restrict wks, NodeLink* const restrict p_root, unsigned* const restrict p_height,
    uint32_t* const restrict p_nr_removed, __mram_ptr key_uint64_t* const restrict qrys, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    __mram_ptr key_uint64_t* cursor = qrys + idx_qry_begin;

    uint32_t nr_removed = 0;
    key_uint64_t prev_key = KEY_MIN;
    bool has_prev = false;

    for (uint32_t nr_left = idx_qry_end - idx_qry_begin; nr_left != 0;) {
        uint32_t n = TASK_DELETE_NR_CACHED_QRYS;
        if (nr_left >= TASK_DELETE_NR_CACHED_QRYS) {
            mram_read(cursor, wks->qrys, sizeof(key_uint64_t) * TASK_DELETE_NR_CACHED_QRYS);
        } else {
            n = nr_left;
            mram_read(cursor, wks->qrys, sizeof(key_uint64_t) * n);
        }
        cursor += n;

        for (uint32_t i = 0; i < n; i++) {
            const key_uint64_t key = wks->qrys[i];
            // Sorted, so the duplicates of a key are next to one another, and
            // the handout never splits them across tasklets.
            if (has_prev && key == prev_key) {
                continue;
            }
            prev_key = key;
            has_prev = true;
            nr_removed += DELETE_execute(wks, p_root, p_height, key);
        }
        nr_left -= n;
    }
    DELETE_leave_leaf(wks, p_root, p_height);

    *p_nr_removed = nr_removed;
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_execute_batch_wram_root(TaskletLocalDeleteWorkspace* const wks, Node* const p_root, uint8_t* const p_height,
    uint8_t* const p_root_numKeys, uint32_t* const p_nr_pairs,
    __mram_ptr key_uint64_t* const qrys, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    unsigned height = *p_height;

    const NodePtr ptr = Allocate_node();
    NodeLink link = {ptr, *p_root_numKeys};

    wks->path[height].link = link;
    wks->path[height].max_key = KEY_MAX;

    if (height == 0) {
        store_leaf_filled(&p_root->lf, &Deref(ptr).lf, *p_root_numKeys);
    } else {
        store_internal_filled(&p_root->inl, &Deref(ptr).inl, *p_root_numKeys);

        NodeLink first_child = NthChild(p_root->inl, 0);
        key_uint64_t max_key_of_first_child = p_root->inl.keys[0] - 1;
        for (unsigned h = height - 1;; h--) {
            wks->path[h].link = first_child;
            wks->path[h].idx_in_parent = 0;
            wks->path[h].max_key = max_key_of_first_child;

            if (h == 0) {
                break;
            }

            fetch_internal_filled(&Deref(first_child.ptr).inl, &p_root->inl, 1);
            first_child = NthChild(p_root->inl, 0);
            max_key_of_first_child = p_root->inl.keys[0] - 1;
        }
    }
    wks->parent_loaded = false;
    wks->leaf_loaded = false;
    wks->leaf_dirty = false;
    wks->path_stale = false;

    uint32_t nr_removed = 0;
    DELETE_execute_batch(wks, &link, &height, &nr_removed, qrys, idx_qry_begin, idx_qry_end);
    *p_nr_pairs -= nr_removed;

    if (link.ptr == NODE_NULLPTR) {
        // Nothing is left of the tree; an empty leaf stands for it.
        p_root->lf.right = NODELINK_NULLPTR;
        p_root->lf.left = NODE_NULLPTR;
#ifdef DEBUG_OCCUPANCY
        p_root->lf.numKeys = 0;
#endif
        *p_height = 0;
        *p_root_numKeys = 0;
        return;
    }

    if (height == 0) {
        fetch_leaf_filled(&Deref(link.ptr).lf, &p_root->lf, link.numKeys);
    } else {
        fetch_internal_filled(&Deref(link.ptr).inl, &p_root->inl, link.numKeys);
    }
    *p_height = (uint8_t)height;
    *p_root_numKeys = (uint8_t)link.numKeys;
    Free_node(link.ptr);
}


OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
__attribute__((noinline)) static void DELETE_build_local_tree(TaskletLocalDeleteWorkspace* const wks,
    const DeletePartition* const partition,
    Node* const restrict orig_root, NodeLink* const restrict p_root, unsigned* const restrict p_height)
{
    Node(*const node_cache)[4] = &workspace.tree.delete.phys.node_cache[me()];
    Node* const restrict inl_cache = &(*node_cache)[1];

    const unsigned idx_partitioning = partition->idx_partitioning;
    const InternalNode* const parent = (idx_partitioning == TASK_DELETE_MAX_NR_PARTITIONINGS - 1 ? &orig_root->inl : &workspace.tree.delete.phys.node_cache[idx_partitioning][0].inl);

    const unsigned parent_height = partition->parent_height, child_height = parent_height - 1;
    const unsigned idx_child_begin = partition->idx_child_begin, idx_child_end = partition->idx_child_end,
                   nr_keys = idx_child_end - idx_child_begin - 1;

    NodeLink first_child = NthChild(*parent, idx_child_begin);
    key_uint64_t max_key_of_first_child;
    NodeLink last_child;

    if (nr_keys == 0) {
        max_key_of_first_child = KEY_MAX;
        *p_root = last_child = first_child;
        *p_height = child_height;
    } else {
        max_key_of_first_child = parent->keys[idx_child_begin] - 1;

        NthChild(inl_cache->inl, 0) = first_child;
        {
            unsigned idx_child = idx_child_begin + 1;
            do {
                inl_cache->inl.keys[idx_child - idx_child_begin - 1] = parent->keys[idx_child - 1];
                last_child = NthChild(inl_cache->inl, idx_child - idx_child_begin) = NthChild(*parent, idx_child);

                idx_child++;
            } while (idx_child < idx_child_end);
        }
#ifdef DEBUG_OCCUPANCY
        inl_cache->inl.numKeys = nr_keys;
#endif

        const NodePtr ptr = Allocate_node();
        store_internal_filled(&inl_cache->inl, &Deref(ptr).inl, nr_keys);

        const NodeLink root = (NodeLink){ptr, nr_keys};
        wks->path[parent_height].link = root;
        wks->path[parent_height].max_key = KEY_MAX;

        *p_root = root;
        *p_height = parent_height;
    }

    for (unsigned h = child_height; h > 0; h--) {
        last_child = DELETE_mram_child(last_child.ptr, last_child.numKeys);
    }
    DELETE_write_right_of_leaf(last_child.ptr, NODELINK_NULLPTR);

    for (unsigned h = child_height;; h--) {
        wks->path[h].link = first_child;
        wks->path[h].idx_in_parent = 0;
        wks->path[h].max_key = max_key_of_first_child;

        if (h == 0) {
            break;
        }

        fetch_internal_filled(&Deref(first_child.ptr).inl, &inl_cache->inl, 1);
        first_child = NthChild(inl_cache->inl, 0);
        max_key_of_first_child = inl_cache->inl.keys[0] - 1;
    }

    DELETE_write_left_of_leaf(first_child, NODE_NULLPTR);

    wks->parent_loaded = false;
    wks->leaf_loaded = false;
    wks->leaf_dirty = false;
    wks->path_stale = false;
}

//! @brief Where a join inserts: the nodes of the receiving tree from the one
//! that takes the moved children (level 0) up to its root (level `top`).
typedef struct {
    NodeLink path[MAX_HEIGHT + 1];
    uint8_t top;
    bool at_tail;
} DeleteJoinPath;

//! @brief Inserts `child` at position `p` of the node at level `d` of `jp`, with
//! `key` as the delimiter before it, or after it when it goes to the head.  Both
//! node caches are used, so the caller must have written back what it holds in
//! them; `jp` is left pointing at the nodes the insertion side now runs through.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DJOIN_insert_child(DeleteJoinPath* const jp,
    unsigned d, unsigned p, NodeLink child, key_uint64_t key)
{
    Node(*const node_cache)[4] = &workspace.tree.delete.phys.node_cache[me()];

    // The new link to the level below: only a split leaves this node's copy of
    // it stale.
    bool has_fix = false;
    NodeLink fix = NODELINK_NULLPTR;

    for (;;) {
        Node* const node = &(*node_cache)[0];
        Node* const sibling = &(*node_cache)[1];
        const unsigned n = (unsigned)jp->path[d].numKeys + 1;
        const unsigned q = (p == 0 ? 0 : p - 1);

        fetch_internal_filled(&Deref(jp->path[d].ptr).inl, &node->inl, jp->path[d].numKeys);
        if (has_fix) {
            NthChild(node->inl, jp->at_tail ? n - 1 : 0) = fix;
        }

        if (n < MAX_NR_CHILDREN) {
            for (unsigned i = n; i-- > p;) {
                NthChild(node->inl, i + 1) = NthChild(node->inl, i);
            }
            for (unsigned i = n - 1; i-- > q;) {
                node->inl.keys[i + 1] = node->inl.keys[i];
            }
            NthChild(node->inl, p) = child;
            node->inl.keys[q] = key;
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = n;
#endif
            store_internal_filled(&node->inl, &Deref(jp->path[d].ptr).inl, n);
            jp->path[d].numKeys = n;
            if (d != jp->top) {
                DELETE_write_child_link(jp->path[d + 1].ptr,
                    (uint16_t)(jp->at_tail ? jp->path[d + 1].numKeys : 0), jp->path[d]);
            }
            return;
        }

        _Static_assert(MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN, "MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN");
        const unsigned nr_left = MAX_NR_CHILDREN - MIN_NR_CHILDREN;
        const key_uint64_t promoted = DELETE_combined_key(&node->inl, q, key, nr_left - 1);

        for (unsigned i = nr_left; i <= MAX_NR_CHILDREN; i++) {
            NthChild(sibling->inl, i - nr_left) = DELETE_combined_child(&node->inl, p, child, i);
            if (i != nr_left) {
                sibling->inl.keys[i - nr_left - 1] = DELETE_combined_key(&node->inl, q, key, i - 1);
            }
        }
        for (unsigned i = nr_left; i-- > 0;) {
            if (i != 0) {
                node->inl.keys[i - 1] = DELETE_combined_key(&node->inl, q, key, i - 1);
            }
            NthChild(node->inl, i) = DELETE_combined_child(&node->inl, p, child, i);
        }
#ifdef DEBUG_OCCUPANCY
        node->inl.numKeys = nr_left - 1;
        sibling->inl.numKeys = MAX_NR_CHILDREN - nr_left;
#endif
        const NodePtr sibling_ptr = Allocate_node();
        store_internal_filled(&node->inl, &Deref(jp->path[d].ptr).inl, nr_left - 1);
        store_internal_filled(&sibling->inl, &Deref(sibling_ptr).inl, MAX_NR_CHILDREN - nr_left);

        const NodeLink left = {jp->path[d].ptr, nr_left - 1},
                       right = {sibling_ptr, MAX_NR_CHILDREN - nr_left};
        jp->path[d] = (jp->at_tail ? right : left);

        if (d == jp->top) {
            node->inl.keys[0] = promoted;
            NthChild(node->inl, 0) = left;
            NthChild(node->inl, 1) = right;
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = 1;
#endif
            const NodePtr root_ptr = Allocate_node();
            store_internal_filled(&node->inl, &Deref(root_ptr).inl, 1);
            jp->path[++jp->top] = (NodeLink){root_ptr, 1};
            return;
        }

        d++;
        p = (jp->at_tail ? (unsigned)jp->path[d].numKeys + 1 : 1);
        child = right;
        key = promoted;
        has_fix = true;
        fix = left;
    }
}

//! @param[in,out] p_root, p_height  The left tree, and the tree that results.
//! @param mid  The boundary: the smallest key the right tree's range covers.
//! @note A local tree that shrank to one short leaf is merged into the leaf it
//! meets here.  That is safe because the two trees are this tasklet's alone and
//! their chains of leaves are already stitched to each other.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
__attribute__((noinline)) static void DJOIN_trees(NodeLink* const p_root, uint8_t* const p_height,
    NodeLink right_root, const uint8_t right_height, key_uint64_t mid)
{
    Node(*const node_cache)[4] = &workspace.tree.delete.phys.node_cache[me()];
    Node* const node = &(*node_cache)[0];
    LeafNode* const lf_left = &(*node_cache)[1].lf;
    LeafNode* const lf_right = &(*node_cache)[3].lf;

    if (*p_height == 0 && right_height == 0) {
        // A root may hold as little as it likes, so either of these may be
        // short.  Whichever is has to stop being short here.
        if (p_root->numKeys < MIN_NR_PAIRS || right_root.numKeys < MIN_NR_PAIRS) {
            const unsigned nr_total = (unsigned)p_root->numKeys + right_root.numKeys;
            fetch_leaf_filled(&Deref(p_root->ptr).lf, lf_left, p_root->numKeys);
            fetch_leaf_filled(&Deref(right_root.ptr).lf, lf_right, right_root.numKeys);
            const unsigned nr_left = DELETE_combine_leaves(lf_left, lf_right, p_root->ptr, right_root.ptr,
                p_root->numKeys, right_root.numKeys);
            DeletePhysWorkspace* const phys = &workspace.tree.delete.phys;
            if (nr_left == nr_total) {
                *p_root = (NodeLink){p_root->ptr, nr_total};
                phys->leftmost_leaf[me()] = phys->rightmost_leaf[me()] = *p_root;
                return;
            }
            *p_root = (NodeLink){p_root->ptr, nr_left};
            right_root = (NodeLink){right_root.ptr, nr_total - nr_left};
            phys->leftmost_leaf[me()] = *p_root;
            phys->rightmost_leaf[me()] = right_root;
            mid = lf_right->keys[0];
        }

        node->inl.keys[0] = mid;
        NthChild(node->inl, 0) = *p_root;
        NthChild(node->inl, 1) = right_root;
#ifdef DEBUG_OCCUPANCY
        node->inl.numKeys = 1;
#endif
        const NodePtr ptr = Allocate_node();
        store_internal_filled(&node->inl, &Deref(ptr).inl, 1);
        *p_root = (NodeLink){ptr, 1};
        *p_height = 1;
        return;
    }

    const bool at_tail = (*p_height >= right_height);
    NodeLink donor = (at_tail ? right_root : *p_root);
    const NodeLink recv = (at_tail ? *p_root : right_root);
    const uint8_t donor_height = (at_tail ? right_height : *p_height),
                  recv_height = (at_tail ? *p_height : right_height);
    const unsigned nr_moved = (donor_height == 0 ? 1u : (unsigned)donor.numKeys + 1);
    const uint8_t bottom_height = (donor_height == 0 ? 1 : donor_height);

    DeleteJoinPath jp;
    jp.at_tail = at_tail;
    jp.top = (uint8_t)(recv_height - bottom_height);
    jp.path[jp.top] = recv;
    for (unsigned i = jp.top; i-- > 0;) {
        jp.path[i] = DELETE_mram_child(jp.path[i + 1].ptr,
            (uint16_t)(at_tail ? jp.path[i + 1].numKeys : 0));
    }

    InternalNode* const donor_node = &(*node_cache)[2].inl;
    if (donor_height != 0) {
        fetch_internal_filled(&Deref(donor.ptr).inl, donor_node, donor.numKeys);
    }
    fetch_internal_filled(&Deref(jp.path[0].ptr).inl, &node->inl, jp.path[0].numKeys);
    unsigned nr_children = (unsigned)jp.path[0].numKeys + 1;

    if (donor_height == 0 && donor.numKeys < MIN_NR_PAIRS) {
        // A local tree that shrank to one short leaf: allowed while it was that
        // tree's root, not allowed hung under an ordinary node.  Both leaves are
        // in the tree being built, so merging reaches nothing outside it.
        const unsigned idx_edge = (at_tail ? nr_children - 1 : 0);
        const NodeLink edge = NthChild(node->inl, idx_edge);
        const NodeLink left_link = (at_tail ? edge : donor), right_link = (at_tail ? donor : edge);
        const unsigned nr_total = (unsigned)left_link.numKeys + right_link.numKeys;

        fetch_leaf_filled(&Deref(left_link.ptr).lf, lf_left, left_link.numKeys);
        fetch_leaf_filled(&Deref(right_link.ptr).lf, lf_right, right_link.numKeys);
        const unsigned nr_left = DELETE_combine_leaves(lf_left, lf_right, left_link.ptr, right_link.ptr,
            left_link.numKeys, right_link.numKeys);

        // Which leaf ends the joined tree's chain is only known here.
        DeletePhysWorkspace* const phys = &workspace.tree.delete.phys;
        NodeLink* const p_end = (at_tail ? &phys->rightmost_leaf[me()] : &phys->leftmost_leaf[me()]);

        if (nr_left == nr_total) {
            // Nothing is left to hang; the node above keeps its children.
            NthChild(node->inl, idx_edge) = (NodeLink){left_link.ptr, nr_total};
            store_internal_filled(&node->inl, &Deref(jp.path[0].ptr).inl, jp.path[0].numKeys);
            *p_end = (NodeLink){left_link.ptr, nr_total};
            *p_root = recv;
            *p_height = recv_height;
            return;
        }

        NthChild(node->inl, idx_edge) = (NodeLink){edge.ptr, (at_tail ? nr_left : nr_total - nr_left)};
        donor = (NodeLink){donor.ptr, (at_tail ? nr_total - nr_left : nr_left)};
        *p_end = donor;
        mid = lf_right->keys[0];
    }

    for (unsigned k = 0; k < nr_moved; k++) {
        // Nearest the boundary first, since each one goes in at the boundary end.
        const unsigned j = (at_tail ? k : nr_moved - 1 - k);
        NodeLink child;
        key_uint64_t key;
        if (donor_height == 0) {
            child = donor;
            key = mid;
        } else if (at_tail) {
            child = NthChild(*donor_node, j);
            key = (j == 0 ? mid : donor_node->keys[j - 1]);
        } else {
            child = NthChild(*donor_node, j);
            key = (j + 1 == nr_moved ? mid : donor_node->keys[j]);
        }

        if (nr_children < MAX_NR_CHILDREN) {
            if (at_tail) {
                node->inl.keys[nr_children - 1] = key;
                NthChild(node->inl, nr_children) = child;
            } else {
                for (unsigned i = nr_children; i-- > 0;) {
                    NthChild(node->inl, i + 1) = NthChild(node->inl, i);
                    if (i != 0) {
                        node->inl.keys[i] = node->inl.keys[i - 1];
                    }
                }
                node->inl.keys[0] = key;
                NthChild(node->inl, 0) = child;
            }
            nr_children++;

        } else {
#ifdef DEBUG_OCCUPANCY
            node->inl.numKeys = nr_children - 1;
#endif
            store_internal_filled(&node->inl, &Deref(jp.path[0].ptr).inl, nr_children - 1);
            jp.path[0].numKeys = nr_children - 1;
            DJOIN_insert_child(&jp, 0, (at_tail ? nr_children : 0), child, key);
            fetch_internal_filled(&Deref(jp.path[0].ptr).inl, &node->inl, jp.path[0].numKeys);
            nr_children = (unsigned)jp.path[0].numKeys + 1;
        }
    }

#ifdef DEBUG_OCCUPANCY
    node->inl.numKeys = nr_children - 1;
#endif
    store_internal_filled(&node->inl, &Deref(jp.path[0].ptr).inl, nr_children - 1);
    jp.path[0].numKeys = nr_children - 1;
    if (jp.top != 0) {
        DELETE_write_child_link(jp.path[1].ptr,
            (uint16_t)(at_tail ? jp.path[1].numKeys : 0), jp.path[0]);
    }
    if (donor_height != 0) {
        Free_node(donor.ptr);
    }

    *p_root = jp.path[jp.top];
    *p_height = (uint8_t)(bottom_height + jp.top);
}

OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_execute_in_parallel(const DeleteSortTopDigit* const top_digit,
    Node* const root, uint8_t* const p_height, uint8_t* const p_root_numKeys, uint32_t* const p_nr_pairs,
    __mram_ptr key_uint64_t* const qrys, const uint32_t nr_qrys)
{
    DeletePhysWorkspace* const wks = &workspace.tree.delete.phys;
    DeletePartitionWorkspace* const part_wks = &wks->part;

    if (nr_qrys == 0) {
        return;
    }

    TaskletLocalDeleteWorkspace* const wks_me = &wks->th[me()];

    uint8_t height = *p_height;
    // DELETE_execute_batch_wram_root writes *p_height, so the read above has to
    // be over before any tasklet can get there.
    DELETE_tree_barrier();
    if (height == 0 || nr_qrys <= TASK_DELETE_SORT_RUN) {
        if (me() == 0) {
            DELETE_execute_batch_wram_root(wks_me, root, p_height, p_root_numKeys, p_nr_pairs, qrys, 0, nr_qrys);
        }
        DELETE_tree_barrier();
        return;
    }

    const unsigned orig_root_numKeys = *p_root_numKeys;
    DeletePartitioning* const root_partitioning = &part_wks->partitionings[TASK_DELETE_MAX_NR_PARTITIONINGS - 1];

    DELETE_find_bucket_begins(top_digit, qrys, 0, nr_qrys, &root->inl.keys, orig_root_numKeys, &root_partitioning->backet_ends, me());

    DeletePartition* const my_partition = &part_wks->partitions[me()];
    my_partition->idx_child_end = 0;

    DELETE_tree_wait_for_all_next();

    if (me() == 0) {
        const unsigned nr_assignments = DELETE_plan_partitioning_impl(root_partitioning,
            0, nr_qrys,
            orig_root_numKeys, height,
            TASK_DELETE_NR_TASKLETS);

        part_wks->nr_partitionings = 0;
        root_partitioning->idx_qry_begin = 0;
        root_partitioning->idx_qry_end = nr_qrys;
        root_partitioning->node_numKeys = (uint8_t)orig_root_numKeys;
        DELETE_store_partitioning_results(TASK_DELETE_MAX_NR_PARTITIONINGS - 1, root_partitioning, 0, root, height, KEY_MIN, nr_assignments);
    }

    DELETE_tree_wait_for_all_prev();

    for (unsigned idx_partitioning = 0;;) {
        const unsigned idx_partitioning_end = part_wks->nr_partitionings;
        if (idx_partitioning == idx_partitioning_end) {
            break;
        }

        height--;

        unsigned idx_key = me();
        for (; idx_partitioning < idx_partitioning_end; idx_partitioning++) {
            DeletePartitioning* const partitioning = &part_wks->partitionings[idx_partitioning];
            Node* const node = &wks->node_cache[idx_partitioning][0];

            idx_key = DELETE_find_bucket_begins(top_digit, qrys, partitioning->idx_qry_begin, partitioning->idx_qry_end,
                &node->inl.keys, partitioning->node_numKeys,
                &partitioning->backet_ends, idx_key);
        }

        DELETE_tree_barrier();

        if (my_partition->idx_child_end == TASK_DELETE_PARTITIONING_LEADER(height)) {
            const unsigned idx_partitioning_of_mine = my_partition->idx_partitioning;
            DeletePartitioning* const partitioning = &part_wks->partitionings[idx_partitioning_of_mine];
            Node* const node = &wks->node_cache[idx_partitioning_of_mine][0];

            const unsigned nr_assignments = DELETE_plan_partitioning(partitioning, height);

            DELETE_store_partitioning_results(idx_partitioning_of_mine, partitioning, me(), node, height, my_partition->min_key, nr_assignments);
        }

        DELETE_tree_barrier();
    }

    if (my_partition->idx_child_end == 0) {
        wks_me->nr_removed_pairs = 0;
        wks->task_tree[me()] = NODELINK_NULLPTR;

        DELETE_tree_barrier();

    } else {
        NodeLink subtree_root;
        unsigned subtree_height;
        DELETE_build_local_tree(wks_me, my_partition, root, &subtree_root, &subtree_height);

        wks->task_min_key[me()] = my_partition->min_key;

        DELETE_tree_barrier();

        DELETE_execute_batch(wks_me, &subtree_root, &subtree_height, &wks_me->nr_removed_pairs,
            qrys, my_partition->idx_qry_begin, my_partition->idx_qry_end);

        wks->task_tree[me()] = subtree_root;
        wks->task_tree_height[me()] = (uint8_t)subtree_height;

        if (subtree_root.ptr != NODE_NULLPTR) {
            wks->leftmost_leaf[me()] = DELETE_edge_leaf_under(subtree_root, (uint8_t)subtree_height, false);
            wks->rightmost_leaf[me()] = DELETE_edge_leaf_under(subtree_root, (uint8_t)subtree_height, true);
        }
    }

    for (unsigned step = 1; step < TASK_DELETE_NR_TASKLETS; step *= 2) {
        DELETE_tree_barrier();
        const unsigned other = me() + step;
        if ((me() & (step * 2 - 1)) == 0 && other < TASK_DELETE_NR_TASKLETS && wks->task_tree[other].ptr != NODE_NULLPTR) {
            if (wks->task_tree[me()].ptr == NODE_NULLPTR) {
                wks->task_tree[me()] = wks->task_tree[other];
                wks->task_tree_height[me()] = wks->task_tree_height[other];
                wks->task_min_key[me()] = wks->task_min_key[other];
                wks->leftmost_leaf[me()] = wks->leftmost_leaf[other];
                wks->rightmost_leaf[me()] = wks->rightmost_leaf[other];
            } else {
                // The place the two chains of leaves meet, and the only leaf
                // link either tree is missing.
                DELETE_write_right_of_leaf(wks->rightmost_leaf[me()].ptr, wks->leftmost_leaf[other]);
                DELETE_write_left_of_leaf(wks->leftmost_leaf[other], wks->rightmost_leaf[me()].ptr);

                // The joining may merge a boundary leaf away, and then it puts
                // the end that survived here itself.
                wks->rightmost_leaf[me()] = wks->rightmost_leaf[other];
                DJOIN_trees(&wks->task_tree[me()], &wks->task_tree_height[me()],
                    wks->task_tree[other], wks->task_tree_height[other], wks->task_min_key[other]);
            }
        }
    }

    if (me() == 0) {
        for (unsigned t = 0; t < TASK_DELETE_NR_TASKLETS; t++) {
            *p_nr_pairs -= wks->th[t].nr_removed_pairs;
        }
        const NodeLink joined = wks->task_tree[0];
        if (joined.ptr == NODE_NULLPTR) {
            // Nothing is left of the tree; an empty leaf stands for it.
            root->lf.right = NODELINK_NULLPTR;
            root->lf.left = NODE_NULLPTR;
#ifdef DEBUG_OCCUPANCY
            root->lf.numKeys = 0;
#endif
            *p_root_numKeys = 0;
            *p_height = 0;
        } else if (wks->task_tree_height[0] == 0) {
            fetch_leaf_filled(&Deref(joined.ptr).lf, &root->lf, joined.numKeys);
            *p_root_numKeys = (uint8_t)joined.numKeys;
            *p_height = 0;
            Free_node(joined.ptr);
        } else {
            fetch_internal_filled(&Deref(joined.ptr).inl, &root->inl, joined.numKeys);
            *p_root_numKeys = (uint8_t)joined.numKeys;
            *p_height = wks->task_tree_height[0];
            Free_node(joined.ptr);
        }
    }
    DELETE_tree_barrier();
}


/* ---------------------------------------------------------------------- *
 *  The host's request for the smallest live key of a range, answered once
 *  the tree is in its final shape.
 * ---------------------------------------------------------------------- */

//! @return The smallest key in `range` (both ends inclusive) as {key, 1},
//! or {0, 0} if the range holds none.
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static KVPair DELETE_refresh_one(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const KeyRange range)
{
    DeleteResultWorkspace* const wks_me = loop_invariant(&workspace.tree.delete.th[me()]);

    if (height == 0) {
        const uint16_t i = search_for_pair_index(&root->lf.keys[0], root_numKeys, range.begin);
        if (i < root_numKeys && root->lf.keys[i] <= range.end) {
            return (KVPair){root->lf.keys[i], 1};
        }
        return (KVPair){0, 0};
    }

    NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, range.begin));
    for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
        fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
        const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, range.begin);
        link = NthChild(wks_me->node_cache.inl, idx_child);
    }

    const key_uint64_t* const cached_keys = loop_invariant(&wks_me->node_cache.lf.keys[0]);
    for (;;) {
        fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
        const uint16_t i = search_for_pair_index(cached_keys, link.numKeys, range.begin);
        if (i < link.numKeys) {
            return (cached_keys[i] > range.end ? (KVPair){0, 0} : (KVPair){cached_keys[i], 1});
        }
        // Only the leaf the search lands in can hold nothing at or past the
        // start of the range, and then the answer is in the next one.
        link = wks_me->node_cache.lf.right;
        if (DELETE_is_null_link(link)) {
            return (KVPair){0, 0};
        }
    }
}
OVERLAY_LOCAL(OVL_SLOT_DELETE_TREE)
static void DELETE_refresh_mins(const uint32_t nr_cold_refreshes, const uint32_t nr_hot_refreshes, const uintptr_t requests, const uintptr_t refresh_results)
{
    DeleteResultWorkspace* const wks_me = loop_invariant(&workspace.tree.delete.th[me()]);

    const uint32_t nr_total = nr_cold_refreshes + nr_hot_refreshes;
    for (uint32_t i = 0; i < nr_total; i++) {
        mram_read((__mram_ptr void*)(requests + sizeof(KeyRange) * i), &wks_me->refresh_range, sizeof(KeyRange));
        if (i < nr_cold_refreshes) {
            wks_me->refresh_response = DELETE_refresh_one(&cold_root, cold_height, cold_root_numKeys, wks_me->refresh_range);
        } else {
            wks_me->refresh_response = DELETE_refresh_one(&hot_root, hot_height, hot_root_numKeys, wks_me->refresh_range);
        }
        mram_write(&wks_me->refresh_response, (__mram_ptr void*)(refresh_results + sizeof(KVPair) * i), sizeof(KVPair));
    }
}


OVERLAY_TASK_STATIC(OVL_SLOT_DELETE, DELETE_result_phase, (void), ())
{
    if (me() >= TASK_DELETE_SORT_NR_TASKLETS) {
        return;
    }
    if (me() == 0) {
        DELETE_layout();
    }
    DELETE_sort_barrier();
    const DeleteLayout* const lo = &workspace.tree.delete.layout;

    // A tasklet at or above TASK_DELETE_NR_TASKLETS has no slice of the result
    // array and goes straight to the barrier that ends this stage.  The chain
    // barrier keeps its state per adjacent pair of tasklets, so what such a
    // tasklet touches there starts at the pair (TASK_DELETE_NR_TASKLETS - 1,
    // TASK_DELETE_NR_TASKLETS) and never meets the pairs the rest run between.
    if (me() < TASK_DELETE_NR_TASKLETS) {
        // Per-tasklet quotas are rounded up to a multiple of 8 so that no two
        // tasklets share an 8-byte word of the 1-byte result flags.
        const uint32_t nr_cold_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_DELETE_NR_TASKLETS(lo->nr_cold_qrys),
                       cold_quota = (nr_cold_qrys_per_tasklet + (nr_cold_qrys_per_tasklet * TASK_DELETE_NR_TASKLETS != lo->nr_cold_qrys) + 7u) / 8u * 8u;
        const uint32_t idx_cold_qry_begin = cold_quota * me() < lo->nr_cold_qrys ? cold_quota * me() : lo->nr_cold_qrys,
                       idx_cold_qry_end = idx_cold_qry_begin + cold_quota < lo->nr_cold_qrys ? idx_cold_qry_begin + cold_quota : lo->nr_cold_qrys;

        const uint32_t nr_hot_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_DELETE_NR_TASKLETS(lo->nr_hot_qrys),
                       hot_quota = (nr_hot_qrys_per_tasklet + (nr_hot_qrys_per_tasklet * TASK_DELETE_NR_TASKLETS != lo->nr_hot_qrys) + 7u) / 8u * 8u;
        const uint32_t idx_hot_qry_begin = (hot_quota * me() < lo->nr_hot_qrys ? hot_quota * me() : lo->nr_hot_qrys) + lo->nr_cold_qrys,
                       idx_hot_qry_end = (idx_hot_qry_begin - lo->nr_cold_qrys) + hot_quota < lo->nr_hot_qrys
                                             ? idx_hot_qry_begin + hot_quota
                                             : lo->nr_hot_qrys + lo->nr_cold_qrys;

        DELETE_claim_pass(&cold_root, cold_height, cold_root_numKeys,
            idx_cold_qry_begin, idx_cold_qry_end, lo->cold_slots + sizeof(uintptr_t) * idx_cold_qry_begin);
        DELETE_claim_pass(&hot_root, hot_height, hot_root_numKeys,
            idx_hot_qry_begin, idx_hot_qry_end, lo->hot_slots + sizeof(uintptr_t) * (idx_hot_qry_begin - lo->nr_cold_qrys));

        DELETE_result_barrier();

        DELETE_report_pass(cold_height,
            idx_cold_qry_begin, idx_cold_qry_end, lo->cold_slots + sizeof(uintptr_t) * idx_cold_qry_begin,
            lo->cold_results + idx_cold_qry_begin);
        DELETE_report_pass(hot_height,
            idx_hot_qry_begin, idx_hot_qry_end, lo->hot_slots + sizeof(uintptr_t) * (idx_hot_qry_begin - lo->nr_cold_qrys),
            lo->hot_results + (idx_hot_qry_begin - lo->nr_cold_qrys));
    }

    // The result array is what fixes the order the queries are answered in, so
    // from here on the batch may be rearranged.
    DELETE_sort_barrier();
    DSORT_execute_in_parallel(&workspace.tree.delete.sort_top_digit[0], lo->qrys, lo->nr_cold_qrys, lo->sort_scratch);
    DSORT_execute_in_parallel(&workspace.tree.delete.sort_top_digit[1], lo->qrys + lo->nr_cold_qrys, lo->nr_hot_qrys, lo->sort_scratch);
}

OVERLAY_TASK_STATIC(OVL_SLOT_DELETE_TREE, DELETE_tree_phase, (void), ())
{
    if (me() >= TASK_DELETE_NR_TASKLETS) {
        return;
    }
    // The first half worked the layout out, and every tasklet went through a
    // barrier after it.
    const DeleteLayout* const lo = &workspace.tree.delete.layout;

    DELETE_execute_in_parallel(&workspace.tree.delete.sort_top_digit[0],
        &cold_root, &cold_height, &cold_root_numKeys, &nr_pairs.cold, lo->qrys, lo->nr_cold_qrys);
    DELETE_execute_in_parallel(&workspace.tree.delete.sort_top_digit[1],
        &hot_root, &hot_height, &hot_root_numKeys, &nr_pairs.hot, lo->qrys + lo->nr_cold_qrys, lo->nr_hot_qrys);

    if (me() == 0) {
        report_nr_pairs((__mram_ptr NrPairs*)lo->counts_result);
        DELETE_refresh_mins(lo->nr_refreshes[0], lo->nr_refreshes[1], lo->refresh_requests, lo->refresh_results);
    }
}
void task_delete(void)
{
    DELETE_result_phase();
    DELETE_tree_phase();
#ifdef TASK_DELETE_CHECK
    CHECK_trees();
#endif
}
#endif


#if SUPPORT_GET
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void GET_prepare_next_qry(key_uint64_t* qrys_cache, unsigned* idx_qry_in_cache, uintptr_t* cursor_on_qrys, uintptr_t* cursor_on_results)
{
    if (*idx_qry_in_cache == TASK_GET_NR_CACHED_QRYS) {
        mram_write(qrys_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_int64_t) * TASK_GET_NR_CACHED_QRYS);
        *cursor_on_results += sizeof(value_int64_t) * TASK_GET_NR_CACHED_QRYS;

        *cursor_on_qrys += sizeof(key_uint64_t) * TASK_GET_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)*cursor_on_qrys, qrys_cache, sizeof(key_uint64_t) * TASK_GET_NR_CACHED_QRYS);
        *idx_qry_in_cache = 0;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void GET_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t results, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    static const uintptr_t qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

    GetWorkspace* const wks_me = loop_invariant(&workspace.tree.get[me()]);

    unsigned idx_qry = idx_qry_begin, idx_qry_in_cache = 0;
    uintptr_t cursor_on_qrys = qrys + sizeof(key_uint64_t) * idx_qry,
              cursor_on_results = results + sizeof(value_int64_t) * idx_qry;
    mram_read((__mram_ptr void*)(cursor_on_qrys), &wks_me->qrys[0], sizeof(key_uint64_t) * TASK_GET_NR_CACHED_QRYS);

    if (height == 0) {
        for (; idx_qry < idx_qry_end; idx_qry++) {
            GET_prepare_next_qry(&wks_me->qrys[0], &idx_qry_in_cache, &cursor_on_qrys, &cursor_on_results);
            const key_uint64_t key = wks_me->qrys[idx_qry_in_cache];

            const uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, key);
            if (idx_pair < root_numKeys && root->lf.keys[idx_pair] == key) {
                wks_me->qrys[idx_qry_in_cache] = NthValue(root->lf, idx_pair);
            } else {
                wks_me->qrys[idx_qry_in_cache] = 0;  // not found
            }
            idx_qry_in_cache++;
        }

    } else {
        for (; idx_qry < idx_qry_end; idx_qry++) {
            GET_prepare_next_qry(&wks_me->qrys[0], &idx_qry_in_cache, &cursor_on_qrys, &cursor_on_results);
            const key_uint64_t key = wks_me->qrys[idx_qry_in_cache];

            NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, key));
            for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
                fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
                const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, key);
                link = NthChild(wks_me->node_cache.inl, idx_child);
            }
            fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
            const uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, key);

            if (idx_pair < link.numKeys && wks_me->node_cache.lf.keys[idx_pair] == key) {
                wks_me->qrys[idx_qry_in_cache] = NthValue(wks_me->node_cache.lf, idx_pair);
            } else {
                wks_me->qrys[idx_qry_in_cache] = 0;  // not found
            }
            idx_qry_in_cache++;
        }
    }
    if (idx_qry_in_cache != 0) {
        mram_write(&wks_me->qrys[0], (__mram_ptr void*)cursor_on_results, sizeof(value_int64_t) * idx_qry_in_cache);
    }
}
OVERLAY_TASK(OVL_SLOT_QUERY, task_get, (void), ())
{
    if (me() < TASK_GET_NR_TASKLETS) {
        const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;
        const uintptr_t results = (uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset;

        const uint32_t nr_cold_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_GET_NR_TASKLETS(nr_cold_qrys),
                       nr_remainder_cold_qrys = nr_cold_qrys - nr_cold_qrys_per_tasklet * TASK_GET_NR_TASKLETS,
                       nr_cold_qrys_for_me = nr_cold_qrys_per_tasklet + (me() < nr_remainder_cold_qrys);
        const uint32_t idx_cold_qry_begin = nr_cold_qrys_per_tasklet * me() + (me() <= nr_remainder_cold_qrys ? me() : nr_remainder_cold_qrys),
                       idx_cold_qry_end = idx_cold_qry_begin + nr_cold_qrys_for_me;

        const uint32_t nr_hot_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_GET_NR_TASKLETS(nr_hot_qrys),
                       nr_remainder_hot_qrys = nr_hot_qrys - nr_hot_qrys_per_tasklet * TASK_GET_NR_TASKLETS,
                       nr_hot_qrys_for_me = nr_hot_qrys_per_tasklet + (me() < nr_remainder_hot_qrys);
        const uint32_t idx_hot_qry_begin = nr_hot_qrys_per_tasklet * me() + (me() <= nr_remainder_hot_qrys ? me() : nr_remainder_hot_qrys)
                                           + nr_cold_qrys,
                       idx_hot_qry_end = idx_hot_qry_begin + nr_hot_qrys_for_me;

        GET_execute(&cold_root, cold_height, cold_root_numKeys,
            results, idx_cold_qry_begin, idx_cold_qry_end);
        GET_execute(&hot_root, hot_height, hot_root_numKeys,
            results, idx_hot_qry_begin, idx_hot_qry_end);
    }
}
#endif /* if SUPPORT_GET */


#if SUPPORT_PRED
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static key_uint64_t PRED_pop_qry(PredWorkspace* wks)
{
    if (wks->idx_qry_in_cache == TASK_PRED_NR_CACHED_QRYS) {
        wks->cursor_on_qrys += sizeof(key_uint64_t) * TASK_PRED_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)wks->cursor_on_qrys, wks->qrys, sizeof(key_uint64_t) * TASK_PRED_NR_CACHED_QRYS);
        wks->idx_qry_in_cache = 0;
    }
    return wks->qrys[wks->idx_qry_in_cache++];
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void PRED_push_result(PredWorkspace* wks, KVPair result)
{
    wks->results[wks->idx_result_in_cache] = result;
    wks->idx_result_in_cache++;
    if (wks->idx_result_in_cache == TASK_PRED_NR_CACHED_RESULTS) {
        mram_write(wks->results, (__mram_ptr void*)wks->cursor_on_results, sizeof(KVPair) * TASK_PRED_NR_CACHED_RESULTS);
        wks->cursor_on_results += sizeof(KVPair) * TASK_PRED_NR_CACHED_RESULTS;
        wks->idx_result_in_cache = 0;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void PRED_flush_results_cache(PredWorkspace* wks)
{
    if (wks->idx_result_in_cache != 0) {
        mram_write(wks->results, (__mram_ptr void*)wks->cursor_on_results, sizeof(KVPair) * wks->idx_result_in_cache);
        wks->cursor_on_results += sizeof(KVPair) * wks->idx_result_in_cache;
        wks->idx_result_in_cache = 0;
    }
}
//! @brief Strict predecessor: the pair with the largest key < `key`.
//! Internal-node descent uses search_for_pair_index (= #{delim < key}), so we
//! always descend the rightmost child whose subtree minimum is < key, i.e. the
//! child that contains the predecessor.  Reaching a leaf whose keys are all
//! >= `key` therefore means the descent never left the leftmost child, and the
//! tree holds no smaller key.  Returns the sentinel {KEY_MIN, NOT_FOUND_VALUE}
//! then, which a correctly routed query never sees
//! (docs/dpu_task_signature.md, TASK_PRED).
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static KVPair PRED_search_one(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const key_uint64_t key)
{
    PredWorkspace* const wks_me = loop_invariant(&workspace.tree.pred[me()]);

    if (height == 0) {
        const uint16_t idx_first_ge = search_for_pair_index(&root->lf.keys[0], root_numKeys, key);
        if (idx_first_ge == 0) {
            return (KVPair){KEY_MIN, NOT_FOUND_VALUE};
        }
        return (KVPair){root->lf.keys[idx_first_ge - 1], NthValue(root->lf, idx_first_ge - 1)};
    }

    NodeLink link = NthChild(root->inl, search_for_pair_index(&root->inl.keys[0], root_numKeys, key));
    for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
        fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
        const uint16_t idx_child = search_for_pair_index(&wks_me->node_cache.inl.keys[0], link.numKeys, key);
        link = NthChild(wks_me->node_cache.inl, idx_child);
    }
    fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
    const uint16_t idx_first_ge = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, key);
    if (idx_first_ge == 0) {
        return (KVPair){KEY_MIN, NOT_FOUND_VALUE};
    }
    return (KVPair){wks_me->node_cache.lf.keys[idx_first_ge - 1], NthValue(wks_me->node_cache.lf, idx_first_ge - 1)};
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void PRED_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t results, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    static const uintptr_t qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

    PredWorkspace* const wks_me = loop_invariant(&workspace.tree.pred[me()]);

    wks_me->idx_qry_in_cache = 0;
    wks_me->cursor_on_qrys = qrys + sizeof(key_uint64_t) * idx_qry_begin;
    mram_read((__mram_ptr void*)wks_me->cursor_on_qrys, &wks_me->qrys[0], sizeof(key_uint64_t) * TASK_PRED_NR_CACHED_QRYS);

    wks_me->idx_result_in_cache = 0;
    wks_me->cursor_on_results = results + sizeof(KVPair) * idx_qry_begin;

    for (unsigned idx_qry = idx_qry_begin; idx_qry < idx_qry_end; idx_qry++) {
        const key_uint64_t key = PRED_pop_qry(wks_me);
        const KVPair result = PRED_search_one(root, height, root_numKeys, key);
        PRED_push_result(wks_me, result);
    }
    PRED_flush_results_cache(wks_me);
}
OVERLAY_TASK(OVL_SLOT_QUERY, task_pred, (void), ())
{
    if (me() < TASK_PRED_NR_TASKLETS) {
        const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;
        const uintptr_t results = (uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset;

        const uint32_t nr_cold_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_PRED_NR_TASKLETS(nr_cold_qrys),
                       nr_remainder_cold_qrys = nr_cold_qrys - nr_cold_qrys_per_tasklet * TASK_PRED_NR_TASKLETS,
                       nr_cold_qrys_for_me = nr_cold_qrys_per_tasklet + (me() < nr_remainder_cold_qrys);
        const uint32_t idx_cold_qry_begin = nr_cold_qrys_per_tasklet * me() + (me() <= nr_remainder_cold_qrys ? me() : nr_remainder_cold_qrys),
                       idx_cold_qry_end = idx_cold_qry_begin + nr_cold_qrys_for_me;

        const uint32_t nr_hot_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_PRED_NR_TASKLETS(nr_hot_qrys),
                       nr_remainder_hot_qrys = nr_hot_qrys - nr_hot_qrys_per_tasklet * TASK_PRED_NR_TASKLETS,
                       nr_hot_qrys_for_me = nr_hot_qrys_per_tasklet + (me() < nr_remainder_hot_qrys);
        const uint32_t idx_hot_qry_begin = nr_hot_qrys_per_tasklet * me() + (me() <= nr_remainder_hot_qrys ? me() : nr_remainder_hot_qrys)
                                           + nr_cold_qrys,
                       idx_hot_qry_end = idx_hot_qry_begin + nr_hot_qrys_for_me;

        PRED_execute(&cold_root, cold_height, cold_root_numKeys,
            results, idx_cold_qry_begin, idx_cold_qry_end);
        PRED_execute(&hot_root, hot_height, hot_root_numKeys,
            results, idx_hot_qry_begin, idx_hot_qry_end);
    }
}
#endif /* if SUPPORT_PRED */


#if SUPPORT_RANGE_MIN
static uint16_t search_for_lump_index(const uint16_t* lump_end_indices, uint16_t nr_lumps, uint16_t query)
{
    // candidate: [left_m1 + 1, right_m1 + 1)
    uint16_t left_m1 = UINT16_MAX, right_m1 = nr_lumps;
    while ((uint16_t)(left_m1 + UINT16_C(1)) < right_m1) {
        const uint16_t probe_m1 = (uint16_t)(left_m1 + right_m1) / 2;
        if (lump_end_indices[probe_m1] >= query) {
            right_m1 = probe_m1;
        } else {
            left_m1 = probe_m1;
        }
    }
    return right_m1;
}
static void RANGE_MIN_prepare_next_delim_key(key_uint64_t* delim_keys_cache, unsigned* idx_delim_in_cache, uintptr_t* cursor_on_delim_keys)
{
    if (*idx_delim_in_cache == TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS) {
        mram_read((__mram_ptr void*)*cursor_on_delim_keys, delim_keys_cache, sizeof(key_uint64_t) * TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS);
        *cursor_on_delim_keys += sizeof(key_uint64_t) * TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS;
        *idx_delim_in_cache = 0;
    }
}
static void RANGE_MIN_prepare_next_lump_end_index(uint16_t* lump_end_indices_cache, unsigned* idx_lump_in_cache, uintptr_t* cursor_on_lump_end_indices)
{
    if (*idx_lump_in_cache == TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES) {
        mram_read((__mram_ptr void*)*cursor_on_lump_end_indices, lump_end_indices_cache, sizeof(uint16_t) * TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES);
        *cursor_on_lump_end_indices += sizeof(uint16_t) * TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES;
        *idx_lump_in_cache = 0;
    }
}
static void RANGE_MIN_commit_next_result(value_int64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    if (*idx_result_in_cache == TASK_RANGE_MIN_NR_CACHED_RESULTS) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_int64_t) * TASK_RANGE_MIN_NR_CACHED_RESULTS);
        *cursor_on_results += sizeof(value_int64_t) * TASK_RANGE_MIN_NR_CACHED_RESULTS;
        *idx_result_in_cache = 0;
    }
}
static void RANGE_MIN_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t lump_end_indices, const uint16_t idx_lump_begin, const uint16_t idx_lump_end,
    const uintptr_t delim_keys, const uintptr_t results)
{
    TaskletLocalRMQWorkspace* const wks_me = loop_invariant(&workspace.tree.rmq.th[me()]);

    unsigned idx_lump = idx_lump_begin, idx_lump_in_cache,
             idx_delim, idx_delim_in_cache,
             /* idx_result,*/ idx_result_in_cache;
    uintptr_t cursor_on_lump_end_indices,
        cursor_on_delim_keys,
        cursor_on_results;

    cursor_on_lump_end_indices = (lump_end_indices + sizeof(uint16_t) * idx_lump_begin) / 8 * 8;
    idx_lump_in_cache = (lump_end_indices + sizeof(uint16_t) * idx_lump_begin) % 8 / sizeof(uint16_t);
    mram_read((__mram_ptr void*)(cursor_on_lump_end_indices),
        &wks_me->lump_end_indices[0], sizeof(uint16_t) * TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES);
    cursor_on_lump_end_indices += sizeof(uint16_t) * TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES;

    idx_delim = wks_me->lump_end_indices[idx_lump_in_cache];
    idx_lump_in_cache++;

    idx_delim_in_cache = TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS;
    cursor_on_delim_keys = delim_keys + sizeof(key_uint64_t) * idx_delim;

    idx_result_in_cache = 0;
    cursor_on_results = results + sizeof(value_int64_t) * (idx_delim - idx_lump);


    if (height == 0) {
        for (; idx_lump < idx_lump_end; idx_lump++) {
            RANGE_MIN_prepare_next_delim_key(&wks_me->delim_keys[0], &idx_delim_in_cache, &cursor_on_delim_keys);
            const key_uint64_t range_begin = wks_me->delim_keys[idx_delim_in_cache];
            idx_delim_in_cache++;

            uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, range_begin);

            RANGE_MIN_prepare_next_lump_end_index(&wks_me->lump_end_indices[0], &idx_lump_in_cache, &cursor_on_lump_end_indices);
            const uint16_t idx_delim_end = wks_me->lump_end_indices[idx_lump_in_cache];
            idx_lump_in_cache++;
            for (idx_delim++; idx_delim < idx_delim_end; idx_delim++) {

                RANGE_MIN_prepare_next_delim_key(&wks_me->delim_keys[0], &idx_delim_in_cache, &cursor_on_delim_keys);
                const key_uint64_t range_end = wks_me->delim_keys[idx_delim_in_cache];
                idx_delim_in_cache++;

                value_int64_t min = VALUE_MAX;
                for (; idx_pair < root_numKeys && root->lf.keys[idx_pair] <= range_end; idx_pair++) {
                    if (min > RevValues(root->lf)[-(int32_t)idx_pair]) {
                        min = RevValues(root->lf)[-(int32_t)idx_pair];
                    }
                }
                wks_me->results[idx_result_in_cache] = min;
                idx_result_in_cache++;
                RANGE_MIN_commit_next_result(&wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
            }
        }
    } else {
        for (; idx_lump < idx_lump_end; idx_lump++) {
            RANGE_MIN_prepare_next_delim_key(&wks_me->delim_keys[0], &idx_delim_in_cache, &cursor_on_delim_keys);
            const key_uint64_t range_begin = wks_me->delim_keys[idx_delim_in_cache];
            idx_delim_in_cache++;

            NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, range_begin));
            for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
                fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
                const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, range_begin);
                link = NthChild(wks_me->node_cache.inl, idx_child);
            }
            fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
            uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, range_begin);

            RANGE_MIN_prepare_next_lump_end_index(&wks_me->lump_end_indices[0], &idx_lump_in_cache, &cursor_on_lump_end_indices);
            const uint16_t idx_delim_end = wks_me->lump_end_indices[idx_lump_in_cache];
            idx_lump_in_cache++;
            for (idx_delim++; idx_delim < idx_delim_end; idx_delim++) {

                RANGE_MIN_prepare_next_delim_key(&wks_me->delim_keys[0], &idx_delim_in_cache, &cursor_on_delim_keys);
                const key_uint64_t range_end = wks_me->delim_keys[idx_delim_in_cache];
                idx_delim_in_cache++;

                value_int64_t min = VALUE_MAX;
                const value_int64_t* const rev_values = loop_invariant(RevValues(wks_me->node_cache.lf));
                const key_uint64_t* const cached_keys = loop_invariant(&wks_me->node_cache.lf.keys[0]);
                for (;;) {
                    for (; idx_pair < link.numKeys; idx_pair++) {
                        if (cached_keys[idx_pair] > range_end) {
                            goto end_of_range;
                        }
                        if (min > rev_values[-(int32_t)idx_pair]) {
                            min = rev_values[-(int32_t)idx_pair];
                        }
                    }
                    const NodeLink right_leaf = wks_me->node_cache.lf.right;
                    if (right_leaf.ptr == NODELINK_NULLPTR.ptr && right_leaf.numKeys == NODELINK_NULLPTR.numKeys) {
                        goto end_of_range;
                    }
                    link = right_leaf;
                    fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
                    idx_pair = 0;
                }
            end_of_range:
                wks_me->results[idx_result_in_cache] = min;
                idx_result_in_cache++;
                RANGE_MIN_commit_next_result(&wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
            }
        }
    }
    if (idx_result_in_cache != 0) {
        mram_write(&wks_me->results[0], (__mram_ptr void*)cursor_on_results, sizeof(value_int64_t) * idx_result_in_cache);
    }
}
void task_range_min(void)
{
    if (me() < TASK_RANGE_MIN_NR_TASKLETS) {
        const uint16_t nr_cold_lumps = input_header.rmq.nr_cold_lumps, nr_hot_lumps = input_header.rmq.nr_hot_lumps,
                       nr_lumps = nr_cold_lumps + nr_hot_lumps;
        assert(nr_cold_lumps + nr_hot_lumps <= MAX_NR_RMQ_LUMPS);
        static const uintptr_t lump_end_indices = (uintptr_t)DPU_MRAM_HEAP_POINTER + 8,
                               cold_lump_end_indices = lump_end_indices;
        const uintptr_t hot_lump_end_indices = cold_lump_end_indices + sizeof(uint16_t) * (nr_cold_lumps + 1);

        static RMQWorkspace* const wks = &workspace.tree.rmq;
        if (me() == 0) {
            uint32_t nr_bytes = (sizeof(uint16_t) * (nr_lumps + 2) + 7) / 8 * 8;
            uintptr_t mram_src = lump_end_indices, wram_dst = (uintptr_t)(&wks->lump_end_indices[0]);
            assert(wram_dst % 8 == 0);
            for (; nr_bytes > 2048; nr_bytes -= 2048, mram_src += 2048, wram_dst += 2048) {
                mram_read((__mram_ptr KeyRange*)mram_src, (KeyRange*)wram_dst, 2048);
            }
            mram_read((__mram_ptr KeyRange*)mram_src, (KeyRange*)wram_dst, nr_bytes);
        } else {
            wait_for_prev_ready();
        }
        if (me() != TASK_RANGE_MIN_NR_TASKLETS - 1) {
            notify_next_of_readiness();
        }

        const uint16_t nr_cold_delims = wks->lump_end_indices[nr_cold_lumps],
                       nr_hot_delims = wks->lump_end_indices[nr_cold_lumps + nr_hot_lumps + 1];
        const uint16_t nr_cold_results = nr_cold_delims - nr_cold_lumps;
        const uintptr_t cold_delim_keys = (hot_lump_end_indices + sizeof(uint16_t) * (nr_hot_lumps + 1) + 7) / 8 * 8,
                        hot_delim_keys = cold_delim_keys + sizeof(key_uint64_t) * nr_cold_delims;

        const uintptr_t cold_results = (uintptr_t)DPU_MRAM_HEAP_POINTER + RESULT_OFFSET,
                        hot_results = cold_results + sizeof(value_int64_t) * nr_cold_results;

        const uint16_t nr_cold_delims_per_tasklet = (uint16_t)DIV_NR_DELIMS_BY_TASK_RANGE_MIN_NR_TASKLETS(nr_cold_delims),
                       nr_remainder_cold_delims = nr_cold_delims - nr_cold_delims_per_tasklet * TASK_RANGE_MIN_NR_TASKLETS,
                       nr_cold_delims_for_me = nr_cold_delims_per_tasklet + (me() < nr_remainder_cold_delims);
        const uint16_t tmp_idx_cold_delim_begin = (uint16_t)(nr_cold_delims_per_tasklet * me() + (me() <= nr_remainder_cold_delims ? me() : nr_remainder_cold_delims)),
                       tmp_idx_cold_delim_end = tmp_idx_cold_delim_begin + nr_cold_delims_for_me;
        const uint16_t idx_cold_lump_begin = search_for_lump_index(&wks->lump_end_indices[0], nr_cold_lumps, tmp_idx_cold_delim_begin),
                       idx_cold_lump_end = search_for_lump_index(&wks->lump_end_indices[0], nr_cold_lumps, tmp_idx_cold_delim_end);

        const uint16_t nr_hot_delims_per_tasklet = (uint16_t)DIV_NR_DELIMS_BY_TASK_RANGE_MIN_NR_TASKLETS(nr_hot_delims),
                       nr_remainder_hot_delims = nr_hot_delims - nr_hot_delims_per_tasklet * TASK_RANGE_MIN_NR_TASKLETS,
                       nr_hot_delims_for_me = nr_hot_delims_per_tasklet + (me() < nr_remainder_hot_delims);
        const uint16_t tmp_idx_hot_delim_begin = (uint16_t)(nr_hot_delims_per_tasklet * me() + (me() <= nr_remainder_hot_delims ? me() : nr_remainder_hot_delims)),
                       tmp_idx_hot_delim_end = tmp_idx_hot_delim_begin + nr_hot_delims_for_me;
        const uint16_t idx_hot_lump_begin = search_for_lump_index(&wks->lump_end_indices[nr_cold_lumps + 1], nr_hot_lumps, tmp_idx_hot_delim_begin),
                       idx_hot_lump_end = search_for_lump_index(&wks->lump_end_indices[nr_cold_lumps + 1], nr_hot_lumps, tmp_idx_hot_delim_end);

        if (me() != TASK_RANGE_MIN_NR_TASKLETS - 1) {
            wait_for_next_ready();
        }
        if (me() != 0) {
            notify_prev_of_readiness();
            wait_for_prev_ready();
        }
        if (me() != TASK_RANGE_MIN_NR_TASKLETS - 1) {
            notify_next_of_readiness();
        }

        RANGE_MIN_execute(&cold_root, cold_height, cold_root_numKeys,
            cold_lump_end_indices, idx_cold_lump_begin, idx_cold_lump_end,
            cold_delim_keys, cold_results);
        RANGE_MIN_execute(&hot_root, hot_height, hot_root_numKeys,
            hot_lump_end_indices, idx_hot_lump_begin, idx_hot_lump_end,
            hot_delim_keys, hot_results);
    }
}
#endif /* if SUPPORT_RANGE_MIN */


#if SUPPORT_RANGE_COUNT
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static RangeCountQuery* RANGE_COUNT_pop_qry(RangeCountQuery* qrys_cache, unsigned* idx_qry_in_cache, uintptr_t* cursor_on_qrys)
{
    if (*idx_qry_in_cache == TASK_RANGE_COUNT_NR_CACHED_QRYS) {
        *cursor_on_qrys += sizeof(RangeCountQuery) * TASK_RANGE_COUNT_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)*cursor_on_qrys, qrys_cache, sizeof(RangeCountQuery) * TASK_RANGE_COUNT_NR_CACHED_QRYS);
        *idx_qry_in_cache = 0;
    }
    return &qrys_cache[(*idx_qry_in_cache)++];
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void RANGE_COUNT_push_result(uint64_t result, uint64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    results_cache[*idx_result_in_cache] = result;
    (*idx_result_in_cache)++;
    if (*idx_result_in_cache == TASK_RANGE_COUNT_NR_CACHED_RESULTS) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(uint64_t) * TASK_RANGE_COUNT_NR_CACHED_RESULTS);
        *cursor_on_results += sizeof(uint64_t) * TASK_RANGE_COUNT_NR_CACHED_RESULTS;
        *idx_result_in_cache = 0;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void RANGE_COUNT_flush_results_cache(uint64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    if (*idx_result_in_cache != 0) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(uint64_t) * *idx_result_in_cache);
        *cursor_on_results += sizeof(uint64_t) * *idx_result_in_cache;
        *idx_result_in_cache = 0;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static uint64_t RANGE_COUNT_impl(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const RangeCountQuery* const qry)
{
    RCQWorkspace* const wks_me = loop_invariant(&workspace.tree.rcq[me()]);
    // Hoisted out of the scan loops by hand: the fetches into node_cache may
    // alias *qry as far as the compiler knows, so it cannot hoist them itself.
    const key_uint64_t range_end = qry->range.end;
    const value_int64_t needle = qry->needle;

    uint64_t count = 0;

    if (height == 0) {
        uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, qry->range.begin);

        const value_int64_t* const rev_values = RevValues(root->lf);
        for (; idx_pair < root_numKeys && root->lf.keys[idx_pair] <= range_end; idx_pair++) {
            if (rev_values[-(int32_t)idx_pair] == needle) {
                count++;
            }
        }
        return count;

    } else {
        NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, qry->range.begin));
        for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
            fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
            const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, qry->range.begin);
            link = NthChild(wks_me->node_cache.inl, idx_child);
        }
        fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
        uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, qry->range.begin);

        const value_int64_t* const rev_values = loop_invariant(RevValues(wks_me->node_cache.lf));
        const key_uint64_t* const cached_keys = loop_invariant(&wks_me->node_cache.lf.keys[0]);
        for (;;) {
            for (; idx_pair < link.numKeys; idx_pair++) {
                if (cached_keys[idx_pair] > range_end) {
                    goto end_of_range;
                }
                if (rev_values[-(int32_t)idx_pair] == needle) {
                    count++;
                }
            }
            const NodeLink right_leaf = wks_me->node_cache.lf.right;
            if (right_leaf.ptr == NODELINK_NULLPTR.ptr && right_leaf.numKeys == NODELINK_NULLPTR.numKeys) {
                goto end_of_range;
            }
            link = right_leaf;
            fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
            idx_pair = 0;
        }
    end_of_range:
        return count;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void RANGE_COUNT_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t results, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    static const uintptr_t qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

    RCQWorkspace* const wks_me = loop_invariant(&workspace.tree.rcq[me()]);

    unsigned idx_qry = idx_qry_begin, idx_qry_in_cache = 0;
    uintptr_t cursor_on_qrys = qrys + sizeof(RangeCountQuery) * idx_qry;
    mram_read((__mram_ptr void*)(cursor_on_qrys), &wks_me->qrys[0], sizeof(RangeCountQuery) * TASK_RANGE_COUNT_NR_CACHED_QRYS);

    unsigned idx_result_in_cache = 0;
    uintptr_t cursor_on_results = results + sizeof(uint64_t) * idx_qry;

    for (; idx_qry < idx_qry_end; idx_qry++) {
        const RangeCountQuery* const qry = RANGE_COUNT_pop_qry(&wks_me->qrys[0], &idx_qry_in_cache, &cursor_on_qrys);
        const uint64_t count = RANGE_COUNT_impl(root, height, root_numKeys, qry);
        RANGE_COUNT_push_result(count, &wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
    }
    RANGE_COUNT_flush_results_cache(&wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
}
OVERLAY_TASK(OVL_SLOT_QUERY, task_range_count, (void), ())
{
    if (me() < TASK_RANGE_COUNT_NR_TASKLETS) {
        const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;
        const uintptr_t results = (uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset;

        const uint32_t nr_cold_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_RANGE_COUNT_NR_TASKLETS(nr_cold_qrys),
                       nr_remainder_cold_qrys = nr_cold_qrys - nr_cold_qrys_per_tasklet * TASK_RANGE_COUNT_NR_TASKLETS,
                       nr_cold_qrys_for_me = nr_cold_qrys_per_tasklet + (me() < nr_remainder_cold_qrys);
        const uint32_t idx_cold_qry_begin = nr_cold_qrys_per_tasklet * me() + (me() <= nr_remainder_cold_qrys ? me() : nr_remainder_cold_qrys),
                       idx_cold_qry_end = idx_cold_qry_begin + nr_cold_qrys_for_me;

        const uint32_t nr_hot_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_RANGE_COUNT_NR_TASKLETS(nr_hot_qrys),
                       nr_remainder_hot_qrys = nr_hot_qrys - nr_hot_qrys_per_tasklet * TASK_RANGE_COUNT_NR_TASKLETS,
                       nr_hot_qrys_for_me = nr_hot_qrys_per_tasklet + (me() < nr_remainder_hot_qrys);
        const uint32_t idx_hot_qry_begin = nr_hot_qrys_per_tasklet * me() + (me() <= nr_remainder_hot_qrys ? me() : nr_remainder_hot_qrys)
                                           + nr_cold_qrys,
                       idx_hot_qry_end = idx_hot_qry_begin + nr_hot_qrys_for_me;

        RANGE_COUNT_execute(&cold_root, cold_height, cold_root_numKeys,
            results, idx_cold_qry_begin, idx_cold_qry_end);
        RANGE_COUNT_execute(&hot_root, hot_height, hot_root_numKeys,
            results, idx_hot_qry_begin, idx_hot_qry_end);
    }
}
#endif /* if SUPPORT_RANGE_COUNT */


#if SUPPORT_RANGE_MAX
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static KeyRange* RANGE_MAX_pop_qry(KeyRange* qrys_cache, unsigned* idx_qry_in_cache, uintptr_t* cursor_on_qrys)
{
    if (*idx_qry_in_cache == TASK_RANGE_MAX_NR_CACHED_QRYS) {
        *cursor_on_qrys += sizeof(KeyRange) * TASK_RANGE_MAX_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)*cursor_on_qrys, qrys_cache, sizeof(KeyRange) * TASK_RANGE_MAX_NR_CACHED_QRYS);
        *idx_qry_in_cache = 0;
    }
    return &qrys_cache[(*idx_qry_in_cache)++];
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void RANGE_MAX_push_result(value_int64_t result, value_int64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    results_cache[*idx_result_in_cache] = result;
    (*idx_result_in_cache)++;
    if (*idx_result_in_cache == TASK_RANGE_MAX_NR_CACHED_RESULTS) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_int64_t) * TASK_RANGE_MAX_NR_CACHED_RESULTS);
        *cursor_on_results += sizeof(value_int64_t) * TASK_RANGE_MAX_NR_CACHED_RESULTS;
        *idx_result_in_cache = 0;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void RANGE_MAX_flush_results_cache(value_int64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    if (*idx_result_in_cache != 0) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_int64_t) * *idx_result_in_cache);
        *cursor_on_results += sizeof(value_int64_t) * *idx_result_in_cache;
        *idx_result_in_cache = 0;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static value_int64_t RANGE_MAX_impl(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const KeyRange* const qry)
{
    RMaxQWorkspace* const wks_me = loop_invariant(&workspace.tree.rmaxq[me()]);
    // Hoisted out of the scan loops by hand: the fetches into node_cache may
    // alias *qry as far as the compiler knows, so it cannot hoist it itself.
    const key_uint64_t range_end = qry->end;

    value_int64_t max = NOT_FOUND_VALUE;

    if (height == 0) {
        uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, qry->begin);

        const value_int64_t* const rev_values = RevValues(root->lf);
        for (; idx_pair < root_numKeys && root->lf.keys[idx_pair] <= range_end; idx_pair++) {
            if (rev_values[-(int32_t)idx_pair] > max) {
                max = rev_values[-(int32_t)idx_pair];
            }
        }
        return max;

    } else {
        NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, qry->begin));
        for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
            fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
            const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, qry->begin);
            link = NthChild(wks_me->node_cache.inl, idx_child);
        }
        fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
        uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, qry->begin);

        const value_int64_t* const rev_values = loop_invariant(RevValues(wks_me->node_cache.lf));
        const key_uint64_t* const cached_keys = loop_invariant(&wks_me->node_cache.lf.keys[0]);
        for (;;) {
            for (; idx_pair < link.numKeys; idx_pair++) {
                if (cached_keys[idx_pair] > range_end) {
                    goto end_of_range;
                }
                if (rev_values[-(int32_t)idx_pair] > max) {
                    max = rev_values[-(int32_t)idx_pair];
                }
            }
            const NodeLink right_leaf = wks_me->node_cache.lf.right;
            if (right_leaf.ptr == NODELINK_NULLPTR.ptr && right_leaf.numKeys == NODELINK_NULLPTR.numKeys) {
                goto end_of_range;
            }
            link = right_leaf;
            fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
            idx_pair = 0;
        }
    end_of_range:
        return max;
    }
}
OVERLAY_LOCAL(OVL_SLOT_QUERY)
static void RANGE_MAX_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t results, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    static const uintptr_t qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

    RMaxQWorkspace* const wks_me = loop_invariant(&workspace.tree.rmaxq[me()]);

    unsigned idx_qry = idx_qry_begin, idx_qry_in_cache = 0;
    uintptr_t cursor_on_qrys = qrys + sizeof(KeyRange) * idx_qry;
    mram_read((__mram_ptr void*)(cursor_on_qrys), &wks_me->qrys[0], sizeof(KeyRange) * TASK_RANGE_MAX_NR_CACHED_QRYS);

    unsigned idx_result_in_cache = 0;
    uintptr_t cursor_on_results = results + sizeof(value_int64_t) * idx_qry;

    for (; idx_qry < idx_qry_end; idx_qry++) {
        const KeyRange* const qry = RANGE_MAX_pop_qry(&wks_me->qrys[0], &idx_qry_in_cache, &cursor_on_qrys);
        const value_int64_t max = RANGE_MAX_impl(root, height, root_numKeys, qry);
        RANGE_MAX_push_result(max, &wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
    }
    RANGE_MAX_flush_results_cache(&wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
}
OVERLAY_TASK(OVL_SLOT_QUERY, task_range_max, (void), ())
{
    if (me() < TASK_RANGE_MAX_NR_TASKLETS) {
        const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;
        const uintptr_t results = (uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset;

        const uint32_t nr_cold_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_RANGE_MAX_NR_TASKLETS(nr_cold_qrys),
                       nr_remainder_cold_qrys = nr_cold_qrys - nr_cold_qrys_per_tasklet * TASK_RANGE_MAX_NR_TASKLETS,
                       nr_cold_qrys_for_me = nr_cold_qrys_per_tasklet + (me() < nr_remainder_cold_qrys);
        const uint32_t idx_cold_qry_begin = nr_cold_qrys_per_tasklet * me() + (me() <= nr_remainder_cold_qrys ? me() : nr_remainder_cold_qrys),
                       idx_cold_qry_end = idx_cold_qry_begin + nr_cold_qrys_for_me;

        const uint32_t nr_hot_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_RANGE_MAX_NR_TASKLETS(nr_hot_qrys),
                       nr_remainder_hot_qrys = nr_hot_qrys - nr_hot_qrys_per_tasklet * TASK_RANGE_MAX_NR_TASKLETS,
                       nr_hot_qrys_for_me = nr_hot_qrys_per_tasklet + (me() < nr_remainder_hot_qrys);
        const uint32_t idx_hot_qry_begin = nr_hot_qrys_per_tasklet * me() + (me() <= nr_remainder_hot_qrys ? me() : nr_remainder_hot_qrys)
                                           + nr_cold_qrys,
                       idx_hot_qry_end = idx_hot_qry_begin + nr_hot_qrys_for_me;

        RANGE_MAX_execute(&cold_root, cold_height, cold_root_numKeys,
            results, idx_cold_qry_begin, idx_cold_qry_end);
        RANGE_MAX_execute(&hot_root, hot_height, hot_root_numKeys,
            results, idx_hot_qry_begin, idx_hot_qry_end);
    }
}
#endif /* if SUPPORT_RANGE_MAX */


OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_init_pair_cache(SerializeWorkspace* wks, uintptr_t result_pairs)
{
    wks->nr_pairs = 0;
    wks->idx_pair_in_cache = 0;
    wks->cursor_on_pairs = result_pairs;
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static KVPair* SERIALIZE_prepare_pair_cache(SerializeWorkspace* wks)
{
    if (wks->idx_pair_in_cache == TASK_SERIALIZE_NR_CACHED_KVPAIRS) {
        mram_write(&wks->pairs[0], (__mram_ptr void*)wks->cursor_on_pairs, sizeof(KVPair) * TASK_SERIALIZE_NR_CACHED_KVPAIRS);
        wks->cursor_on_pairs += sizeof(KVPair) * TASK_SERIALIZE_NR_CACHED_KVPAIRS;
        wks->idx_pair_in_cache = 0;
    }
    wks->nr_pairs++;
    return &wks->pairs[wks->idx_pair_in_cache++];
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_flush_pair_cache(SerializeWorkspace* wks)
{
    if (wks->idx_pair_in_cache != 0) {
        mram_write(&wks->pairs[0], (__mram_ptr void*)wks->cursor_on_pairs, sizeof(KVPair) * wks->idx_pair_in_cache);
    }
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_init_delim_cache(SerializeWorkspace* wks, uint32_t nr_delims, uintptr_t delims)
{
    wks->nr_delims = nr_delims;
    wks->idx_delim_in_cache = TASK_SERIALIZE_NR_CACHED_DELIMS;
    wks->cursor_on_delims = delims;
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static key_uint64_t* SERIALIZE_fetch_next_delim(SerializeWorkspace* wks)
{
    if (wks->nr_delims == 0) {
        return NULL;
    }
    if (wks->idx_delim_in_cache == TASK_SERIALIZE_NR_CACHED_DELIMS) {
        mram_read((__mram_ptr void*)wks->cursor_on_delims, wks->delims, sizeof(key_uint64_t) * TASK_SERIALIZE_NR_CACHED_DELIMS);
        wks->cursor_on_delims += sizeof(key_uint64_t) * TASK_SERIALIZE_NR_CACHED_DELIMS;
        wks->idx_delim_in_cache = 0;
    }
    wks->nr_delims--;
    return &wks->delims[wks->idx_delim_in_cache++];
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_init_incision_cache(SerializeWorkspace* wks, uintptr_t result_incisions)
{
    wks->idx_incision_in_cache = 0;
    wks->cursor_on_incisions = result_incisions;
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static uint32_t* SERIALIZE_prepare_incision_cache(SerializeWorkspace* wks)
{
    if (wks->idx_incision_in_cache == TASK_SERIALIZE_NR_CACHED_INCISIONS) {
        mram_write(&wks->incisions[0], (__mram_ptr void*)wks->cursor_on_incisions, sizeof(uint32_t) * TASK_SERIALIZE_NR_CACHED_INCISIONS);
        wks->cursor_on_incisions += sizeof(uint32_t) * TASK_SERIALIZE_NR_CACHED_INCISIONS;
        wks->idx_incision_in_cache = 0;
    }
    return &wks->incisions[wks->idx_incision_in_cache++];
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_flush_incision_cache(SerializeWorkspace* wks)
{
    if (wks->idx_incision_in_cache != 0) {
        mram_write(&wks->incisions[0], (__mram_ptr void*)wks->cursor_on_incisions, sizeof(uint32_t) * ((wks->idx_incision_in_cache + 1) / 2 * 2));
    }
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_mark_incision(SerializeWorkspace* wks, key_uint64_t** p_delim)
{
    uint32_t* const incision = SERIALIZE_prepare_incision_cache(wks);
    *incision = wks->nr_pairs;
    *p_delim = SERIALIZE_fetch_next_delim(wks);
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void SERIALIZE_execute(uint8_t root_numKeys, const Node* root, uint8_t height,
    uintptr_t result_pairs,
    uint32_t nr_delims, uintptr_t delims,
    uintptr_t result_incisions)
{
    SerializeWorkspace* const wks = loop_invariant(&workspace.tree.serialize[me()]);
    SERIALIZE_init_pair_cache(wks, result_pairs);
    SERIALIZE_init_delim_cache(wks, nr_delims, delims);
    SERIALIZE_init_incision_cache(wks, result_incisions);

    key_uint64_t* delim = SERIALIZE_fetch_next_delim(wks);

    if (height == 0) {
        const value_int64_t* const rev_values = RevValues(root->lf);
        for (uint8_t i = 0; i < root_numKeys; i++) {
            const key_uint64_t key = root->lf.keys[i];

            while (delim != NULL && *delim <= key) {
                SERIALIZE_mark_incision(wks, &delim);
            }

            KVPair* const pair = SERIALIZE_prepare_pair_cache(wks);
            *pair = (KVPair){key, rev_values[-(int32_t)i]};
        }
    } else {
        NodeLink cursor = NthChild(root->inl, 0);
        for (uint8_t height_of_parent = height; height_of_parent > 1; height_of_parent--) {
            // the physical pair {&NthChild(_, 1), &NthChild(_, 0)} holds children 1 and 0
            mram_read(&NthChild(Deref(cursor.ptr).inl, 1), &wks->children_cache[0], sizeof(NodeLink) * 2);
            cursor = wks->children_cache[1];
        }

        // serialize kvpairs in leaf nodes
        const value_int64_t* const rev_values = loop_invariant(RevValues(wks->leaf_cache));
        const key_uint64_t* const cached_keys = loop_invariant(&wks->leaf_cache.keys[0]);
        for (;;) {
            fetch_leaf_filled(&Deref(cursor.ptr).lf, &wks->leaf_cache, cursor.numKeys);

            for (uint8_t i = 0; i < cursor.numKeys; i++) {
                const key_uint64_t key = cached_keys[i];

                while (delim != NULL && *delim <= key) {
                    SERIALIZE_mark_incision(wks, &delim);
                }

                KVPair* const pair = SERIALIZE_prepare_pair_cache(wks);
                *pair = (KVPair){key, rev_values[-(int32_t)i]};
            }

            cursor = wks->leaf_cache.right;
            if (cursor.ptr == NODELINK_NULLPTR.ptr && cursor.numKeys == NODELINK_NULLPTR.numKeys) {
                break;
            }
        }
    }

    while (delim != NULL) {
        SERIALIZE_mark_incision(wks, &delim);
    }

    SERIALIZE_flush_pair_cache(wks);
    SERIALIZE_flush_incision_cache(wks);
}
OVERLAY_TASK(OVL_SLOT_RESHARD, task_serialize, (void), ())
{
    _Static_assert(TASK_SERIALIZE_NR_TASKLETS == 1, "TASK_SERIALIZE_NR_TASKLETS == 1");
    if (me() < TASK_SERIALIZE_NR_TASKLETS) {
        static const uintptr_t input_delims = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader), result_incisions = input_delims;
        const unsigned nr_delims = input_header.serialize.nr_delims,
                       max_nr_delims = input_header.serialize.max_nr_delims;
        const uintptr_t result_pairs = result_incisions + sizeof(key_uint64_t) * max_nr_delims;

        if (input_header.serialize.do_cold) {
            SERIALIZE_execute(cold_root_numKeys, &cold_root, cold_height,
                result_pairs, nr_delims, input_delims, result_incisions);
        }
        if (input_header.serialize.do_hot) {
            const uintptr_t hot_offset = input_header.serialize.do_cold ? sizeof(KVPair) * nr_pairs.cold : 0;
            SERIALIZE_execute(hot_root_numKeys, &hot_root, hot_height,
                result_pairs + hot_offset, 0, input_delims, result_incisions);
        }
    }
}


OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static void tree_clear(uint8_t* const p_root_numKeys, Node* const root, uint8_t* const p_height)
{
    static ClearTreeWorkspace* const wks = &workspace.tree.clear;

    _Static_assert(TREE_CLEAR_NR_TASKLETS == 1, "TREE_CLEAR_NR_TASKLETS == 1");
    if (me() < TREE_CLEAR_NR_TASKLETS) {
        const uint8_t root_numKeys = *p_root_numKeys, height = *p_height;

        if (height == 0) {
        } else if (height == 1) {
            for (uint8_t idx_leaf = 0; idx_leaf <= root_numKeys; idx_leaf++) {
                Free_node(NthChild(root->inl, idx_leaf).ptr);
            }
        } else {
            for (uint8_t idx_child = 0; idx_child <= root_numKeys; idx_child++) {
                uint8_t stack_height = 0;
                NodeLink cursor = NthChild(root->inl, idx_child);

                for (;;) {
                    for (; stack_height + 2 < height; stack_height++) {
                        ClearTreeStackElem* stack_new_elem = &wks->stack[stack_height];
                        stack_new_elem->node = cursor;
                        stack_new_elem->nr_visited_children = 0;
                        // the physical pair {&NthChild(_, 1), &NthChild(_, 0)} holds children 1 and 0
                        mram_read(&NthChild(Deref(cursor.ptr).inl, 1), &stack_new_elem->children_cache[0], sizeof(NodeLink) * 2);
                        cursor = stack_new_elem->children_cache[1];
                    }
                    // here, cursor points to a parent of a leaf

                    {
                        // all the occupied child slots, extended to the 8-byte boundary on the lower side
                        const unsigned nr_child_slots = ((cursor.numKeys + 1) + 1) / 2 * 2;
                        mram_read(&Deref(cursor.ptr).inl.children[MAX_NR_CHILDREN - nr_child_slots], &wks->children_cache[0], nr_child_slots * sizeof(NodeLink));
                        for (uint8_t idx_leaf = 0; idx_leaf <= cursor.numKeys; idx_leaf++) {
                            Free_node(wks->children_cache[nr_child_slots - 1 - idx_leaf].ptr);
                        }
                    }

                    for (;; stack_height--) {
                        Free_node(cursor.ptr);

                        if (stack_height == 0) {
                            goto finish_one_child_of_root;
                        }

                        ClearTreeStackElem* const stack_top = &wks->stack[stack_height - 1];
                        cursor = stack_top->node;
                        const uint8_t orig_nr_visited_children = stack_top->nr_visited_children;

                        if (orig_nr_visited_children != cursor.numKeys) {
                            stack_top->nr_visited_children = orig_nr_visited_children + 1;

                            // the cached physical pair holds children {2j+1, 2j}
                            if (orig_nr_visited_children % 2 == 0) {
                                cursor = stack_top->children_cache[0];
                            } else {
                                mram_read(&NthChild(Deref(cursor.ptr).inl, orig_nr_visited_children + 2), &stack_top->children_cache[0], sizeof(NodeLink) * 2);
                                cursor = stack_top->children_cache[1];
                            }
                            break;
                        }
                    }
                }
            finish_one_child_of_root:;
            }
        }

        // What is left is the root as a leaf with no pair in it.
        *p_root_numKeys = *p_height = 0;
#ifdef DEBUG_OCCUPANCY
        root->lf.numKeys = 0;
#endif
    }
}
OVERLAY_LOCAL(OVL_SLOT_RESHARD)
static NodePtr MOVE_HOT_allocator(unsigned idx_node)
{
    (void)idx_node;
    return Allocate_node();
}
OVERLAY_TASK_STATIC(OVL_SLOT_RESHARD, MOVE_HOT_clear_phase, (void), ())
{
    // Clearing a tree drops its pairs, so its live count goes with them: a
    // renewed tree that receives no pair is never visited again in this task.
    if (input_header.move_hot.renew_cold) {
        tree_clear(&cold_root_numKeys, &cold_root, &cold_height);
        if (me() == 0) {
            nr_pairs.cold = 0;
        }
    }
    if (input_header.move_hot.renew_hot) {
        tree_clear(&hot_root_numKeys, &hot_root, &hot_height);
        if (me() == 0) {
            nr_pairs.hot = 0;
        }
    }

    if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
        TREE_CONSTRUCT_barrier();
    }
}
OVERLAY_TASK_STATIC(OVL_SLOT_RESHARD, MOVE_HOT_construct_phase, (bool cold_tree), (cold_tree))
{
    if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
        static const uintptr_t cold_pairs = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

        if (cold_tree) {
            construct_tree(cold_pairs, input_header.move_hot.nr_cold_pairs,
                &cold_root_numKeys, &cold_root, &cold_height, &cold_min_key, &nr_pairs.cold, MOVE_HOT_allocator);
        } else {
            const uintptr_t hot_pairs = cold_pairs + sizeof(KVPair) * input_header.move_hot.nr_cold_pairs;

            construct_tree(hot_pairs, input_header.move_hot.nr_hot_pairs,
                &hot_root_numKeys, &hot_root, &hot_height, &hot_min_key, &nr_pairs.hot, MOVE_HOT_allocator);
        }
    }
}
OVERLAY_TASK_STATIC(OVL_SLOT_INSERT, MOVE_HOT_upsert_phase, (bool cold_tree), (cold_tree))
{
    // The pairs come from TASK_SERIALIZE, so they are in key order.
    if (me() == 0) {
        if (cold_tree) {
            INSERT_execute_batch_wram_root(&workspace.tree.insert.phys.th[me()], &cold_root, &cold_height, &cold_root_numKeys, &nr_pairs.cold,
                (__mram_ptr KVPair*)((uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader)), 0, input_header.move_hot.nr_cold_pairs);
        } else {
            INSERT_execute_batch_wram_root(&workspace.tree.insert.phys.th[me()], &hot_root, &hot_height, &hot_root_numKeys, &nr_pairs.hot,
                (__mram_ptr KVPair*)((uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader)), input_header.move_hot.nr_cold_pairs,
                input_header.move_hot.nr_cold_pairs + input_header.move_hot.nr_hot_pairs);
        }
    }
}
void task_move_hot(void)
{
    if (input_header.move_hot.renew_cold && input_header.move_hot.renew_hot) {
        return task_init();
    }

    if (input_header.move_hot.renew_cold || input_header.move_hot.renew_hot) {
        MOVE_HOT_clear_phase();
    }

    _Static_assert(TREE_CONSTRUCT_NR_TASKLETS > 0, "TREE_CONSTRUCT_NR_TASKLETS > 0");
    if (input_header.move_hot.nr_cold_pairs > 0) {
        if (input_header.move_hot.renew_cold) {
            MOVE_HOT_construct_phase(true);
        } else {
            MOVE_HOT_upsert_phase(true);
        }

        if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
            TREE_CONSTRUCT_barrier();
        }
    }

    if (input_header.move_hot.nr_hot_pairs > 0) {
        if (input_header.move_hot.renew_hot) {
            MOVE_HOT_construct_phase(false);
        } else {
            MOVE_HOT_upsert_phase(false);
        }
    }

#ifdef TASK_MOVE_HOT_CHECK
    CHECK_trees();
#endif
}


OVERLAY_LOCAL(OVL_SLOT_CHECK)
static bool checkLeaf(NodeLink link)
{
    bool success = true;

    __dma_aligned Node leaf;
    mram_read(&Deref(link.ptr), &leaf, sizeof(Node));

#ifdef DEBUG_OCCUPANCY
    if (leaf.lf.numKeys != link.numKeys) {
        success = false;
        printf("Node[%u].numKeys == %u != %u in the parent's link\n", link.ptr, leaf.lf.numKeys, link.numKeys);
    }
#endif
    if (link.numKeys < MIN_NR_PAIRS) {
        success = false;
        printf("Node[%u].numKeys == %u < %u pairs, and it is not the root\n", link.ptr, link.numKeys, (unsigned)MIN_NR_PAIRS);
    }
    const NodePtr left = leaf.lf.left;
    if (left != NODE_NULLPTR) {
        union {
            NodeLink link_from_left;
            __dma_aligned uint64_t aligner;
        } cache;
        mram_read(&Deref(left).lf.right, &cache, 8);
        if (cache.link_from_left.ptr != link.ptr) {
            success = false;
            printf("Node[%u].left->right.ptr == %u != %u\n", link.ptr, cache.link_from_left.ptr, link.ptr);
        }
        if (cache.link_from_left.numKeys != link.numKeys) {
            success = false;
            printf("Node[%u].left->right.numKeys == %u != %u\n", link.ptr, cache.link_from_left.numKeys, link.numKeys);
        }
    } else {
        printf("Node[%u].left == NULL\n", link.ptr);
    }
    const NodeLink right = leaf.lf.right;
    if (!(right.ptr == NODELINK_NULLPTR.ptr && right.numKeys == NODELINK_NULLPTR.numKeys)) {
        union {
            NodePtr ptr_from_right;
            __dma_aligned uint64_t aligner;
        } cache;
        mram_read(&Deref(right.ptr).lf.left, &cache, 8);
        if (cache.ptr_from_right != link.ptr) {
            success = false;
            printf("Node[%u].right->left == %u != %u\n", link.ptr, cache.ptr_from_right, link.ptr);
        }
    } else {
        printf("Node[%u].right == NULL\n", link.ptr);
    }
    for (unsigned i = 1; i < link.numKeys; i++) {
        if (leaf.lf.keys[i - 1] >= leaf.lf.keys[i]) {
            success = false;
            printf("Node[%u].keys[%u] == %lu >= %lu == Node[%u].keys[%u]\n", link.ptr, i - 1, leaf.lf.keys[i - 1], leaf.lf.keys[i], link.ptr, i);
        }
    }
    return success;
}

OVERLAY_LOCAL(OVL_SLOT_CHECK)
static bool checkInternal(NodeLink link, bool is_child_leaf)
{
    bool success = true;

    __dma_aligned Node internal;
    mram_read(&Deref(link.ptr), &internal, sizeof(Node));

#ifdef DEBUG_OCCUPANCY
    if (internal.inl.numKeys != link.numKeys) {
        success = false;
        printf("Node[%u].numKeys == %u != %u in the parent's link\n", link.ptr, internal.inl.numKeys, link.numKeys);
    }
#endif
    if (link.numKeys + 1 < MIN_NR_CHILDREN) {
        success = false;
        printf("Node[%u] has %u children, fewer than %u, and it is not the root\n", link.ptr, link.numKeys + 1, MIN_NR_CHILDREN);
    }
    for (unsigned i = 1; i < link.numKeys; i++) {
        if (internal.inl.keys[i - 1] >= internal.inl.keys[i]) {
            success = false;
            printf("Node[%u].keys[%u] == %lu >= %lu == Node[%u].keys[%u]\n", link.ptr, i - 1, internal.inl.keys[i - 1], internal.inl.keys[i], link.ptr, i);
        }
    }
    for (unsigned i = 0; i <= link.numKeys; i++) {
        const NodeLink child_link = NthChild(internal.inl, i);
        if (child_link.ptr == NODE_NULLPTR) {
            success = false;
            printf("NthChild(Node[%u], %u) == null\n", link.ptr, i);
        } else {
            __dma_aligned key_uint64_t key_in_child;
            if (i != 0) {
                mram_read((is_child_leaf ? &Deref(child_link.ptr).lf.keys[0]
                                         : &Deref(child_link.ptr).inl.keys[0]),
                    &key_in_child, sizeof(key_uint64_t));
                if (is_child_leaf) {
                    if (internal.inl.keys[i - 1] > key_in_child) {
                        success = false;
                        printf("Node[%u].keys[%u] > NthChild(Node[%u], %u)->keys[0]\n", link.ptr, i - 1, link.ptr, i);
                    }
                } else {
                    if (internal.inl.keys[i - 1] >= key_in_child) {
                        success = false;
                        printf("Node[%u].keys[%u] >= NthChild(Node[%u], %u)->keys[0]\n", link.ptr, i - 1, link.ptr, i);
                    }
                }
            }
            if (i != link.numKeys) {
                mram_read((is_child_leaf ? &Deref(child_link.ptr).lf.keys[child_link.numKeys - 1]
                                         : &Deref(child_link.ptr).inl.keys[child_link.numKeys - 1]),
                    &key_in_child, sizeof(key_uint64_t));
                if (internal.inl.keys[i] <= key_in_child) {
                    success = false;
                    printf("Node[%u].keys[%u] <= NthChild(Node[%u], %u)->keys[%u]\n", link.ptr, i, link.ptr, i, child_link.numKeys - 1);
                }
            }
        }
    }
    return success;
}

OVERLAY_LOCAL(OVL_SLOT_CHECK)
static bool check_tree_structure(const Node* root, unsigned height, unsigned root_numKeys)
{
    bool success = true;

    struct {
        NodeLink link;
        unsigned nr_visited_children;
    } stack[MAX_HEIGHT + 1];
    unsigned stack_height = 0;

    if (height == 0) {
#ifdef DEBUG_OCCUPANCY
        if (root->lf.numKeys != root_numKeys) {
            success = false;
            printf("Root.numKeys == %u != %u in the parent's link\n", root->lf.numKeys, root_numKeys);
        }
#endif
        for (unsigned i = 1; i < root_numKeys; i++) {
            if (root->lf.keys[i - 1] >= root->lf.keys[i]) {
                success = false;
                printf("Root.keys[%u] == %lu >= %lu == Root.keys[%u]\n", i - 1, root->lf.keys[i - 1], root->lf.keys[i], i);
            }
        }
    } else {
#ifdef DEBUG_OCCUPANCY
        if (root->inl.numKeys != root_numKeys) {
            success = false;
            printf("Root.numKeys == %u != %u in the parent's link\n", root->inl.numKeys, root_numKeys);
        }
#endif
        for (unsigned idx_child_of_root = 0; idx_child_of_root <= root_numKeys; idx_child_of_root++) {
            stack[0].link = NthChild(root->inl, idx_child_of_root);
            stack[0].nr_visited_children = 0;
            stack_height = 1;

            while (stack_height != 0) {
                if (stack_height == height) {  // leaf
                    success = (checkLeaf(stack[stack_height - 1].link) && success);
                    stack_height--;
                } else {  // internal
                    const unsigned nr_visited = stack[stack_height - 1].nr_visited_children;
                    if (nr_visited <= stack[stack_height - 1].link.numKeys) {
                        stack[stack_height - 1].nr_visited_children++;
                        {
                            __dma_aligned NodeLink pair[2];
                            mram_read(&NthChild(Deref(stack[stack_height - 1].link.ptr).inl, nr_visited / 2 * 2 + 1), &pair[0], sizeof(NodeLink) * 2);
                            stack[stack_height].link = pair[1 - nr_visited % 2];
                        }
                        stack[stack_height].nr_visited_children = 0;
                        stack_height++;
                    } else {
                        checkInternal(stack[stack_height - 1].link, stack_height + 1 == height);
                        stack_height--;
                    }
                }
            }
        }

        const bool is_child_of_root_leaf = (height == 1);
        for (unsigned i = 1; i < root_numKeys; i++) {
            if (root->inl.keys[i - 1] >= root->inl.keys[i]) {
                success = false;
                printf("Root.keys[%u] == %lu >= %lu == Root.keys[%u]\n", i - 1, root->inl.keys[i - 1], root->inl.keys[i], i);
            }
        }
        for (unsigned i = 0; i <= root_numKeys; i++) {
            const NodeLink child_link = NthChild(root->inl, i);
            if (child_link.ptr == NODE_NULLPTR) {
                success = false;
                printf("NthChild(Root, %u) == null\n", i);
            } else {
                __dma_aligned key_uint64_t key_in_child;
                if (i != 0) {
                    mram_read((is_child_of_root_leaf ? &Deref(child_link.ptr).lf.keys[0]
                                                     : &Deref(child_link.ptr).inl.keys[0]),
                        &key_in_child, sizeof(key_uint64_t));
                    if (is_child_of_root_leaf) {
                        if (root->inl.keys[i - 1] > key_in_child) {
                            success = false;
                            printf("Root.keys[%u] > NthChild(Root, %u)->keys[0]\n", i - 1, i);
                        }
                    } else {
                        if (root->inl.keys[i - 1] >= key_in_child) {
                            success = false;
                            printf("Root.keys[%u] >= NthChild(Root, %u)->keys[0]\n", i - 1, i);
                        }
                    }
                }
                if (i != root_numKeys) {
                    mram_read((is_child_of_root_leaf ? &Deref(child_link.ptr).lf.keys[child_link.numKeys - 1]
                                                     : &Deref(child_link.ptr).inl.keys[child_link.numKeys - 1]),
                        &key_in_child, sizeof(key_uint64_t));
                    if (root->inl.keys[i] <= key_in_child) {
                        success = false;
                        printf("Root.keys[%u] <= NthChild(Root, %u)->keys[%u]\n", i, i, child_link.numKeys - 1);
                    }
                }
            }
        }
    }
    return success;
}


#ifdef TASK_TREE_CHECK
//! @brief Checks both trees and stops the DPU if either of them is broken: the
//! DPU log is only read when a DPU faults, so a broken tree has to fault.
OVERLAY_TASK_STATIC(OVL_SLOT_CHECK, CHECK_trees, (void), ())
{
    // A build without the overlay loads nothing, so the wait is asked for here.
    barrier_wait(&tasklet_barrier);
    if (me() == 0) {
        const bool cold_ok = check_tree_structure(&cold_root, cold_height, cold_root_numKeys),
                   hot_ok = check_tree_structure(&hot_root, hot_height, hot_root_numKeys);
        assert(cold_ok && hot_ok);
        (void)cold_ok;
        (void)hot_ok;
    }
}
#endif
