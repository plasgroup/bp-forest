#include "bplustree.h"

#include "tree.h"

#include "allocator.h"
#include "bit_ops_macro.h"
#include "common.h"
#include "div_by_const.h"
#include "dpu_params.h"
#include "input_header.h"
#include "node_ptr.h"
#include "sync.h"
#include "workload_types.h"
#include "workspace.h"

#include <attributes.h>
#include <defs.h>
#include <mram.h>

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

#ifdef SUPPORT_DELETE
static DEFINE_DIV_BY(TASK_DELETE_NR_TASKLETS, 32, _NR_QRYS);
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
__dma_aligned struct {
    uint32_t cold, hot;
} nr_pairs;


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


static void fetch_leaf_filled(const __mram_ptr LeafNode* src, LeafNode* dst, unsigned numKeys)
{
    const uintptr_t begin = offsetof(LeafNode, values) + sizeof(value_uint64_t) * (MAX_NR_PAIRS - numKeys),
                    end = offsetof(LeafNode, keys) + sizeof(key_uint64_t) * numKeys;
    mram_read((const __mram_ptr void*)((uintptr_t)src + begin), (void*)((uintptr_t)dst + begin), end - begin);
}

// The start is rounded down to the 8-byte DMA granularity.
static void fetch_internal_filled(const __mram_ptr InternalNode* src, InternalNode* dst, unsigned numKeys)
{
    const uintptr_t begin = (offsetof(InternalNode, children) + sizeof(NodeLink) * (MAX_NR_CHILDREN - (numKeys + 1))) & ~(uintptr_t)7,
                    end = offsetof(InternalNode, keys) + sizeof(key_uint64_t) * numKeys;
    mram_read((const __mram_ptr void*)((uintptr_t)src + begin), (void*)((uintptr_t)dst + begin), end - begin);
}


// Optimization barrier for loop-invariant addresses: at every -O level the
// compiler rematerializes the me()-scaled workspace address at each use in
// the query loops instead of keeping it in a register.  The empty asm makes
// the value opaque, so later uses must keep (or spill) it, not recompute it.
static inline void* loop_invariant(void* p)
{
    __asm__("" : "+r"(p));
    return p;
}


__attribute__((unused)) static bool check_tree_structure(const Node* root, unsigned height, unsigned root_numKeys);


//! @sa /docs/tree_initialization.md
//! @return Sum of nr. of KV pairs that [0, idx_leaf)-th leaves have
static unsigned TREE_CONSTRUCT_idx_leaf_to_idx_pair(unsigned idx_leaf, unsigned nr_leaves, bool is_2nd_last_leaf_not_full, unsigned nr_pairs)
{
    return (idx_leaf + is_2nd_last_leaf_not_full < nr_leaves ? idx_leaf * MAX_NR_PAIRS
                                                             : (idx_leaf == nr_leaves ? nr_pairs
                                                                                      : nr_pairs - MIN_NR_PAIRS));
}
//! @sa /docs/tree_initialization.md
//! @return Sum of nr. of children that [0, idx_parent)-th parents have
static unsigned TREE_CONSTRUCT_idx_parent_to_idx_child(unsigned idx_parent, unsigned nr_parents, bool is_2nd_last_parent_not_full, unsigned nr_children)
{
    return (idx_parent + is_2nd_last_parent_not_full < nr_parents ? idx_parent * MAX_NR_CHILDREN
                                                                  : (idx_parent == nr_parents ? nr_children
                                                                                              : nr_children - MIN_NR_CHILDREN));
}
//! @sa /docs/tree_initialization.md
//! @return max{ i | TREE_CONSTRUCT_idx_parent_to_idx_child(i, nr_parents, _) <= idx_chlid }
static unsigned TREE_CONSTRUCT_idx_child_to_idx_parent(unsigned idx_child, unsigned nr_children, unsigned nr_parents)
{
    return (idx_child + MIN_NR_CHILDREN < nr_children ? DIV_NR_NODES_BY_MAX_NR_CHILDREN(idx_child)
                                                      : (idx_child < nr_children ? nr_parents - 1
                                                                                 : nr_parents));
}

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

    // Distribute the leaf initialization task among the tasklets
    const unsigned nr_leaves_per_tasklet = DIV_NR_NODES_BY_TREE_CONSTRUCT_NR_TASKLETS(nr_nodes),
                   nr_remainder_leaves = nr_nodes - nr_leaves_per_tasklet * TREE_CONSTRUCT_NR_TASKLETS,
                   nr_leaves_for_me = nr_leaves_per_tasklet + (me() < nr_remainder_leaves);
    unsigned idx_node_begin = nr_leaves_per_tasklet * me() + (me() <= nr_remainder_leaves ? me() : nr_remainder_leaves),
             idx_node_end = idx_node_begin + nr_leaves_for_me;

    // Correspondence between the distributed leaves and KV pairs
    const bool is_2nd_last_node_not_full = nr_nodes > 1u && MAX_NR_PAIRS * (nr_nodes - 1u) + MIN_NR_PAIRS > nr_pairs;
    unsigned idx_pair = TREE_CONSTRUCT_idx_leaf_to_idx_pair(idx_node_begin, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
    const uintptr_t pairs_for_me = initial_pairs + sizeof(KVPair) * idx_pair;
    mram_read((__mram_ptr KVPair*)pairs_for_me, &wks->in.pairs[0], sizeof(KVPair) * TREE_CONSTRUCT_NR_CACHED_KVPAIRS);
    if (me() == 0) {
        *min_key = wks->in.pairs[0].key;
        *p_nr_pairs = nr_pairs;
    }

    // Distribute the initialization task of 2nd layer among the tasklets
    unsigned nr_parents = DIV_NR_NODES_BY_MAX_NR_CHILDREN(nr_nodes + MAX_NR_CHILDREN - 1),
             idx_parent_begin = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_begin, nr_nodes, nr_parents),
             idx_parent_end = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_end, nr_nodes, nr_parents);

    // Correspondence between the distributed 2nd layer and leaves
    bool is_2nd_last_parent_not_full = nr_parents > 1u && MAX_NR_CHILDREN * (nr_parents - 1u) + MIN_NR_CHILDREN > nr_nodes;
    unsigned idx_node_begin_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_begin, nr_parents, is_2nd_last_parent_not_full, nr_nodes),
             idx_node_end_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_end, nr_parents, is_2nd_last_parent_not_full, nr_nodes);

    // Data move required to resolve the mismatch in the distribution of the leaf and 2nd layer.
    bool is_any_node_sent_from_junior_to_senior = idx_node_end_used_by_me < idx_node_begin;
    const unsigned idx_node_begin_sent_to_senior = (is_any_node_sent_from_junior_to_senior ? idx_node_begin : idx_node_end_used_by_me),
                   nr_nodes_not_sent = idx_node_begin_sent_to_senior - idx_node_begin;

    // Cursors on WRAM cache
    unsigned idx_pair_cache = 0;
    // To place (idx_node_begin_sent_to_senior)-th node in wks->out.lifted[0], where should (idx_node_begin)-th be placed?
    //     -> wks->out.lifted[idx_lift_cache_begin]
    unsigned idx_lift_cache_begin = DIV_NR_NODES_BY_TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT(nr_nodes_not_sent + TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT - 1u)
                                        * TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT
                                    - nr_nodes_not_sent,
             idx_lift_cache = idx_lift_cache_begin;

    // Place the information lifted to the 2nd layer in the place where the KV pairs were
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

            idx_node++;
            if (idx_node == idx_node_end) {
                node_cache->lf.right = NODELINK_NULLPTR;
                wks->last_leaf = left_node;
                if (node_cache != root) {
                    mram_write(node_cache, &Deref(link_to_this_node.ptr), sizeof(Node));
                }
                break;
            }

            idx_pair_end_for_this_node = TREE_CONSTRUCT_idx_leaf_to_idx_pair(idx_node + 1, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
            numKeys_in_this_node = idx_pair_end_for_this_node - idx_pair;
            link_to_this_node = (NodeLink){allocator(idx_node), numKeys_in_this_node};

            node_cache->lf.right = link_to_this_node;
            mram_write(node_cache, &Deref(left_node), sizeof(Node));
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

        // Remember about children
        const bool is_2nd_last_node_not_full = is_2nd_last_parent_not_full;
        const unsigned nr_children = nr_nodes,
                       idx_child_begin = idx_node_begin_used_by_me,
                       idx_child_end = idx_node_end_used_by_me,
                       idx_child_begin_from_me = idx_node_begin,
                       idx_child_end_from_me = idx_node_end;
        const bool is_any_child_sent_from_junior_to_senior = is_any_node_sent_from_junior_to_senior;

        // Distribution of the initialization task
        nr_nodes = nr_parents;
        idx_node_begin = idx_parent_begin;
        idx_node_end = idx_parent_end;

        // Distribute the initialization task of the next layer up among the tasklets
        nr_parents = DIV_NR_NODES_BY_MAX_NR_CHILDREN(nr_nodes + MAX_NR_CHILDREN - 1);
        idx_parent_begin = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_begin, nr_nodes, nr_parents);
        idx_parent_end = TREE_CONSTRUCT_idx_child_to_idx_parent(idx_node_end, nr_nodes, nr_parents);

        // Correspondence between the distributions of this layer and the next layer up
        is_2nd_last_parent_not_full = nr_parents > 1u && MAX_NR_CHILDREN * (nr_parents - 1u) + MIN_NR_CHILDREN > nr_nodes;
        idx_node_begin_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_begin, nr_parents, is_2nd_last_parent_not_full, nr_nodes);
        idx_node_end_used_by_me = TREE_CONSTRUCT_idx_parent_to_idx_child(idx_parent_end, nr_parents, is_2nd_last_parent_not_full, nr_nodes);

        // Data move required to resolve the mismatch in the distribution of this layer and the next layer up
        is_any_node_sent_from_junior_to_senior = idx_node_end_used_by_me < idx_node_begin;
        idx_lift_cache = 0;  // reset as default

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
                // Correspondence between the distributed nodes and children
                unsigned idx_child = idx_child_begin_from_me;

                // Data move required to resolve the mismatch in the distribution of this layer and the next layer up
                const unsigned idx_node_begin_sent_to_senior = (is_any_node_sent_from_junior_to_senior ? idx_node_begin : idx_node_end_used_by_me),
                               nr_nodes_not_sent = idx_node_begin_sent_to_senior - idx_node_begin;

                // Cursors on WRAM cache
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
                // Place the information lifted to the 2nd layer in the place where the KV pairs were
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
                    mram_write(node_cache, &Deref(link_to_this_node.ptr), sizeof(Node));
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

static NodePtr INIT_cold_allocator(unsigned idx_node)
{
    return idx_node;
}
unsigned node_idx_shift;
static NodePtr INIT_hot_allocator(unsigned idx_node)
{
    return idx_node + node_idx_shift;
}
void task_init(void)
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

#ifdef TASK_INIT_CHECK
        TREE_CONSTRUCT_barrier();
        if (me() == 0) {
            printf("check cold tree:\n");
            check_tree_structure(&cold_root, cold_height, cold_root_numKeys);
            printf("check hot tree:\n");
            check_tree_structure(&hot_root, hot_height, hot_root_numKeys);
        }
#endif
    }
}


static KVPair* INSERT_fetch_next_qry(InsertWorkspace* wks)
{
    if (wks->idx_qry_in_cache == TASK_INSERT_NR_CACHED_QRYS) {
        mram_read((__mram_ptr void*)wks->cursor_on_qrys, wks->qrys, sizeof(KVPair) * TASK_INSERT_NR_CACHED_QRYS);
        wks->cursor_on_qrys += sizeof(KVPair) * TASK_INSERT_NR_CACHED_QRYS;
        wks->idx_qry_in_cache = 0;
    }
    return &wks->qrys[wks->idx_qry_in_cache++];
}
static bool /* inserted? */ INSERT_execute(Node* const root, uint8_t* const height, uint8_t* const root_numKeys, const KVPair* const qry)
{
    InsertWorkspace* const wks_me = loop_invariant(&workspace.tree.insert[me()]);

    if (*height == 0) {
        // Insert into the leaf
        const uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], *root_numKeys, qry->key);
        if (idx_pair < *root_numKeys && root->lf.keys[idx_pair] == qry->key) {
            NthValue(root->lf, idx_pair) = qry->value;  // update
            return false;
        } else {
            if (*root_numKeys < MAX_NR_PAIRS) {
                // No split
                for (unsigned i = *root_numKeys; i > idx_pair; i--) {
                    root->lf.keys[i] = root->lf.keys[i - 1];
                    NthValue(root->lf, i) = NthValue(root->lf, i - 1);
                }
                root->lf.keys[idx_pair] = qry->key;
                NthValue(root->lf, idx_pair) = qry->value;
                (*root_numKeys)++;
#ifdef DEBUG_OCCUPANCY
                root->lf.numKeys++;
#endif
            } else {
                // Split
                Node* const new_sibling = &wks_me->node_cache[0];
                const NodePtr old_root_ptr = Allocate_node(), new_sibling_ptr = Allocate_node();
                const unsigned old_root_numKeys = MAX_NR_PAIRS - MIN_NR_PAIRS + 1, new_sibling_numKeys = MIN_NR_PAIRS;
                const NodeLink old_root_link = {old_root_ptr, old_root_numKeys}, new_sibling_link = {new_sibling_ptr, new_sibling_numKeys};

                root->lf.right = new_sibling_link;
                new_sibling->lf.right = NODELINK_NULLPTR;
                new_sibling->lf.left = old_root_ptr;
#ifdef DEBUG_OCCUPANCY
                root->lf.numKeys = old_root_numKeys;
                new_sibling->lf.numKeys = new_sibling_numKeys;
#endif

                if (idx_pair <= old_root_numKeys - 1) {
                    // Move the last MIN_NR_PAIRS pairs to the new sibling
                    for (unsigned i = 0; i < new_sibling_numKeys; i++) {
                        new_sibling->lf.keys[i] = root->lf.keys[i + old_root_numKeys - 1];
                        NthValue(new_sibling->lf, i) = NthValue(root->lf, i + old_root_numKeys - 1);
                    }
                    // Insert the new pair into the old root
                    for (unsigned i = old_root_numKeys - 1; i > idx_pair; i--) {
                        root->lf.keys[i] = root->lf.keys[i - 1];
                        NthValue(root->lf, i) = NthValue(root->lf, i - 1);
                    }
                    root->lf.keys[idx_pair] = qry->key;
                    NthValue(root->lf, idx_pair) = qry->value;
                } else {
                    // Move the last MIN_NR_PAIRS-1 pairs and the new pair to the new sibling
                    for (unsigned i = old_root_numKeys; i < idx_pair; i++) {
                        new_sibling->lf.keys[i - old_root_numKeys] = root->lf.keys[i];
                        NthValue(new_sibling->lf, i - old_root_numKeys) = NthValue(root->lf, i);
                    }
                    new_sibling->lf.keys[idx_pair - old_root_numKeys] = qry->key;
                    NthValue(new_sibling->lf, idx_pair - old_root_numKeys) = qry->value;
                    for (unsigned i = idx_pair; i < new_sibling_numKeys; i++) {
                        new_sibling->lf.keys[i - (old_root_numKeys - 1)] = root->lf.keys[i];
                        NthValue(new_sibling->lf, i - (old_root_numKeys - 1)) = NthValue(root->lf, i);
                    }
                }
                mram_write(root, &Deref(old_root_ptr), sizeof(Node));
                mram_write(new_sibling, &Deref(new_sibling_ptr), sizeof(Node));
                // Create a new root
                root->inl.keys[0] = new_sibling->lf.keys[0];
                NthChild(root->inl, 0) = old_root_link;
                NthChild(root->inl, 1) = new_sibling_link;
#ifdef DEBUG_OCCUPANCY
                root->inl.numKeys = 1;
#endif
                *root_numKeys = 1;
                *height = 1;
            }

            return true;
        }

    } else {
        bool idx_unused_cache = 0, is_cache_dirty = false;

        NodePtr parent_ptr = /* parent of root, or root */ NODE_NULLPTR;
        uint16_t idx_node = 0;
        // WRAM copy, in logical order, of the physical pair of children slots
        // that contains the link to the current node.  Keeping it logical and
        // reversing only at the write-back sites compiles shorter than holding
        // it in physical order.
        NodeLink link_to_node[2] = /* root */ {NODELINK_NULLPTR, NODELINK_NULLPTR};

        uint8_t node_height = *height;
        uint16_t idx_child = search_for_child_index(&root->inl.keys[0], *root_numKeys, qry->key);
        __dma_aligned NodeLink child_link = NthChild(root->inl, idx_child);


        if (*root_numKeys == MAX_NR_CHILDREN - 1) {
            // Split the root
            _Static_assert(MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN, "MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN");
            const bool old_root_cache = idx_unused_cache, new_sibling_cache = !idx_unused_cache;
            Node *const old_root = &wks_me->node_cache[old_root_cache], *const new_sibling = &wks_me->node_cache[new_sibling_cache];
            const NodePtr old_root_ptr = Allocate_node(), new_sibling_ptr = Allocate_node();
            const unsigned old_root_numKeys = MAX_NR_CHILDREN - MIN_NR_CHILDREN - 1, new_sibling_numKeys = MIN_NR_CHILDREN - 1;
            const NodeLink old_root_link = {old_root_ptr, old_root_numKeys}, new_sibling_link = {new_sibling_ptr, new_sibling_numKeys};

            NthChild(new_sibling->inl, 0) = NthChild(root->inl, old_root_numKeys + 1);
            for (unsigned i = 1; i < new_sibling_numKeys + 1; i++) {
                new_sibling->inl.keys[i - 1] = root->inl.keys[i - 1 + (old_root_numKeys + 1)];
                NthChild(new_sibling->inl, i) = NthChild(root->inl, i + old_root_numKeys + 1);
            }
#ifdef DEBUG_OCCUPANCY
            new_sibling->inl.numKeys = new_sibling_numKeys;
#endif

            if (idx_child < old_root_numKeys + 1) {
                mram_write(new_sibling, &Deref(new_sibling_ptr), sizeof(Node));
                idx_unused_cache = new_sibling_cache;

                // Move the old root to the cache
                for (unsigned i = 0; i < old_root_numKeys; i++) {
                    old_root->inl.keys[i] = root->inl.keys[i];
                    NthChild(old_root->inl, i) = NthChild(root->inl, i);
                }
                NthChild(old_root->inl, old_root_numKeys) = NthChild(root->inl, old_root_numKeys);
#ifdef DEBUG_OCCUPANCY
                old_root->inl.numKeys = old_root_numKeys;
#endif

                // Continue to the appropriate child
                idx_node = 0;

            } else {
#ifdef DEBUG_OCCUPANCY
                root->inl.numKeys = old_root_numKeys;
#endif
                mram_write(root, &Deref(old_root_ptr), sizeof(Node));
                idx_unused_cache = old_root_cache;

                // Continue to the appropriate child
                idx_node = 1;
                idx_child -= old_root_numKeys + 1;
            }
            is_cache_dirty = true;

            // Create a new root
            root->inl.keys[0] = root->inl.keys[old_root_numKeys];
            link_to_node[0] = NthChild(root->inl, 0) = old_root_link;
            link_to_node[1] = NthChild(root->inl, 1) = new_sibling_link;
#ifdef DEBUG_OCCUPANCY
            root->inl.numKeys = 1;
#endif
            *root_numKeys = 1;
            *height += 1;
        }

        for (; node_height > 1; node_height--) {
            const bool child_cache = idx_unused_cache;
            Node* const child = &wks_me->node_cache[child_cache];
            mram_read(&Deref(child_link.ptr), child, sizeof(Node));

            const uint16_t idx_grandchild = search_for_child_index(&child->inl.keys[0], child_link.numKeys, qry->key);
            const NodeLink grandchild_link = NthChild(child->inl, idx_grandchild);

            if (child_link.numKeys == MAX_NR_CHILDREN - 1) {
                // Split this internal node
                _Static_assert(MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN, "MIN_NR_CHILDREN * 2 <= MAX_NR_CHILDREN");
                const bool new_sibling_cache = !child_cache;
                const NodePtr new_sibling_ptr = Allocate_node();
                const unsigned new_child_numKeys = MAX_NR_CHILDREN - MIN_NR_CHILDREN - 1, new_sibling_numKeys = MIN_NR_CHILDREN - 1;
                const NodeLink new_child_link = {child_link.ptr, new_child_numKeys}, new_sibling_link = {new_sibling_ptr, new_sibling_numKeys};

                // Insert a new key and a new child link to the parent
                NodeLink* const node_link = &link_to_node[idx_node % 2];
                if (/* root */ (node_link->ptr == NODELINK_NULLPTR.ptr && node_link->numKeys == NODELINK_NULLPTR.numKeys)) {
                    for (unsigned i = *root_numKeys; i > idx_child; i--) {
                        root->inl.keys[i] = root->inl.keys[i - 1];
                        NthChild(root->inl, i + 1) = NthChild(root->inl, i);
                    }
                    root->inl.keys[idx_child] = child->inl.keys[new_child_numKeys];
                    NthChild(root->inl, idx_child) = new_child_link;
                    NthChild(root->inl, idx_child + 1) = new_sibling_link;
#ifdef DEBUG_OCCUPANCY
                    root->inl.numKeys += 1;
#endif
                    *root_numKeys += 1;

                    if (idx_grandchild >= new_child_numKeys + 1) {
                        idx_child += 1;
                    }
                    link_to_node[0] = NthChild(root->inl, idx_child / 2 * 2);
                    link_to_node[1] = NthChild(root->inl, idx_child / 2 * 2 + 1);

                } else {
                    Node* const node = &wks_me->node_cache[!child_cache];
                    for (unsigned i = node_link->numKeys; i > idx_child; i--) {
                        node->inl.keys[i] = node->inl.keys[i - 1];
                        NthChild(node->inl, i + 1) = NthChild(node->inl, i);
                    }
                    node->inl.keys[idx_child] = child->inl.keys[new_child_numKeys];
                    NthChild(node->inl, idx_child) = new_child_link;
                    NthChild(node->inl, idx_child + 1) = new_sibling_link;
#ifdef DEBUG_OCCUPANCY
                    node->inl.numKeys += 1;
#endif

                    node_link->numKeys += 1;
                    if (/* root */ parent_ptr == NODE_NULLPTR) {
                        NthChild(root->inl, idx_node) = *node_link;
                    } else {
                        __dma_aligned const NodeLink pair_in_phys_order[2] = {link_to_node[1], link_to_node[0]};
                        mram_write(&pair_in_phys_order[0], &NthChild(Deref(parent_ptr).inl, idx_node / 2 * 2 + 1), sizeof(NodeLink) * 2);
                    }

                    mram_write(node, &Deref(node_link->ptr), sizeof(Node));

                    parent_ptr = node_link->ptr;
                    if (idx_grandchild >= new_child_numKeys + 1) {
                        idx_child += 1;
                    }
                    link_to_node[0] = NthChild(node->inl, idx_child / 2 * 2);
                    link_to_node[1] = NthChild(node->inl, idx_child / 2 * 2 + 1);
                }
                idx_node = idx_child;

                // Move the appropriate half of children to the new sibling
                Node* const new_sibling = &wks_me->node_cache[new_sibling_cache];
                NthChild(new_sibling->inl, 0) = NthChild(child->inl, new_child_numKeys + 1);
                for (unsigned i = 1; i < new_sibling_numKeys + 1; i++) {
                    new_sibling->inl.keys[i - 1] = child->inl.keys[i + new_child_numKeys];
                    NthChild(new_sibling->inl, i) = NthChild(child->inl, i + new_child_numKeys + 1);
                }
#ifdef DEBUG_OCCUPANCY
                new_sibling->inl.numKeys = new_sibling_numKeys;
                child->inl.numKeys = new_child_numKeys;

                const uintptr_t offset = offsetof(InternalNode, numKeys) / 8 * 8;
                mram_write((const void*)((uintptr_t)child + offset), (__mram_ptr void*)((uintptr_t)&Deref(child_link.ptr) + offset), 8);
#endif

                mram_write(new_sibling, &Deref(new_sibling_ptr), sizeof(Node));
                if (idx_grandchild < new_child_numKeys + 1) {
                    idx_unused_cache = new_sibling_cache;

                    // Continue to the appropriate grandchild
                    idx_child = idx_grandchild;
                    is_cache_dirty = false;

                } else {
                    idx_unused_cache = child_cache;  // no change

                    // Continue to the appropriate grandchild
                    idx_child = idx_grandchild - (new_child_numKeys + 1);
                    is_cache_dirty = true;
                }

            } else {
                const NodeLink node_link = link_to_node[idx_node % 2];
                Node* const node = &wks_me->node_cache[!child_cache];
                if (is_cache_dirty) {
                    mram_write(node, &Deref(node_link.ptr), sizeof(Node));

                    is_cache_dirty = false;
                }
                idx_unused_cache = !child_cache;

                parent_ptr = node_link.ptr;
                if (/* root */ (node_link.ptr == NODELINK_NULLPTR.ptr && node_link.numKeys == NODELINK_NULLPTR.numKeys)) {
                    link_to_node[0] = NthChild(root->inl, idx_child / 2 * 2);
                    link_to_node[1] = NthChild(root->inl, idx_child / 2 * 2 + 1);
                } else {
                    link_to_node[0] = NthChild(node->inl, idx_child / 2 * 2);
                    link_to_node[1] = NthChild(node->inl, idx_child / 2 * 2 + 1);
                }
                idx_node = idx_child;

                idx_child = idx_grandchild;
            }

            child_link = grandchild_link;
        }

        // Insert into the leaf
        const bool leaf_cache = idx_unused_cache;
        Node* const leaf = &wks_me->node_cache[leaf_cache];
        mram_read(&Deref(child_link.ptr), leaf, sizeof(Node));

        NodeLink* const node_link = &link_to_node[idx_node % 2];
        Node* const node = &wks_me->node_cache[!leaf_cache];

        const uint16_t idx_pair = search_for_pair_index(&leaf->lf.keys[0], child_link.numKeys, qry->key);
        if (idx_pair < child_link.numKeys && leaf->lf.keys[idx_pair] == qry->key) {
            mram_write(&qry->value, &NthValue(Deref(child_link.ptr).lf, idx_pair), sizeof(value_uint64_t));  // update
            if (is_cache_dirty) {
                mram_write(node, &Deref(node_link->ptr), sizeof(Node));
            }
            return false;
        } else {
            if (child_link.numKeys < MAX_NR_PAIRS) {
                // No split
                for (unsigned i = child_link.numKeys; i > idx_pair; i--) {
                    leaf->lf.keys[i] = leaf->lf.keys[i - 1];
                    NthValue(leaf->lf, i) = NthValue(leaf->lf, i - 1);
                }
                leaf->lf.keys[idx_pair] = qry->key;
                NthValue(leaf->lf, idx_pair) = qry->value;
#ifdef DEBUG_OCCUPANCY
                leaf->lf.numKeys += 1;
#endif
                mram_write(leaf, &Deref(child_link.ptr), sizeof(Node));

                // Update the number of keys in the parent
                child_link.numKeys += 1;
                if (/* root */ (node_link->ptr == NODELINK_NULLPTR.ptr && node_link->numKeys == NODELINK_NULLPTR.numKeys)) {
                    NthChild(root->inl, idx_child) = child_link;
                } else {
                    NthChild(node->inl, idx_child) = child_link;
                    if (is_cache_dirty) {
                        mram_write(node, &Deref(node_link->ptr), sizeof(Node));
                    } else {
                        // both sides in physical order: the pair starts at its higher logical index
                        mram_write(&NthChild(node->inl, idx_child / 2 * 2 + 1),
                            &NthChild(Deref(node_link->ptr).inl, idx_child / 2 * 2 + 1),
                            sizeof(NodeLink) * 2);
                    }
                }

                // Update the number of keys in the predecessor leaf
                if (leaf->lf.left != NODE_NULLPTR) {
                    mram_write(&child_link, &Deref(leaf->lf.left).lf.right, 8);
                }

            } else {
                // Split
                const bool new_sibling_cache = !leaf_cache;
                const NodePtr new_sibling_ptr = Allocate_node();
                const unsigned new_leaf_numKeys = MAX_NR_PAIRS - MIN_NR_PAIRS + 1, new_sibling_numKeys = MIN_NR_PAIRS;
                __dma_aligned const NodeLink new_leaf_link = {child_link.ptr, new_leaf_numKeys}, new_sibling_link = {new_sibling_ptr, new_sibling_numKeys};
                const key_uint64_t new_sibling_min_key = idx_pair == new_leaf_numKeys  ? qry->key
                                                         : idx_pair < new_leaf_numKeys ? leaf->lf.keys[new_leaf_numKeys - 1]
                                                                                       : leaf->lf.keys[new_leaf_numKeys];

                // Insert a new key and a new child link to the parent
                if (/* root */ (node_link->ptr == NODELINK_NULLPTR.ptr && node_link->numKeys == NODELINK_NULLPTR.numKeys)) {
                    for (unsigned i = *root_numKeys; i > idx_child; i--) {
                        root->inl.keys[i] = root->inl.keys[i - 1];
                        NthChild(root->inl, i + 1) = NthChild(root->inl, i);
                    }
                    root->inl.keys[idx_child] = new_sibling_min_key;
                    NthChild(root->inl, idx_child) = new_leaf_link;
                    NthChild(root->inl, idx_child + 1) = new_sibling_link;
#ifdef DEBUG_OCCUPANCY
                    root->inl.numKeys += 1;
#endif
                    *root_numKeys += 1;

                } else {
                    Node* const node = &wks_me->node_cache[!leaf_cache];
                    for (unsigned i = node_link->numKeys; i > idx_child; i--) {
                        node->inl.keys[i] = node->inl.keys[i - 1];
                        NthChild(node->inl, i + 1) = NthChild(node->inl, i);
                    }
                    node->inl.keys[idx_child] = new_sibling_min_key;
                    NthChild(node->inl, idx_child) = new_leaf_link;
                    NthChild(node->inl, idx_child + 1) = new_sibling_link;
#ifdef DEBUG_OCCUPANCY
                    node->inl.numKeys += 1;
#endif

                    node_link->numKeys += 1;
                    if (/* root */ parent_ptr == NODE_NULLPTR) {
                        NthChild(root->inl, idx_node) = *node_link;
                    } else {
                        __dma_aligned const NodeLink pair_in_phys_order[2] = {link_to_node[1], link_to_node[0]};
                        mram_write(&pair_in_phys_order[0], &NthChild(Deref(parent_ptr).inl, idx_node / 2 * 2 + 1), sizeof(NodeLink) * 2);
                    }

                    mram_write(node, &Deref(node_link->ptr), sizeof(Node));
                }

                Node* const new_sibling = &wks_me->node_cache[new_sibling_cache];
                new_sibling->lf.right = leaf->lf.right;
                leaf->lf.right = new_sibling_link;
                new_sibling->lf.left = child_link.ptr;
#ifdef DEBUG_OCCUPANCY
                leaf->lf.numKeys = new_leaf_numKeys;
                new_sibling->lf.numKeys = new_sibling_numKeys;
#endif
                if (leaf->lf.left != NODE_NULLPTR) {
                    mram_write(&new_leaf_link, &Deref(leaf->lf.left).lf.right, 8);
                }
                if (!(new_sibling->lf.right.ptr == NODELINK_NULLPTR.ptr && new_sibling->lf.right.numKeys == NODELINK_NULLPTR.numKeys)) {
                    struct {
                        __dma_aligned NodePtr ptr;
                        unsigned numKeys;
                    } tmp;
                    tmp.ptr = new_sibling_ptr;
#ifdef DEBUG_OCCUPANCY
                    tmp.numKeys = new_sibling->lf.right.numKeys;
#endif
                    mram_write(&tmp, &Deref(new_sibling->lf.right.ptr).lf.left, 8);
                }

                if (idx_pair <= new_leaf_numKeys - 1) {
                    // Move the last MIN_NR_PAIRS pairs to the new sibling
                    for (unsigned i = 0; i < new_sibling_numKeys; i++) {
                        new_sibling->lf.keys[i] = leaf->lf.keys[i + new_leaf_numKeys - 1];
                        NthValue(new_sibling->lf, i) = NthValue(leaf->lf, i + new_leaf_numKeys - 1);
                    }
                    // Insert the new pair into the old root
                    for (unsigned i = new_leaf_numKeys - 1; i > idx_pair; i--) {
                        leaf->lf.keys[i] = leaf->lf.keys[i - 1];
                        NthValue(leaf->lf, i) = NthValue(leaf->lf, i - 1);
                    }
                    leaf->lf.keys[idx_pair] = qry->key;
                    NthValue(leaf->lf, idx_pair) = qry->value;
                } else {
                    // Move the last MIN_NR_PAIRS-1 pairs and the new pair to the new sibling
                    for (unsigned i = new_leaf_numKeys; i < idx_pair; i++) {
                        new_sibling->lf.keys[i - new_leaf_numKeys] = leaf->lf.keys[i];
                        NthValue(new_sibling->lf, i - new_leaf_numKeys) = NthValue(leaf->lf, i);
                    }
                    new_sibling->lf.keys[idx_pair - new_leaf_numKeys] = qry->key;
                    NthValue(new_sibling->lf, idx_pair - new_leaf_numKeys) = qry->value;
                    for (unsigned i = idx_pair; i < MAX_NR_PAIRS; i++) {
                        new_sibling->lf.keys[i - (new_leaf_numKeys - 1)] = leaf->lf.keys[i];
                        NthValue(new_sibling->lf, i - (new_leaf_numKeys - 1)) = NthValue(leaf->lf, i);
                    }
                }
                mram_write(leaf, &Deref(child_link.ptr), sizeof(Node));
                mram_write(new_sibling, &Deref(new_sibling_ptr), sizeof(Node));
            }

            return true;
        }
    }
}
static void INSERT_execute_batch(Node* const root, uint8_t* const height, uint8_t* const root_numKeys, uint32_t* const p_nr_pairs,
    const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    InsertWorkspace* const wks_me = loop_invariant(&workspace.tree.insert[me()]);
    wks_me->idx_qry_in_cache = TASK_INSERT_NR_CACHED_QRYS;  // to trigger the first fetch
    wks_me->cursor_on_qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader) + sizeof(KVPair) * idx_qry_begin;

    uint32_t tmp_nr_pairs = *p_nr_pairs;

    for (unsigned idx_qry = idx_qry_begin; idx_qry < idx_qry_end; idx_qry++) {
        const KVPair* const qry = INSERT_fetch_next_qry(wks_me);
        tmp_nr_pairs += INSERT_execute(root, height, root_numKeys, qry);
    }

    *p_nr_pairs = tmp_nr_pairs;
}
#if SUPPORT_INSERT
void task_insert(void)
{
    _Static_assert(TASK_INSERT_NR_TASKLETS == 1, "TASK_INSERT_NR_TASKLETS == 1");
    if (me() < TASK_INSERT_NR_TASKLETS) {
        const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;

        INSERT_execute_batch(&cold_root, &cold_height, &cold_root_numKeys, &nr_pairs.cold,
            0, nr_cold_qrys);
#ifdef TASK_INSERT_CHECK
        check_tree_structure(&cold_root, cold_height, cold_root_numKeys);
#endif

        INSERT_execute_batch(&hot_root, &hot_height, &hot_root_numKeys, &nr_pairs.hot,
            nr_cold_qrys, nr_cold_qrys + nr_hot_qrys);
#ifdef TASK_INSERT_CHECK
        check_tree_structure(&hot_root, hot_height, hot_root_numKeys);
#endif

        mram_write(&nr_pairs, (__mram_ptr void*)((uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset), sizeof(uint32_t[2]));
    }
}
#endif


#if SUPPORT_DELETE
__attribute__((unused)) static void DELETE_barrier(void)
{
    if (me() != 0) {
        wait_for_prev_ready();
    }
    if (me() != TASK_DELETE_NR_TASKLETS - 1) {
        notify_next_of_readiness();
        wait_for_next_ready();
    }
    if (me() != 0) {
        notify_prev_of_readiness();
    }
}
static key_uint64_t* DELETE_fetch_next_qry(DeleteWorkspace* wks)
{
    if (wks->idx_qry_in_cache == TASK_INSERT_NR_CACHED_QRYS) {
        mram_read((__mram_ptr void*)wks->cursor_on_qrys, wks->qrys, sizeof(key_uint64_t) * TASK_INSERT_NR_CACHED_QRYS);
        wks->cursor_on_qrys += sizeof(key_uint64_t) * TASK_INSERT_NR_CACHED_QRYS;
        wks->idx_qry_in_cache = 0;
    }
    return &wks->qrys[wks->idx_qry_in_cache++];
}
static uint32_t /* # of deleted pairs */ DELETE_execute(Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    DeleteWorkspace* const wks_me = loop_invariant(&workspace.tree.delete[me()]);
    wks_me->idx_qry_in_cache = TASK_INSERT_NR_CACHED_QRYS;  // to trigger the first fetch
    wks_me->cursor_on_qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader) + sizeof(key_uint64_t) * idx_qry_begin;

    uint32_t nr_deleted = 0;

    if (height == 0) {
        for (unsigned idx_qry = idx_qry_begin; idx_qry < idx_qry_end; idx_qry++) {
            const key_uint64_t key = *DELETE_fetch_next_qry(wks_me);

            const uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, key);
            if (idx_pair < root_numKeys && root->lf.keys[idx_pair] == key) {
                NthValue(root->lf, idx_pair) = NOT_FOUND_VALUE;  // mark as deleted
                nr_deleted++;
            }
        }

    } else {
        __dma_aligned const value_uint64_t tombstone = NOT_FOUND_VALUE;
        for (unsigned idx_qry = idx_qry_begin; idx_qry < idx_qry_end; idx_qry++) {
            const key_uint64_t key = *DELETE_fetch_next_qry(wks_me);

            NodeLink link = NthChild(root->inl, search_for_child_index(&root->inl.keys[0], root_numKeys, key));
            for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
                fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
                const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, key);
                link = NthChild(wks_me->node_cache.inl, idx_child);
            }
            fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
            const uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, key);

            if (idx_pair < link.numKeys && wks_me->node_cache.lf.keys[idx_pair] == key) {
                mram_write(&tombstone, &NthValue(Deref(link.ptr).lf, idx_pair), sizeof(value_uint64_t));
                nr_deleted++;
            }
        }
    }

    return nr_deleted;
}
void task_delete(void)
{
    if (me() < TASK_DELETE_NR_TASKLETS) {
        const uint32_t nr_cold_qrys = input_header.qrys.nr_cold_qrys, nr_hot_qrys = input_header.qrys.nr_hot_qrys;

        const uint32_t nr_cold_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_DELETE_NR_TASKLETS(nr_cold_qrys),
                       nr_remainder_cold_qrys = nr_cold_qrys - nr_cold_qrys_per_tasklet * TASK_DELETE_NR_TASKLETS,
                       nr_cold_qrys_for_me = nr_cold_qrys_per_tasklet + (me() < nr_remainder_cold_qrys);
        const uint32_t idx_cold_qry_begin = nr_cold_qrys_per_tasklet * me() + (me() <= nr_remainder_cold_qrys ? me() : nr_remainder_cold_qrys),
                       idx_cold_qry_end = idx_cold_qry_begin + nr_cold_qrys_for_me;

        const uint32_t nr_hot_qrys_per_tasklet = DIV_NR_QRYS_BY_TASK_DELETE_NR_TASKLETS(nr_hot_qrys),
                       nr_remainder_hot_qrys = nr_hot_qrys - nr_hot_qrys_per_tasklet * TASK_DELETE_NR_TASKLETS,
                       nr_hot_qrys_for_me = nr_hot_qrys_per_tasklet + (me() < nr_remainder_hot_qrys);
        const uint32_t idx_hot_qry_begin = nr_hot_qrys_per_tasklet * me() + (me() <= nr_remainder_hot_qrys ? me() : nr_remainder_hot_qrys)
                                           + nr_cold_qrys,
                       idx_hot_qry_end = idx_hot_qry_begin + nr_hot_qrys_for_me;

        const uint32_t nr_deleted_cold = DELETE_execute(&cold_root, cold_height, cold_root_numKeys,
            idx_cold_qry_begin, idx_cold_qry_end);
#ifdef TASK_DELETE_CHECK
        DELETE_barrier();
        if (me() == 0) {
            check_tree_structure(&cold_root, cold_height, cold_root_numKeys);
        }
#endif

        const uint32_t nr_deleted_hot = DELETE_execute(&hot_root, hot_height, hot_root_numKeys,
            idx_hot_qry_begin, idx_hot_qry_end);
#ifdef TASK_DELETE_CHECK
        DELETE_barrier();
        if (me() == 0) {
            check_tree_structure(&hot_root, hot_height, hot_root_numKeys);
        }
#endif

        if (me() != 0) {
            wait_for_prev_ready();
        }
        nr_pairs.cold -= nr_deleted_cold;
        nr_pairs.hot -= nr_deleted_hot;
        if (me() != TASK_DELETE_NR_TASKLETS - 1) {
            notify_next_of_readiness();
        } else {
            mram_write(&nr_pairs, (__mram_ptr void*)((uintptr_t)DPU_MRAM_HEAP_POINTER + input_header.qrys.result_offset), sizeof(uint32_t[2]));
        }
    }
}
#endif


#if SUPPORT_GET
static void GET_prepare_next_qry(key_uint64_t* qrys_cache, unsigned* idx_qry_in_cache, uintptr_t* cursor_on_qrys, uintptr_t* cursor_on_results)
{
    if (*idx_qry_in_cache == TASK_GET_NR_CACHED_QRYS) {
        mram_write(qrys_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_uint64_t) * TASK_GET_NR_CACHED_QRYS);
        *cursor_on_results += sizeof(value_uint64_t) * TASK_GET_NR_CACHED_QRYS;

        *cursor_on_qrys += sizeof(key_uint64_t) * TASK_GET_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)*cursor_on_qrys, qrys_cache, sizeof(key_uint64_t) * TASK_GET_NR_CACHED_QRYS);
        *idx_qry_in_cache = 0;
    }
}
static void GET_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t results, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    static const uintptr_t qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

    GetWorkspace* const wks_me = loop_invariant(&workspace.tree.get[me()]);

    unsigned idx_qry = idx_qry_begin, idx_qry_in_cache = 0;
    uintptr_t cursor_on_qrys = qrys + sizeof(key_uint64_t) * idx_qry,
              cursor_on_results = results + sizeof(value_uint64_t) * idx_qry;
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
        mram_write(&wks_me->qrys[0], (__mram_ptr void*)cursor_on_results, sizeof(value_uint64_t) * idx_qry_in_cache);
    }
}
void task_get(void)
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
static key_uint64_t PRED_pop_qry(PredWorkspace* wks)
{
    if (wks->idx_qry_in_cache == TASK_PRED_NR_CACHED_QRYS) {
        wks->cursor_on_qrys += sizeof(key_uint64_t) * TASK_PRED_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)wks->cursor_on_qrys, wks->qrys, sizeof(key_uint64_t) * TASK_PRED_NR_CACHED_QRYS);
        wks->idx_qry_in_cache = 0;
    }
    return wks->qrys[wks->idx_qry_in_cache++];
}
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
//! child that contains the global predecessor.  Host-side lower_bound routing
//! guarantees the query reaches a tree that owns the predecessor, hence the
//! reached leaf always has idx_pair > 0 (no idx_pair==0 path is needed).
static KVPair PRED_search_one(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const key_uint64_t key)
{
    PredWorkspace* const wks_me = loop_invariant(&workspace.tree.pred[me()]);

    if (height == 0) {
        const uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, key);
        return (KVPair){root->lf.keys[idx_pair - 1], NthValue(root->lf, idx_pair - 1)};
    }

    NodeLink link = NthChild(root->inl, search_for_pair_index(&root->inl.keys[0], root_numKeys, key));
    for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
        fetch_internal_filled(&Deref(link.ptr).inl, &wks_me->node_cache.inl, link.numKeys);
        const uint16_t idx_child = search_for_pair_index(&wks_me->node_cache.inl.keys[0], link.numKeys, key);
        link = NthChild(wks_me->node_cache.inl, idx_child);
    }
    fetch_leaf_filled(&Deref(link.ptr).lf, &wks_me->node_cache.lf, link.numKeys);
    const uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, key);
    return (KVPair){wks_me->node_cache.lf.keys[idx_pair - 1], NthValue(wks_me->node_cache.lf, idx_pair - 1)};
}
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
void task_pred(void)
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
static void RANGE_MIN_commit_next_result(value_uint64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    if (*idx_result_in_cache == TASK_RANGE_MIN_NR_CACHED_RESULTS) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_uint64_t) * TASK_RANGE_MIN_NR_CACHED_RESULTS);
        *cursor_on_results += sizeof(value_uint64_t) * TASK_RANGE_MIN_NR_CACHED_RESULTS;
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
    cursor_on_results = results + sizeof(value_uint64_t) * (idx_delim - idx_lump);


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

                value_uint64_t min = VALUE_MAX;
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

                value_uint64_t min = VALUE_MAX;
                const value_uint64_t* const rev_values = loop_invariant(RevValues(wks_me->node_cache.lf));
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
        mram_write(&wks_me->results[0], (__mram_ptr void*)cursor_on_results, sizeof(value_uint64_t) * idx_result_in_cache);
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
                        hot_results = cold_results + sizeof(value_uint64_t) * nr_cold_results;

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
static RangeCountQuery* RANGE_COUNT_pop_qry(RangeCountQuery* qrys_cache, unsigned* idx_qry_in_cache, uintptr_t* cursor_on_qrys)
{
    if (*idx_qry_in_cache == TASK_RANGE_COUNT_NR_CACHED_QRYS) {
        *cursor_on_qrys += sizeof(RangeCountQuery) * TASK_RANGE_COUNT_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)*cursor_on_qrys, qrys_cache, sizeof(RangeCountQuery) * TASK_RANGE_COUNT_NR_CACHED_QRYS);
        *idx_qry_in_cache = 0;
    }
    return &qrys_cache[(*idx_qry_in_cache)++];
}
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
static void RANGE_COUNT_flush_results_cache(uint64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    if (*idx_result_in_cache != 0) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(uint64_t) * *idx_result_in_cache);
        *cursor_on_results += sizeof(uint64_t) * *idx_result_in_cache;
        *idx_result_in_cache = 0;
    }
}
static uint64_t RANGE_COUNT_impl(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const RangeCountQuery* const qry)
{
    RCQWorkspace* const wks_me = loop_invariant(&workspace.tree.rcq[me()]);
    // hoisted out of the scan loops by hand: the compiler must assume the
    // fetches into node_cache may alias *qry, so it cannot
    const key_uint64_t range_end = qry->range.end;
    const value_uint64_t needle = qry->needle;

    uint64_t count = 0;

    if (height == 0) {
        uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, qry->range.begin);

        const value_uint64_t* const rev_values = RevValues(root->lf);
        for (; idx_pair < root_numKeys && root->lf.keys[idx_pair] <= range_end; idx_pair++) {
            if (rev_values[-(int32_t)idx_pair] != NOT_FOUND_VALUE && rev_values[-(int32_t)idx_pair] == needle) {
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

        const value_uint64_t* const rev_values = loop_invariant(RevValues(wks_me->node_cache.lf));
        const key_uint64_t* const cached_keys = loop_invariant(&wks_me->node_cache.lf.keys[0]);
        for (;;) {
            for (; idx_pair < link.numKeys; idx_pair++) {
                if (cached_keys[idx_pair] > range_end) {
                    goto end_of_range;
                }
                if (rev_values[-(int32_t)idx_pair] != NOT_FOUND_VALUE && rev_values[-(int32_t)idx_pair] == needle) {
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
void task_range_count(void)
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
static KeyRange* RANGE_MAX_pop_qry(KeyRange* qrys_cache, unsigned* idx_qry_in_cache, uintptr_t* cursor_on_qrys)
{
    if (*idx_qry_in_cache == TASK_RANGE_MAX_NR_CACHED_QRYS) {
        *cursor_on_qrys += sizeof(KeyRange) * TASK_RANGE_MAX_NR_CACHED_QRYS;
        mram_read((__mram_ptr void*)*cursor_on_qrys, qrys_cache, sizeof(KeyRange) * TASK_RANGE_MAX_NR_CACHED_QRYS);
        *idx_qry_in_cache = 0;
    }
    return &qrys_cache[(*idx_qry_in_cache)++];
}
static void RANGE_MAX_push_result(value_uint64_t result, value_uint64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    results_cache[*idx_result_in_cache] = result;
    (*idx_result_in_cache)++;
    if (*idx_result_in_cache == TASK_RANGE_MAX_NR_CACHED_RESULTS) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_uint64_t) * TASK_RANGE_MAX_NR_CACHED_RESULTS);
        *cursor_on_results += sizeof(value_uint64_t) * TASK_RANGE_MAX_NR_CACHED_RESULTS;
        *idx_result_in_cache = 0;
    }
}
static void RANGE_MAX_flush_results_cache(value_uint64_t* results_cache, unsigned* idx_result_in_cache, uintptr_t* cursor_on_results)
{
    if (*idx_result_in_cache != 0) {
        mram_write(results_cache, (__mram_ptr void*)*cursor_on_results, sizeof(value_uint64_t) * *idx_result_in_cache);
        *cursor_on_results += sizeof(value_uint64_t) * *idx_result_in_cache;
        *idx_result_in_cache = 0;
    }
}
static value_uint64_t RANGE_MAX_impl(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const KeyRange* const qry)
{
    RMaxQWorkspace* const wks_me = loop_invariant(&workspace.tree.rmaxq[me()]);
    // hoisted out of the scan loops by hand: the compiler must assume the
    // fetches into node_cache may alias *qry, so it cannot
    const key_uint64_t range_end = qry->end;

    value_uint64_t max = NOT_FOUND_VALUE;

    if (height == 0) {
        uint16_t idx_pair = search_for_pair_index(&root->lf.keys[0], root_numKeys, qry->begin);

        const value_uint64_t* const rev_values = RevValues(root->lf);
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

        const value_uint64_t* const rev_values = loop_invariant(RevValues(wks_me->node_cache.lf));
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
static void RANGE_MAX_execute(const Node* const root, const uint8_t height, const uint8_t root_numKeys,
    const uintptr_t results, const uint32_t idx_qry_begin, const uint32_t idx_qry_end)
{
    static const uintptr_t qrys = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

    RMaxQWorkspace* const wks_me = loop_invariant(&workspace.tree.rmaxq[me()]);

    unsigned idx_qry = idx_qry_begin, idx_qry_in_cache = 0;
    uintptr_t cursor_on_qrys = qrys + sizeof(KeyRange) * idx_qry;
    mram_read((__mram_ptr void*)(cursor_on_qrys), &wks_me->qrys[0], sizeof(KeyRange) * TASK_RANGE_MAX_NR_CACHED_QRYS);

    unsigned idx_result_in_cache = 0;
    uintptr_t cursor_on_results = results + sizeof(value_uint64_t) * idx_qry;

    for (; idx_qry < idx_qry_end; idx_qry++) {
        const KeyRange* const qry = RANGE_MAX_pop_qry(&wks_me->qrys[0], &idx_qry_in_cache, &cursor_on_qrys);
        const value_uint64_t max = RANGE_MAX_impl(root, height, root_numKeys, qry);
        RANGE_MAX_push_result(max, &wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
    }
    RANGE_MAX_flush_results_cache(&wks_me->results[0], &idx_result_in_cache, &cursor_on_results);
}
void task_range_max(void)
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


static void SERIALIZE_init_pair_cache(SerializeWorkspace* wks, uintptr_t result_pairs)
{
    wks->nr_pairs = 0;
    wks->idx_pair_in_cache = 0;
    wks->cursor_on_pairs = result_pairs;
}
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
static void SERIALIZE_flush_pair_cache(SerializeWorkspace* wks)
{
    if (wks->idx_pair_in_cache != 0) {
        mram_write(&wks->pairs[0], (__mram_ptr void*)wks->cursor_on_pairs, sizeof(KVPair) * wks->idx_pair_in_cache);
    }
}
static void SERIALIZE_init_delim_cache(SerializeWorkspace* wks, uint32_t nr_delims, uintptr_t delims)
{
    wks->nr_delims = nr_delims;
    wks->idx_delim_in_cache = TASK_SERIALIZE_NR_CACHED_DELIMS;
    wks->cursor_on_delims = delims;
}
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
static void SERIALIZE_init_incision_cache(SerializeWorkspace* wks, uintptr_t result_incisions)
{
    wks->idx_incision_in_cache = 0;
    wks->cursor_on_incisions = result_incisions;
}
static uint32_t* SERIALIZE_prepare_incision_cache(SerializeWorkspace* wks)
{
    if (wks->idx_incision_in_cache == TASK_SERIALIZE_NR_CACHED_INCISIONS) {
        mram_write(&wks->incisions[0], (__mram_ptr void*)wks->cursor_on_incisions, sizeof(uint32_t) * TASK_SERIALIZE_NR_CACHED_INCISIONS);
        wks->cursor_on_incisions += sizeof(uint32_t) * TASK_SERIALIZE_NR_CACHED_INCISIONS;
        wks->idx_incision_in_cache = 0;
    }
    return &wks->incisions[wks->idx_incision_in_cache++];
}
static void SERIALIZE_flush_incision_cache(SerializeWorkspace* wks)
{
    if (wks->idx_incision_in_cache != 0) {
        mram_write(&wks->incisions[0], (__mram_ptr void*)wks->cursor_on_incisions, sizeof(uint32_t) * ((wks->idx_incision_in_cache + 1) / 2 * 2));
    }
}
static void SERIALIZE_mark_incision(SerializeWorkspace* wks, key_uint64_t** p_delim)
{
    uint32_t* const incision = SERIALIZE_prepare_incision_cache(wks);
    *incision = wks->nr_pairs;
    *p_delim = SERIALIZE_fetch_next_delim(wks);
}
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
        const value_uint64_t* const rev_values = RevValues(root->lf);
        for (uint8_t i = 0; i < root_numKeys; i++) {
            const value_uint64_t value = rev_values[-(int32_t)i];
            if (value != NOT_FOUND_VALUE) {
                const key_uint64_t key = root->lf.keys[i];

                while (delim != NULL && *delim < key) {
                    SERIALIZE_mark_incision(wks, &delim);
                }

                KVPair* const pair = SERIALIZE_prepare_pair_cache(wks);
                *pair = (KVPair){key, value};
            }
        }
    } else {
        NodeLink cursor = NthChild(root->inl, 0);
        // find first leaf
        for (uint8_t height_of_parent = height; height_of_parent > 1; height_of_parent--) {
            // the physical pair {&NthChild(_, 1), &NthChild(_, 0)} holds children 1 and 0
            mram_read(&NthChild(Deref(cursor.ptr).inl, 1), &wks->children_cache[0], sizeof(NodeLink) * 2);
            cursor = wks->children_cache[1];
        }

        // serialize kvpairs in leaf nodes
        const value_uint64_t* const rev_values = loop_invariant(RevValues(wks->leaf_cache));
        const key_uint64_t* const cached_keys = loop_invariant(&wks->leaf_cache.keys[0]);
        for (;;) {
            fetch_leaf_filled(&Deref(cursor.ptr).lf, &wks->leaf_cache, cursor.numKeys);

            for (uint8_t i = 0; i < cursor.numKeys; i++) {
                const value_uint64_t value = rev_values[-(int32_t)i];
                if (value != NOT_FOUND_VALUE) {
                    const key_uint64_t key = cached_keys[i];

                    while (delim != NULL && *delim < key) {
                        SERIALIZE_mark_incision(wks, &delim);
                    }

                    KVPair* const pair = SERIALIZE_prepare_pair_cache(wks);
                    *pair = (KVPair){key, value};
                }
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
void task_serialize(void)
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


static void tree_clear(uint8_t* const p_root_numKeys, const Node* const root, uint8_t* const p_height)
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

        *p_root_numKeys = *p_height = 0;
    }
}
static NodePtr MOVE_HOT_allocator(unsigned idx_node)
{
    (void)idx_node;
    return Allocate_node();
}
void task_move_hot(void)
{
    if (input_header.move_hot.renew_cold && input_header.move_hot.renew_hot) {
        return task_init();
    }

    if (input_header.move_hot.renew_cold) {
        tree_clear(&cold_root_numKeys, &cold_root, &cold_height);
    }
    if (input_header.move_hot.renew_hot) {
        tree_clear(&hot_root_numKeys, &hot_root, &hot_height);
    }

    _Static_assert(TREE_CONSTRUCT_NR_TASKLETS > 0, "TREE_CONSTRUCT_NR_TASKLETS > 0");
    if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
        if (input_header.move_hot.renew_cold || input_header.move_hot.renew_hot) {
            TREE_CONSTRUCT_barrier();
        }

        static const uintptr_t cold_pairs = (uintptr_t)DPU_MRAM_HEAP_POINTER + sizeof(InputHeader);

        if (input_header.move_hot.nr_cold_pairs > 0) {
            if (input_header.move_hot.renew_cold) {
                construct_tree(cold_pairs, input_header.move_hot.nr_cold_pairs,
                    &cold_root_numKeys, &cold_root, &cold_height, &cold_min_key, &nr_pairs.cold, MOVE_HOT_allocator);
            } else {
                _Static_assert(TASK_INSERT_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS, "TASK_INSERT_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS");
                if (me() < TASK_INSERT_NR_TASKLETS) {
                    INSERT_execute_batch(&cold_root, &cold_height, &cold_root_numKeys, &nr_pairs.cold,
                        0, input_header.move_hot.nr_cold_pairs);
                }
            }

            TREE_CONSTRUCT_barrier();
        }

        if (input_header.move_hot.nr_hot_pairs > 0) {
            if (input_header.move_hot.renew_hot) {
                const uintptr_t hot_pairs = cold_pairs + sizeof(KVPair) * input_header.move_hot.nr_cold_pairs;

                construct_tree(hot_pairs, input_header.move_hot.nr_hot_pairs,
                    &hot_root_numKeys, &hot_root, &hot_height, &hot_min_key, &nr_pairs.hot, MOVE_HOT_allocator);
            } else {
                _Static_assert(TASK_INSERT_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS, "TASK_INSERT_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS");
                if (me() < TASK_INSERT_NR_TASKLETS) {
                    INSERT_execute_batch(&hot_root, &hot_height, &hot_root_numKeys, &nr_pairs.hot,
                        input_header.move_hot.nr_cold_pairs, input_header.move_hot.nr_hot_pairs);
                }
            }
        }

#ifdef TASK_MOVE_HOT_CHECK
        _Static_assert(TREE_CONSTRUCT_NR_TASKLETS >= TASK_INSERT_NR_TASKLETS, "TREE_CONSTRUCT_NR_TASKLETS >= TASK_INSERT_NR_TASKLETS");
        TREE_CONSTRUCT_barrier();
        if (me() == 0) {
            check_tree_structure(&cold_root, cold_height, cold_root_numKeys);
            check_tree_structure(&hot_root, hot_height, hot_root_numKeys);
        }
#endif
    }
}


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
                            // fetch the aligned physical pair containing the child
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
