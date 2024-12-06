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

static DEFINE_DIV_BY(TASK_RANGE_MIN_NR_TASKLETS, 16, _NR_DELIMS);

Node cold_root, hot_root;
key_uint64_t cold_min_key, hot_min_key;
uint8_t cold_height, hot_height;
uint8_t cold_root_numKeys, hot_root_numKeys;


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
static uint16_t search_for_pair_index(const key_uint64_t* keys, uint8_t nr_keys, key_uint64_t query)
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
        dest_node->inl.children[idx_child_in_this_node] = p_lift->child;
    }
}

//! @sa /docs/tree_initialization.md
//! @return number of the allocated nodes
static unsigned construct_tree(uint8_t* root_numKeys, Node* root, uint8_t* height, key_uint64_t* min_key, NodePtr (*allocator)(unsigned))
{
    static const uintptr_t initial_pairs = (uintptr_t)DPU_MRAM_HEAP_POINTER + 8;

    InitWorkspace* const wks = &workspace.tree.init[me()];

    const unsigned nr_pairs = input_header.init.nr_pairs;
    if (nr_pairs == 0) {
        if (me() == 0) {
            *height = 0;
            *root_numKeys = 0;
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
        NodeLink link_to_this_node = {(nr_nodes == 1 ? NODE_NULLPTR : allocator(idx_node)), idx_pair_end_for_this_node - idx_pair};
        wks->first_leaf = link_to_this_node;

        for (;;) {
            for (unsigned idx_pair_in_this_node = 0; idx_pair < idx_pair_end_for_this_node; idx_pair++, idx_pair_in_this_node++) {
                if (idx_pair_cache == TREE_CONSTRUCT_NR_CACHED_KVPAIRS) {
                    mram_read((__mram_ptr KVPair*)(initial_pairs + sizeof(KVPair) * idx_pair), &wks->in.pairs[0],
                        sizeof(KVPair) * TREE_CONSTRUCT_NR_CACHED_KVPAIRS);
                    idx_pair_cache = 0;
                }
                node_cache->lf.keys[idx_pair_in_this_node] = wks->in.pairs[idx_pair_cache].key;
                node_cache->lf.values[idx_pair_in_this_node] = wks->in.pairs[idx_pair_cache].value;
                idx_pair_cache++;
            }
            node_cache->lf.left = left_node;

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
            link_to_this_node = (NodeLink){allocator(idx_node), idx_pair_end_for_this_node - idx_pair};

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
                    mram_write(&workspace.tree.init[me() - 1].last_leaf, &Deref(wks->first_leaf.ptr).lf.left, 8);
                }
                if (me() + 1 != TREE_CONSTRUCT_NR_TASKLETS && me() + 1 < nr_leaves) {
                    mram_write(&workspace.tree.init[me() + 1].first_leaf, &Deref(wks->last_leaf).lf.right, 8);
                }
            }
            if (idx_node_begin != idx_node_end) {
                *height = tmp_height;
                *root_numKeys = wks->out.lifted[TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT - 1].child.numKeys;
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

                const unsigned incoming_links = lifted_links;
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
                        node_cache->inl.children[idx_child_in_this_node] = p_lift->child;
                        idx_child_cache++;
                    }

                    const NodeLink link_to_this_node = {(nr_nodes == 1 ? NODE_NULLPTR : allocator(nr_nodes_in_lower + idx_node)), idx_child_in_this_node - 1};
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

static NodePtr INIT_allocator(unsigned idx_node)
{
    return idx_node;
}
void task_init(void)
{
    _Static_assert(TREE_CONSTRUCT_NR_TASKLETS > 0, "TREE_CONSTRUCT_NR_TASKLETS > 0");
    if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
        const unsigned nr_allocated_nodes = construct_tree(&cold_root_numKeys, &cold_root, &cold_height, &cold_min_key, INIT_allocator);

        TREE_CONSTRUCT_barrier();

        _Static_assert(TASK_INIT_ALLOC_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS, "TASK_INIT_ALLOC_NR_TASKLETS <= TREE_CONSTRUCT_NR_TASKLETS");
        Allocator_init(nr_allocated_nodes);

#ifdef TASK_INIT_CHECK
        TREE_CONSTRUCT_barrier();
        if (me() == 0) {
            check_tree_structure(&cold_root, cold_height, cold_root_numKeys);
        }
#endif
    }
}


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
    TaskletLocalRMQWorkspace* const wks_me = &workspace.tree.rmq.th[me()];

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
                    if (min > root->lf.values[idx_pair]) {
                        min = root->lf.values[idx_pair];
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

            NodeLink link = root->inl.children[search_for_child_index(&root->inl.keys[0], root_numKeys, range_begin)];
            for (uint8_t height_of_linked = height - 1; height_of_linked > 0; height_of_linked--) {
                mram_read(&Deref(link.ptr).inl.keys[0], &wks_me->node_cache.inl.keys[0], sizeof(key_uint64_t) * link.numKeys);
                const uint16_t idx_child = search_for_child_index(&wks_me->node_cache.inl.keys[0], link.numKeys, range_begin);
                mram_read(&Deref(link.ptr).inl.children[idx_child / 2 * 2], &wks_me->node_cache.inl.children[0], sizeof(NodeLink) * 2);
                link = wks_me->node_cache.inl.children[idx_child % 2];
            }
            mram_read(&Deref(link.ptr).lf, &wks_me->node_cache.lf, offsetof(LeafNode, left));
            uint16_t idx_pair = search_for_pair_index(&wks_me->node_cache.lf.keys[0], link.numKeys, range_begin);

            RANGE_MIN_prepare_next_lump_end_index(&wks_me->lump_end_indices[0], &idx_lump_in_cache, &cursor_on_lump_end_indices);
            const uint16_t idx_delim_end = wks_me->lump_end_indices[idx_lump_in_cache];
            idx_lump_in_cache++;
            for (idx_delim++; idx_delim < idx_delim_end; idx_delim++) {

                RANGE_MIN_prepare_next_delim_key(&wks_me->delim_keys[0], &idx_delim_in_cache, &cursor_on_delim_keys);
                const key_uint64_t range_end = wks_me->delim_keys[idx_delim_in_cache];
                idx_delim_in_cache++;

                value_uint64_t min = VALUE_MAX;
                for (;;) {
                    for (; idx_pair < link.numKeys; idx_pair++) {
                        if (wks_me->node_cache.lf.keys[idx_pair] > range_end) {
                            goto end_of_range;
                        }
                        if (min > wks_me->node_cache.lf.values[idx_pair]) {
                            min = wks_me->node_cache.lf.values[idx_pair];
                        }
                    }
                    const NodeLink right_leaf = wks_me->node_cache.lf.right;
                    if (right_leaf.ptr == NODELINK_NULLPTR.ptr && right_leaf.numKeys == NODELINK_NULLPTR.numKeys) {
                        goto end_of_range;
                    }
                    link = right_leaf;
                    mram_read(&Deref(link.ptr).lf, &wks_me->node_cache.lf, offsetof(LeafNode, left));
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

        static RMQWorkSpace* const wks = &workspace.tree.rmq;
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

        const uintptr_t cold_results = (uintptr_t)DPU_MRAM_HEAP_POINTER + RMQ_RESULT_OFFSET,
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


//! @return index of child
static uint8_t distribute_fewer_subtrees_than_tasklets(const uint8_t nr_subtrees, uint8_t* const nr_tasklets, uint8_t* const idx_tasklet)
{
    const uint8_t orig_nr_tasklets = *nr_tasklets,
                  orig_idx_tasklet = *idx_tasklet;
    const uint8_t nr_tasklets_per_child = orig_nr_tasklets / nr_subtrees,
                  nr_remainder_tasklets = orig_nr_tasklets % nr_subtrees;

    const uint8_t nr_tasklets_in_greater_chunks = (nr_tasklets_per_child + 1) * nr_remainder_tasklets;
    if (orig_idx_tasklet < nr_tasklets_in_greater_chunks) {
        const uint8_t idx_subtree = orig_idx_tasklet / (nr_tasklets_per_child + 1);
        *idx_tasklet = orig_idx_tasklet % (nr_tasklets_per_child + 1);
        *nr_tasklets = nr_tasklets_per_child + 1;
        return idx_subtree;

    } else {
        const uint8_t idx_subtree = (orig_idx_tasklet - nr_remainder_tasklets) / nr_tasklets_per_child;
        *idx_tasklet = (orig_idx_tasklet - nr_remainder_tasklets) % nr_tasklets_per_child;
        *nr_tasklets = nr_tasklets_per_child;
        return idx_subtree;
    }
}
//! @return index of the beginning subtree
static uint8_t distribute_more_or_equal_subtrees_than_tasklets(uint8_t* const nr_subtrees, const uint8_t nr_tasklets, const uint8_t idx_tasklet)
{
    const uint8_t orig_nr_subtrees = *nr_subtrees;

    const uint8_t nr_subtrees_per_tasklet = orig_nr_subtrees / nr_tasklets,
                  nr_remainder_subtrees = orig_nr_subtrees % nr_tasklets,
                  nr_subtrees_for_me = nr_subtrees_per_tasklet + (idx_tasklet >= nr_tasklets - nr_remainder_subtrees);
    const uint8_t tmp = nr_tasklets - nr_remainder_subtrees,
                  idx_subtree_begin = (uint8_t)(nr_subtrees_per_tasklet * idx_tasklet + (idx_tasklet >= tmp ? idx_tasklet : tmp) - tmp);
    *nr_subtrees = nr_subtrees_for_me;
    return idx_subtree_begin;
}

void task_summarize(void)
{
    static SummarizeWorkspace* const wks = &workspace.tree.summarize;
    static const uintptr_t result_blocks = (uintptr_t)DPU_MRAM_HEAP_POINTER + (6 + sizeof(uint16_t) * MAX_NR_SUMMARY_CHUNKS + 7) / 8 * 8;

    if (cold_height == 0) {
        if (me() == 0) {
            wks->result_header.nr_pairs = cold_root_numKeys;
            wks->result_header.nr_chunks = 1;
            wks->result_header.chunk_end_indices[0] = 1;
            mram_write(&wks->result_header, DPU_MRAM_HEAP_POINTER, 8);

            SummaryBlock* const block0 = &wks->th[0].summary[0];
            for (unsigned i = 0; i < 4; i++) {
                block0->head_keys[i] = cold_min_key;
                block0->nr_keys[i] = 0;
            }
            block0->nr_keys[3] = cold_root_numKeys;
            mram_write(block0, (__mram_ptr SummaryBlock*)result_blocks, sizeof(SummaryBlock));
        }

    } else if (cold_height == 1) {
        if (me() == 0) {
            SummaryBlock* const blocks_cache = &wks->th[0].summary[0];
            uint16_t idx_cached_summary = 0;

            uint32_t nr_pairs = 0;
            key_uint64_t min_key = cold_min_key;
            for (; idx_cached_summary < (cold_root_numKeys + 1) % 4; idx_cached_summary++) {
                blocks_cache[0].nr_keys[idx_cached_summary] = 0;
                blocks_cache[0].head_keys[idx_cached_summary] = min_key;
            }

            uint16_t nr_blocks_in_mram = 0;
            uintptr_t next_result_blocks = result_blocks;

            for (uint8_t idx_leaf = 0; idx_leaf < cold_root_numKeys; idx_leaf++, idx_cached_summary++) {
                if (idx_cached_summary == NR_SUMMARY_BLOCKS_PER_CHUNK * 4) {
                    mram_write(blocks_cache, (__mram_ptr SummaryBlock*)next_result_blocks, sizeof(SummaryBlock) * NR_SUMMARY_BLOCKS_PER_CHUNK);
                    idx_cached_summary = 0;
                    nr_blocks_in_mram += NR_SUMMARY_BLOCKS_PER_CHUNK;
                    next_result_blocks += sizeof(SummaryBlock) * NR_SUMMARY_BLOCKS_PER_CHUNK;
                }

                const uint8_t leaf_numKeys = cold_root.inl.children[idx_leaf].numKeys;
                nr_pairs += leaf_numKeys;
                blocks_cache[idx_cached_summary / 4].nr_keys[idx_cached_summary % 4] = leaf_numKeys;
                blocks_cache[idx_cached_summary / 4].head_keys[idx_cached_summary % 4] = min_key;
                min_key = cold_root.inl.keys[idx_leaf];
            }
            uint16_t idx_cached_block = idx_cached_summary / 4;
            const uint8_t leaf_numKeys = cold_root.inl.children[cold_root_numKeys].numKeys;
            nr_pairs += leaf_numKeys;
            blocks_cache[idx_cached_block].nr_keys[3] = leaf_numKeys;
            blocks_cache[idx_cached_block].head_keys[3] = min_key;
            idx_cached_block++;

            mram_write(blocks_cache, (__mram_ptr SummaryBlock*)next_result_blocks, sizeof(SummaryBlock) * idx_cached_block);
            idx_cached_summary = 0;
            nr_blocks_in_mram += idx_cached_block;

            wks->result_header.nr_pairs = nr_pairs;
            wks->result_header.nr_chunks = 1;
            wks->result_header.chunk_end_indices[0] = nr_blocks_in_mram;
            mram_write(&wks->result_header, DPU_MRAM_HEAP_POINTER, 8);
        }
    } else {
        _Static_assert(TASK_SUMMARIZE_NR_TASKLETS > 0, "TASK_SUMMARIZE_NR_TASKLETS > 0");
        if (me() < TASK_SUMMARIZE_NR_TASKLETS) {
            if (me() == TASK_SUMMARIZE_NR_TASKLETS - 1) {
                wks->nr_allocated_bytes = 0;
                wks->result_header.nr_pairs = 0;
                wks->result_header.nr_chunks = 0;
                wks->nr_committed_blocks = 0;
            } else {
                wait_for_next_ready();
            }
            if (me() != 0) {
                notify_prev_of_readiness();
            }

            TaskletLocalSummarizeWorkspace* const wks_me = &wks->th[me()];
            // going to summarize [traversal_root, traversal_root + nr_subtrees),
            // whose height is (height) and
            //       minimum key is (min_key) and
            //       delimiter keys between subtrees are [delim_key_begin, delim_key_begin + nr_subtrees - 1)
            NodeLink* traversal_root;
            uint8_t nr_subtrees = cold_root_numKeys + 1;
            uint8_t height = cold_height - 1;
            __dma_aligned key_uint64_t min_key = cold_min_key;
            key_uint64_t* delim_key_begin;

            {
                uint8_t idx_tasklet = (uint8_t)me(), nr_tasklets = TASK_SUMMARIZE_NR_TASKLETS;
                if (nr_subtrees >= nr_tasklets) {
                    const uint8_t idx_subtree_begin = distribute_more_or_equal_subtrees_than_tasklets(&nr_subtrees, nr_tasklets, idx_tasklet);
                    traversal_root = &cold_root.inl.children[idx_subtree_begin];
                    if (idx_subtree_begin != 0) {
                        min_key = cold_root.inl.keys[idx_subtree_begin - 1];
                    }
                    delim_key_begin = &cold_root.inl.keys[idx_subtree_begin];

                } else /* nr_subtrees < nr_tasklets */ {
                    const uint8_t idx_subtree_begin = distribute_fewer_subtrees_than_tasklets(nr_subtrees, &nr_tasklets, &idx_tasklet);
                    nr_subtrees = 1;
                    traversal_root = &cold_root.inl.children[idx_subtree_begin];
                    if (idx_subtree_begin != 0) {
                        min_key = cold_root.inl.keys[idx_subtree_begin - 1];
                    }

                    for (; nr_tasklets > 1; height--) {
                        if (height == 1) {
                            nr_subtrees = (idx_tasklet == 0);
                            break;
                        }
                        const NodePtr subtree_root = traversal_root->ptr;
                        nr_subtrees = traversal_root->numKeys + 1;
                        if (traversal_root->numKeys + 1 >= nr_tasklets) {
                            const uint8_t idx_subtree_begin = distribute_more_or_equal_subtrees_than_tasklets(&nr_subtrees, nr_tasklets, idx_tasklet),
                                          idx_subtree_end = idx_subtree_begin + nr_subtrees;

                            const uint8_t idx_copy_begin = idx_subtree_begin / 2 * 2,
                                          idx_copy_end = (idx_subtree_end + 1) / 2 * 2;
                            mram_read(&Deref(subtree_root).inl.children[idx_copy_begin],
                                &wks_me->traversal_roots[0],
                                sizeof(NodeLink) * (idx_copy_end - idx_copy_begin));
                            traversal_root = &wks_me->traversal_roots[idx_subtree_begin % 2];
                            height--;

                            if (idx_subtree_begin != 0) {
                                mram_read(&Deref(subtree_root).inl.keys[idx_subtree_begin - 1],
                                    &wks_me->delims_of_traversal_roots[0],
                                    sizeof(key_uint64_t) * nr_subtrees);
                                min_key = wks_me->delims_of_traversal_roots[0];
                                delim_key_begin = &wks_me->delims_of_traversal_roots[1];

                            } else if (nr_subtrees != 1) {
                                mram_read(&Deref(subtree_root).inl.keys[idx_subtree_begin],
                                    &wks_me->delims_of_traversal_roots[0],
                                    sizeof(key_uint64_t) * (nr_subtrees - 1));
                                delim_key_begin = &wks_me->delims_of_traversal_roots[0];
                            }
                            break;

                        } else {
                            const uint8_t idx_subtree_begin = distribute_fewer_subtrees_than_tasklets(traversal_root->numKeys + 1, &nr_tasklets, &idx_tasklet);
                            traversal_root = &wks_me->traversal_roots[idx_subtree_begin % 2];
                            mram_read(&Deref(subtree_root).inl.children[idx_subtree_begin / 2 * 2],
                                &wks_me->traversal_roots[0],
                                sizeof(NodeLink) * 2);
                            if (idx_subtree_begin != 0 && idx_tasklet == 0) {
                                mram_read(&Deref(subtree_root).inl.keys[idx_subtree_begin - 1], &min_key, sizeof(key_uint64_t));
                            }
                        }
                    }
                }
            }

            uint16_t idx_summary_in_cache = 0;
            uint32_t nr_pairs_from_me = 0;
            uint16_t next_idx_in_chunk_linked_list = (uint16_t)me();
            if (nr_subtrees != 0) {
                for (uint8_t idx_subtree = 0;;) {
                    // invariant: stack_height + height_of(cursor) == height
                    // invariant: i + height_of(wks_me->stack[i]) == height
                    uint8_t stack_height = 0;
                    NodeLink cursor = *traversal_root;
                    for (;;) {
                        for (; stack_height < height - 1; stack_height++) {
                            wks_me->stack[stack_height].node = cursor;
                            wks_me->stack[stack_height].nr_visited_children = 0;
                            mram_read(&Deref(cursor.ptr).inl.children[0], &wks_me->stack[stack_height].children_cache[0], sizeof(NodeLink) * 2);
                            cursor = wks_me->stack[stack_height].children_cache[0];
                        }

                        {
                            mram_read(&Deref(cursor.ptr).inl.children[0], &wks_me->children_cache[0], ((cursor.numKeys + 1) + 1) / 2 * 2 * sizeof(NodeLink));
                            uint16_t nr_keys = 0;
                            for (uint8_t idx_leaf = 0; idx_leaf <= cursor.numKeys; idx_leaf++) {
                                nr_keys += wks_me->children_cache[idx_leaf].numKeys;
                            }
                            nr_pairs_from_me += nr_keys;
                            wks_me->summary[idx_summary_in_cache / 4].nr_keys[idx_summary_in_cache % 4] = nr_keys;
                        }
                        wks_me->summary[idx_summary_in_cache / 4].head_keys[idx_summary_in_cache % 4] = min_key;
                        idx_summary_in_cache++;

                        if (idx_summary_in_cache == NR_SUMMARY_BLOCKS_PER_CHUNK * 4) {
                            static const uint16_t written_bytes = NR_SUMMARY_BLOCKS_PER_CHUNK * sizeof(SummaryBlock);
                            acquire_lock();
                            const uint32_t orig_nr_allocated_bytes = wks->nr_allocated_bytes;
                            wks->nr_allocated_bytes = orig_nr_allocated_bytes + written_bytes;
                            const uint16_t orig_nr_chunks = wks->result_header.nr_chunks;
                            wks->result_header.nr_chunks = orig_nr_chunks + 1;
                            release_lock();

                            mram_write(&wks_me->summary[0], (__mram_ptr SummaryBlock*)(result_blocks + orig_nr_allocated_bytes), written_bytes);
                            idx_summary_in_cache = 0;

                            next_idx_in_chunk_linked_list = (wks->chunk_linked_list[next_idx_in_chunk_linked_list]
                                                             = orig_nr_chunks + TASK_SUMMARIZE_NR_TASKLETS);
                        }

                        bool finish = false;
                        for (;; stack_height--) {
                            if (stack_height == 0) {
                                finish = true;
                                break;
                            }

                            SummarizeStackElem* const stack_elem = &wks_me->stack[stack_height - 1];
                            cursor = stack_elem->node;
                            const uint8_t orig_nr_visited_children = stack_elem->nr_visited_children;
                            if (orig_nr_visited_children != cursor.numKeys) {
                                mram_read(&Deref(cursor.ptr).inl.keys[orig_nr_visited_children], &min_key, sizeof(key_uint64_t));
                                stack_elem->nr_visited_children = orig_nr_visited_children + 1;

                                if (orig_nr_visited_children % 2 == 0) {
                                    cursor = stack_elem->children_cache[1];
                                } else {
                                    mram_read(&Deref(cursor.ptr).inl.children[orig_nr_visited_children + 1], &stack_elem->children_cache[0], sizeof(NodeLink) * 2);
                                    cursor = stack_elem->children_cache[0];
                                }
                                break;
                            }
                        }
                        if (finish) {
                            break;
                        }
                    }


                    //----------------------------------------
                    idx_subtree++;
                    if (idx_subtree == nr_subtrees) {
                        break;
                    }
                    min_key = *delim_key_begin;
                    delim_key_begin++;
                    traversal_root++;
                }

                if (idx_summary_in_cache % 4 != 0) {
                    const uint16_t idx_block = idx_summary_in_cache / 4;
                    uint16_t idx_in_block = idx_summary_in_cache % 4 - 1;
                    const uint16_t last_nr_keys = wks_me->summary[idx_block].nr_keys[idx_in_block];
                    for (; idx_in_block < 4; idx_in_block++) {
                        wks_me->summary[idx_block].nr_keys[idx_in_block] = 0;
                        wks_me->summary[idx_block].head_keys[idx_in_block] = min_key;
                    }
                    wks_me->summary[idx_block].nr_keys[3] = last_nr_keys;

                    // adjust to ensure that nr_trailing_blocks be a correct value
                    idx_summary_in_cache += 3;
                }
            }
            const uint16_t nr_trailing_blocks = idx_summary_in_cache / 4;

            if (me() != 0) {
                wait_for_prev_ready();
            }
            uint16_t nr_committed_blocks = wks->nr_committed_blocks;

            for (uint16_t idx_in_chunk_linked_list = next_idx_in_chunk_linked_list; idx_in_chunk_linked_list != next_idx_in_chunk_linked_list;) {
                idx_in_chunk_linked_list = wks->chunk_linked_list[idx_in_chunk_linked_list];
                const uint16_t idx_chunk = idx_in_chunk_linked_list - TASK_SUMMARIZE_NR_TASKLETS;

                nr_committed_blocks += NR_SUMMARY_BLOCKS_PER_CHUNK;
                wks->result_header.chunk_end_indices[idx_chunk] = nr_committed_blocks;
            }

            if (nr_trailing_blocks != 0) {
                const uint16_t written_bytes = nr_trailing_blocks * sizeof(SummaryBlock);
                acquire_lock();
                const uint32_t orig_nr_allocated_bytes = wks->nr_allocated_bytes;
                wks->nr_allocated_bytes = orig_nr_allocated_bytes + written_bytes;
                const uint16_t orig_nr_chunks = wks->result_header.nr_chunks;
                wks->result_header.nr_chunks = orig_nr_chunks + 1;
                release_lock();

                mram_write(&wks_me->summary[0], (__mram_ptr SummaryBlock*)(result_blocks + orig_nr_allocated_bytes), written_bytes);

                nr_committed_blocks += nr_trailing_blocks;
                wks->result_header.chunk_end_indices[orig_nr_chunks] = nr_committed_blocks;
            }

            wks->result_header.nr_pairs += nr_pairs_from_me;
            if (me() != TASK_SUMMARIZE_NR_TASKLETS - 1) {
                wks->nr_committed_blocks = nr_committed_blocks;
                notify_next_of_readiness();
            } else {
                uint32_t result_header_size = (6 + sizeof(uint16_t) * wks->result_header.nr_chunks + 7) / 8 * 8;
                uintptr_t result_header_src = (uintptr_t)(&wks->result_header), result_header_dest = (uintptr_t)DPU_MRAM_HEAP_POINTER;
                for (; result_header_size >= 2048; result_header_size -= 2048, result_header_src += 2048, result_header_dest += 2048) {
                    mram_write((const void*)result_header_src, (__mram_ptr void*)result_header_dest, 2048);
                }
                if (result_header_size != 0) {
                    mram_write((const void*)result_header_src, (__mram_ptr void*)result_header_dest, result_header_size);
                }
            }
        }
    }
}


static void EXTRACT_nodes(void)
{
    static ExtractWorkspace* const wks = &workspace.tree.extract;
    {
        uint32_t nr_ranges = input_header.extract.nr_ranges;
        uintptr_t mram_src = (uintptr_t)DPU_MRAM_HEAP_POINTER + 8, wram_dst = (uintptr_t)(&wks->hot_ranges[0]);
        for (; nr_ranges > 2048 / sizeof(KeyRange); nr_ranges -= 2048 / sizeof(KeyRange), mram_src += 2048, wram_dst += 2048) {
            mram_read((__mram_ptr KeyRange*)mram_src, (KeyRange*)wram_dst, 2048);
        }
        mram_read((__mram_ptr KeyRange*)mram_src, (KeyRange*)wram_dst, sizeof(KeyRange) * nr_ranges);
    }

    static const uintptr_t result_nr_pairs = (uintptr_t)DPU_MRAM_HEAP_POINTER;
    const uintptr_t result_pairs = result_nr_pairs + (NR_RANKS * MAX_NR_DPUS_IN_RANK * sizeof(uint32_t) + 7) / 8 * 8;

    if (me() == 0) {
        if (cold_height == 0) {
            wks->nr_pairs_cache[0] = cold_root_numKeys;
            mram_write(&wks->nr_pairs_cache[0], (__mram_ptr uint32_t*)result_nr_pairs, 8);

            for (uint8_t i = 0; i < cold_root_numKeys; i++) {
                wks->kvpair = (KVPair){cold_root.lf.keys[0], cold_root.lf.values[0]};
                mram_write(&wks->kvpair, (__mram_ptr KVPair*)(result_pairs + sizeof(KVPair) * i), sizeof(KVPair));
            }

            cold_root_numKeys = 0;

        } else {
            uint32_t idx_hot = 0, idx_pair = 0;
            for (; idx_hot < input_header.extract.nr_ranges; idx_hot++) {
                const KeyRange range = wks->hot_ranges[idx_hot];
                uint32_t nr_pairs = 0;

                uint16_t nr_passed_children_of_root = search_for_child_index(&cold_root.inl.keys[0], cold_root_numKeys, range.begin);
                const uint16_t initial_nr_passed_children_of_root = nr_passed_children_of_root;

                NodeLink cursor = cold_root.inl.children[nr_passed_children_of_root];
                __dma_aligned key_uint64_t next_key = UINT64_MAX;

                uint8_t stack_height = 0;
                for (; stack_height < cold_height - 1; stack_height++) {
                    wks->stack[stack_height].node = cursor;

                    mram_read(&Deref(cursor.ptr).inl.keys[0], &wks->node_cache.inl.keys[0], sizeof(key_uint64_t) * cursor.numKeys);
                    const uint16_t idx_child = search_for_child_index(&wks->node_cache.inl.keys[0], cursor.numKeys, range.begin);

                    wks->stack[stack_height].nr_passed_children = idx_child;
                    mram_read(&Deref(cursor.ptr).inl.children[idx_child / 2 * 2], &wks->stack[stack_height].children_cache[0], sizeof(NodeLink) * 2);
                    cursor = wks->stack[stack_height].children_cache[idx_child % 2];
                }
                {
                    uintptr_t copy_src = (uintptr_t)(&wks->stack[0]), copy_dest = (uintptr_t)(&wks->initial_stack[0]),
                              copy_src_end = ((uintptr_t)(&wks->stack[cold_height - 1]) + 7) / 8 * 8;
                    for (; copy_src < copy_src_end; copy_src += 8, copy_dest += 8) {
                        uint64_t buf;
                        memcpy(&buf, (const void*)copy_src, 8);
                        memcpy((void*)copy_dest, &buf, 8);
                    }
                }

                mram_read(&Deref(cursor.ptr), &wks->node_cache, sizeof(LeafNode));

                for (;;) {
                    nr_pairs += cursor.numKeys;
                    for (uint8_t i = 0; i < cursor.numKeys; i++, idx_pair++) {
                        wks->kvpair = (KVPair){wks->node_cache.lf.keys[i], wks->node_cache.lf.values[i]};
                        mram_write(&wks->kvpair, (__mram_ptr KVPair*)(result_pairs + sizeof(KVPair) * idx_pair), sizeof(KVPair));
                    }

                    Free_node(cursor.ptr);

                    for (;;) {
                        if (stack_height == 0) {
                            const uint16_t orig_nr_passed_children_of_root = nr_passed_children_of_root;
                            nr_passed_children_of_root++;
                            if (orig_nr_passed_children_of_root == cold_root_numKeys) {
                                goto end_of_hot_range;
                            }
                            next_key = cold_root.inl.keys[orig_nr_passed_children_of_root];
                            if (range.end < next_key) {
                                goto end_of_hot_range;
                            }
                            cursor = cold_root.inl.children[nr_passed_children_of_root];
                            break;
                        }
                        ExtractStackElem* const stack_elem = &wks->stack[stack_height - 1];
                        const uint16_t orig_nr_passed_children = stack_elem->nr_passed_children;
                        stack_elem->nr_passed_children++;
                        if (orig_nr_passed_children != stack_elem->node.numKeys) {
                            mram_read(&Deref(stack_elem->node.ptr).inl.keys[orig_nr_passed_children], &next_key, sizeof(key_uint64_t));
                            if (range.end < next_key) {
                                goto end_of_hot_range;
                            }

                            if (stack_elem->nr_passed_children % 2 == 0) {
                                mram_read(&Deref(stack_elem->node.ptr).inl.children[stack_elem->nr_passed_children], &stack_elem->children_cache[0], sizeof(NodeLink) * 2);
                                cursor = stack_elem->children_cache[0];
                            } else {
                                cursor = stack_elem->children_cache[1];
                            }
                            break;
                        } else {
                            stack_height--;
                            const NodeLink touched = stack_elem->node, initial = wks->initial_stack[stack_height].node;
                            if (!(initial.ptr == touched.ptr && initial.numKeys == touched.numKeys)) {
                                Free_node(touched.ptr);
                            }
                        }
                    }
                    for (; stack_height < cold_height - 1; stack_height++) {
                        wks->stack[stack_height].node = cursor;
                        wks->stack[stack_height].nr_passed_children = 0;
                        mram_read(&Deref(cursor.ptr).inl.children[0], &wks->stack[stack_height].children_cache[0], sizeof(NodeLink) * 2);
                        cursor = wks->stack[stack_height].children_cache[0];
                    }

                    mram_read(&Deref(cursor.ptr), &wks->node_cache, offsetof(LeafNode, left));
                }

            end_of_hot_range:
                if (wks->node_cache.lf.left != NODE_NULLPTR) {
                    mram_write(&wks->node_cache.lf.right, &Deref(wks->node_cache.lf.left).lf.right, 8);
                }
                if (!(wks->node_cache.lf.right.ptr == NODELINK_NULLPTR.ptr && wks->node_cache.lf.right.numKeys == NODELINK_NULLPTR.numKeys)) {
                    mram_write(&wks->node_cache.lf.left, &Deref(wks->node_cache.lf.right.ptr).lf.left, 8);
                }

printf("hot[%u] = %unr_pairs\n", idx_hot, nr_pairs;
                if (idx_hot % 2 == 0) {
                    wks->nr_pairs_cache[0] = nr_pairs;
                } else {
                    wks->nr_pairs_cache[1] = nr_pairs;
                    mram_write(&wks->nr_pairs_cache[0], (__mram_ptr uint32_t*)(result_nr_pairs + sizeof(uint32_t) * idx_hot - 4), sizeof(uint32_t) * 2);
                }

                for (; stack_height != 0; stack_height--) {
                    ExtractStackElem* const stack_elem = &wks->stack[stack_height - 1];
                    const NodeLink left_cut = wks->initial_stack[stack_height - 1].node, right_cut = stack_elem->node;
                    if (left_cut.ptr == right_cut.ptr && left_cut.numKeys == right_cut.numKeys) {
                        break;
                    }

                    mram_read(&Deref(right_cut.ptr), &wks->node_cache, sizeof(Node));
                    const uint16_t idx_src_begin = stack_elem->nr_passed_children,
                                   nr_keys = stack_elem->node.numKeys - idx_src_begin;
                    for (uint8_t idx_dest = 0;; idx_dest++) {
                        wks->node_cache.inl.children[idx_dest] = wks->node_cache.inl.children[idx_src_begin + idx_dest];
                        if (idx_dest == nr_keys) {
                            break;
                        }
                        wks->node_cache.inl.keys[idx_dest] = wks->node_cache.inl.keys[idx_src_begin + idx_dest];
                    }
                    mram_write(&wks->node_cache, &Deref(right_cut.ptr), sizeof(Node));

                    const NodeLink new_link = {right_cut.ptr, nr_keys};
                    if (stack_height == 1) {
                        cold_root.inl.children[nr_passed_children_of_root] = new_link;
                    } else {
                        ExtractStackElem* const parent_stack_elem = &wks->stack[stack_height - 2];
                        const uint16_t idx_as_child = parent_stack_elem->nr_passed_children;
                        parent_stack_elem->children_cache[idx_as_child % 2] = new_link;
                        mram_write(&parent_stack_elem->children_cache[0], &Deref(parent_stack_elem->node.ptr).inl.children[idx_as_child / 2 * 2], sizeof(NodeLink) * 2);
                    }
                }

                bool is_child_alive = false;
                for (uint8_t initial_stack_height = cold_height - 1; initial_stack_height != stack_height; initial_stack_height--) {
                    ExtractStackElem* const stack_elem = &wks->initial_stack[initial_stack_height - 1];
                    const uint16_t nr_alive_children = stack_elem->nr_passed_children + is_child_alive;
                    if (nr_alive_children == 0) {
                        Free_node(stack_elem->node.ptr);
                    } else {
                        is_child_alive = true;
                        const NodeLink new_link = {stack_elem->node.ptr, nr_alive_children - 1};
                        if (initial_stack_height == 0) {
                            cold_root.inl.children[initial_nr_passed_children_of_root] = new_link;
                        } else {
                            ExtractStackElem* const parent_stack_elem = &wks->initial_stack[initial_stack_height - 2];
                            const uint16_t idx_as_child = parent_stack_elem->nr_passed_children;
                            parent_stack_elem->children_cache[idx_as_child % 2] = new_link;
                            mram_write(&parent_stack_elem->children_cache[0], &Deref(parent_stack_elem->node.ptr).inl.children[idx_as_child / 2 * 2], sizeof(NodeLink) * 2);
                        }
                    }
                }

                if (stack_height == 0) {
                    const uint16_t nr_left_alive_children = initial_nr_passed_children_of_root + is_child_alive,
                                   idx_right_alive_children_begin = nr_passed_children_of_root,
                                   nr_right_alive_children = cold_root_numKeys + 1 - idx_right_alive_children_begin,
                                   nr_alive_children = nr_left_alive_children + nr_right_alive_children;
                    if (nr_alive_children == 0) {
                        cold_height = 0;
                        cold_root_numKeys = 0;
                    } else {
                        if (nr_right_alive_children != 0) {
                            if (nr_left_alive_children == 0) {
                                cold_min_key = next_key;
                            } else {
                                cold_root.inl.keys[nr_left_alive_children - 1] = next_key;
                            }
                            const uint16_t move_offset = idx_right_alive_children_begin - nr_left_alive_children;
                            for (uint16_t idx_dest = nr_left_alive_children, idx_src = idx_right_alive_children_begin;; idx_dest++, idx_src++) {
                                cold_root.inl.children[idx_src - move_offset] = cold_root.inl.children[idx_src];
                                if (idx_src == cold_root_numKeys) {
                                    break;
                                }
                                cold_root.inl.keys[idx_src - move_offset] = cold_root.inl.keys[idx_src];
                            }
                        }
                        cold_root_numKeys = (uint8_t)(nr_alive_children - 1);
                    }
                } else {
                    ExtractStackElem* const initial_stack_elem = &wks->initial_stack[stack_height - 1];
                    ExtractStackElem* const stack_elem = &wks->stack[stack_height - 1];
                    mram_read(&Deref(stack_elem->node.ptr), &wks->node_cache, sizeof(Node));

                    const uint16_t nr_left_alive_children = initial_stack_elem->nr_passed_children + is_child_alive,
                                   idx_right_alive_children_begin = stack_elem->nr_passed_children,
                                   nr_right_alive_children = stack_elem->node.numKeys + 1 - idx_right_alive_children_begin,
                                   nr_alive_children = nr_left_alive_children + nr_right_alive_children;
                    const NodeLink new_link = {stack_elem->node.ptr, nr_alive_children - 1};
                    if (stack_height == 1) {
                        cold_root.inl.children[nr_passed_children_of_root] = new_link;
                    } else {
                        ExtractStackElem* const parent_stack_elem = &wks->stack[stack_height - 2];
                        const uint16_t idx_as_child = parent_stack_elem->nr_passed_children;
                        parent_stack_elem->children_cache[idx_as_child % 2] = new_link;
                        mram_write(&parent_stack_elem->children_cache[0], &Deref(parent_stack_elem->node.ptr).inl.children[idx_as_child / 2 * 2], sizeof(NodeLink) * 2);
                    }
                    if (nr_left_alive_children == 0) {
                        for (;;) {
                            stack_height--;
                            if (stack_height != 0) {
                                ExtractStackElem* const ancestor_stack_elem = &wks->stack[stack_height - 1];
                                if (ancestor_stack_elem->nr_passed_children == 0) {
                                    continue;
                                }
                                mram_write(&next_key, &Deref(ancestor_stack_elem->node.ptr).inl.keys[ancestor_stack_elem->nr_passed_children - 1], sizeof(key_uint64_t));
                            } else {
                                if (nr_passed_children_of_root == 0) {
                                    cold_min_key = next_key;
                                } else {
                                    cold_root.inl.keys[nr_passed_children_of_root - 1] = next_key;
                                }
                            }
                            break;
                        }
                    } else {
                        wks->node_cache.inl.keys[nr_left_alive_children - 1] = next_key;
                    }
                    const uint16_t move_offset = idx_right_alive_children_begin - nr_left_alive_children;
                    for (uint16_t idx_dest = nr_left_alive_children, idx_src = idx_right_alive_children_begin;; idx_dest++, idx_src++) {
                        wks->node_cache.inl.children[idx_src - move_offset] = wks->node_cache.inl.children[idx_src];
                        if (idx_src == stack_elem->node.numKeys) {
                            break;
                        }
                        wks->node_cache.inl.keys[idx_src - move_offset] = wks->node_cache.inl.keys[idx_src];
                    }
                    mram_write(&wks->node_cache, &Deref(stack_elem->node.ptr), sizeof(Node));
                }
            }
            if (idx_hot % 2 != 0) {
                mram_write(&wks->nr_pairs_cache[0], (__mram_ptr uint32_t*)(result_nr_pairs + sizeof(uint32_t) * idx_hot - 4), sizeof(uint32_t) * 2);
            }
        }
    }
}

void task_extract(void)
{
    EXTRACT_nodes();

#ifdef TASK_EXTRACT_CHECK
    if (me() == 0) {
        check_tree_structure(&cold_root, cold_height, cold_root_numKeys);
    }
#endif
}


static NodePtr CONSTRUCT_HOT_allocator(unsigned idx_node)
{
    (void)idx_node;
    return Allocate_node();
}
void task_construct_hot(void)
{
    _Static_assert(TREE_CONSTRUCT_NR_TASKLETS > 0, "TREE_CONSTRUCT_NR_TASKLETS > 0");
    if (me() < TREE_CONSTRUCT_NR_TASKLETS) {
        construct_tree(&hot_root_numKeys, &hot_root, &hot_height, &hot_min_key, CONSTRUCT_HOT_allocator);

#ifdef TASK_CONSTRUCT_HOT_CHECK
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

    for (unsigned i = 1; i < link.numKeys; i++) {
        if (internal.inl.keys[i - 1] >= internal.inl.keys[i]) {
            success = false;
            printf("Node[%u].keys[%u] == %lu >= %lu == Node[%u].keys[%u]\n", link.ptr, i - 1, internal.inl.keys[i - 1], internal.inl.keys[i], link.ptr, i);
        }
    }
    for (unsigned i = 0; i <= link.numKeys; i++) {
        const NodeLink child_link = internal.inl.children[i];
        if (child_link.ptr == NODE_NULLPTR) {
            success = false;
            printf("Node[%u].children[%u] == null\n", link.ptr, i);
        } else {
            __dma_aligned key_uint64_t key_in_child;
            if (i != 0) {
                mram_read((is_child_leaf ? &Deref(child_link.ptr).lf.keys[0]
                                         : &Deref(child_link.ptr).inl.keys[0]),
                    &key_in_child, sizeof(key_uint64_t));
                if (is_child_leaf) {
                    if (internal.inl.keys[i - 1] > key_in_child) {
                        success = false;
                        printf("Node[%u].keys[%u] > Node[%u].children[%u]->keys[0]\n", link.ptr, i - 1, link.ptr, i);
                    }
                } else {
                    if (internal.inl.keys[i - 1] >= key_in_child) {
                        success = false;
                        printf("Node[%u].keys[%u] >= Node[%u].children[%u]->keys[0]\n", link.ptr, i - 1, link.ptr, i);
                    }
                }
            }
            if (i != link.numKeys) {
                mram_read((is_child_leaf ? &Deref(child_link.ptr).lf.keys[child_link.numKeys - 1]
                                         : &Deref(child_link.ptr).inl.keys[child_link.numKeys - 1]),
                    &key_in_child, sizeof(key_uint64_t));
                if (internal.inl.keys[i] <= key_in_child) {
                    success = false;
                    printf("Node[%u].keys[%u] <= Node[%u].children[%u]->keys[%u]\n", link.ptr, i, link.ptr, i, child_link.numKeys - 1);
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
        for (unsigned i = 1; i < root_numKeys; i++) {
            if (root->lf.keys[i - 1] >= root->lf.keys[i]) {
                success = false;
                printf("Root.keys[%u] == %lu >= %lu == Root.keys[%u]\n", i - 1, root->lf.keys[i - 1], root->lf.keys[i], i);
            }
        }
    } else {
        for (unsigned idx_child_of_root = 0; idx_child_of_root <= root_numKeys; idx_child_of_root++) {
            stack[0].link = root->inl.children[idx_child_of_root];
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
                        stack[stack_height].link = Deref(stack[stack_height - 1].link.ptr).inl.children[nr_visited];
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
            const NodeLink child_link = root->inl.children[i];
            if (child_link.ptr == NODE_NULLPTR) {
                success = false;
                printf("Root.children[%u] == null\n", i);
            } else {
                __dma_aligned key_uint64_t key_in_child;
                if (i != 0) {
                    mram_read((is_child_of_root_leaf ? &Deref(child_link.ptr).lf.keys[0]
                                                     : &Deref(child_link.ptr).inl.keys[0]),
                        &key_in_child, sizeof(key_uint64_t));
                    if (is_child_of_root_leaf) {
                        if (root->inl.keys[i - 1] > key_in_child) {
                            success = false;
                            printf("Root.keys[%u] > Root.children[%u]->keys[0]\n", i - 1, i);
                        }
                    } else {
                        if (root->inl.keys[i - 1] >= key_in_child) {
                            success = false;
                            printf("Root.keys[%u] >= Root.children[%u]->keys[0]\n", i - 1, i);
                        }
                    }
                }
                if (i != root_numKeys) {
                    mram_read((is_child_of_root_leaf ? &Deref(child_link.ptr).lf.keys[child_link.numKeys - 1]
                                                     : &Deref(child_link.ptr).inl.keys[child_link.numKeys - 1]),
                        &key_in_child, sizeof(key_uint64_t));
                    if (root->inl.keys[i] <= key_in_child) {
                        success = false;
                        printf("Root.keys[%u] <= Root.children[%u]->keys[%u]\n", i, i, child_link.numKeys - 1);
                    }
                }
            }
        }
    }
    return success;
}
