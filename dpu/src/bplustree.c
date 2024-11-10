#include "bplustree.h"

#include "allocator.h"
#include "bit_ops_macro.h"
#include "common.h"
#include "div_by_const.h"
#include "input_header.h"
#include "node_ptr.h"
#include "tree.h"
#include "workload_types.h"

#include <attributes.h>
#include <defs.h>
#include <mram.h>

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#define DEBUG_PRINT(datum) printf("th[%02d] " __FILE__ ":%d: " #datum " = %u (0x%x)\n", me(), __LINE__, datum, datum)


#ifndef TASK_INIT_NR_TASKLETS
#define TASK_INIT_NR_TASKLETS NR_TASKLETS
#endif

#ifndef TASK_INIT_NR_CACHED_KVPAIRS
#define TASK_INIT_NR_CACHED_KVPAIRS 1
#endif

#ifndef TASK_INIT_NR_CACHED_INPUT_LIFT
#define TASK_INIT_NR_CACHED_INPUT_LIFT 2
#endif

#ifndef TASK_INIT_NR_CACHED_OUTPUT_LIFT
#define TASK_INIT_NR_CACHED_OUTPUT_LIFT (MAX_NR_CHILDREN / 2 * 2)
#endif

#ifndef TASK_INIT_NR_CACHED_NODES
#define TASK_INIT_NR_CACHED_NODES 1
#endif


static DEFINE_DIV_BY(MAX_NR_PAIRS, BITWIDTH_UINT32(MAX_NR_PAIRS* MAX_NUM_NODES_IN_DPU), _NR_PAIRS);
static DEFINE_DIV_BY(MAX_NR_CHILDREN, NODE_PTR_WIDTH, _NR_NODES);

static DEFINE_DIV_BY(TASK_INIT_NR_TASKLETS, NODE_PTR_WIDTH, _NR_NODES);
static DEFINE_DIV_BY(TASK_INIT_NR_CACHED_OUTPUT_LIFT, NODE_PTR_WIDTH, _NR_NODES);

// HEIGHT <= log_{MIN_NR_CHILDREN} [ (MAX_NUM_NODES_IN_DPU - 1) * (MIN_NR_CHILDREN - 1) / 2.0 + 1 ]
#define MAX_HEIGHT ((CEIL_LOG2_UINT32((MAX_NUM_NODES_IN_DPU - 1) * (MIN_NR_CHILDREN - 1) + 2) - 1) / FLOOR_LOG2_UINT32(MIN_NR_CHILDREN))

Node cold_root;
uint8_t cold_height;
uint8_t cold_root_numKeys;

static uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];

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

union {
    InitWorkspace init[TASK_INIT_NR_TASKLETS];
} workspace;


__attribute__((unused)) static bool TreeCheckStructure(Node* root, unsigned height, unsigned root_numKeys);


static unsigned distribution_begin(unsigned per_tasklet, unsigned remainder, unsigned tasklet_id)
{
    return per_tasklet * tasklet_id + (tasklet_id <= remainder ? tasklet_id : remainder);
}
static unsigned distribution_length(unsigned per_tasklet, unsigned remainder, unsigned tasklet_id)
{
    return per_tasklet + (tasklet_id < remainder);
}

//! @sa /docs/tree_initialization.md
//! @return Sum of nr. of KV pairs that [0, idx_leaf)-th leaves have
static unsigned INIT_idx_leaf_to_idx_pair(unsigned idx_leaf, unsigned nr_leaves, bool is_2nd_last_leaf_not_full, unsigned nr_pairs)
{
    return (idx_leaf + is_2nd_last_leaf_not_full < nr_leaves ? idx_leaf * MAX_NR_PAIRS
                                                             : (idx_leaf == nr_leaves ? nr_pairs
                                                                                      : nr_pairs - MIN_NR_PAIRS));
}
//! @sa /docs/tree_initialization.md
//! @return Sum of nr. of children that [0, idx_parent)-th parents have
static unsigned INIT_idx_parent_to_idx_child(unsigned idx_parent, unsigned nr_parents, bool is_2nd_last_parent_not_full, unsigned nr_children)
{
    return (idx_parent + is_2nd_last_parent_not_full < nr_parents ? idx_parent * MAX_NR_CHILDREN
                                                                  : (idx_parent == nr_parents ? nr_children
                                                                                              : nr_children - MIN_NR_CHILDREN));
}
//! @sa /docs/tree_initialization.md
//! @return max{ i | INIT_idx_parent_to_idx_child(i, nr_parents, _) <= idx_chlid }
static unsigned INIT_idx_child_to_idx_parent(unsigned idx_child, unsigned nr_children, unsigned nr_parents)
{
    return (idx_child + MIN_NR_CHILDREN < nr_children ? DIV_NR_NODES_BY_MAX_NR_CHILDREN(idx_child)
                                                      : (idx_child < nr_children ? nr_parents - 1
                                                                                 : nr_parents));
}

static void INIT_notify_ready_for_out_lifted(void)
{
    __asm__("acquire id, %[base], t, .+1\n"
            "resume id, 1" ::[base] "i"(&AtomicBits)
            :);
}
static void INIT_wait_for_out_lifted_ready(void)
{
    __asm__("0:\n"
            "release id, %[base] - 1, nz, .+2\n"
            "stop true, 0b" ::[base] "i"(&AtomicBits)
            :);
}

static void INIT_notify_end_of_use_of_out_lifted(void)
{
    __asm__("acquire id, %[base] + %[nr_tasklets], true, .+1\n"
            "resume id, -1" ::[base] "i"(&AtomicBits), [nr_tasklets] "i"(TASK_INIT_NR_TASKLETS)
            :);
}
static void INIT_wait_for_end_of_use_of_out_lifted(void)
{
    __asm__("0:\n"
            "release id, %[base] + %[nr_tasklets] + 1, nz, .+2\n"
            "stop true, 0b" ::[base] "i"(&AtomicBits),
            [nr_tasklets] "i"(TASK_INIT_NR_TASKLETS)
            :);
}

static void INIT_receive_lifted_links_from_junior(unsigned nr_children_received, Node* dest_node, key_uint64_t* key_min_subtree)
{
    unsigned idx_junior = me() - 1, nr_lift_left_in_this_junior = workspace.init[idx_junior].out.nr_cached_lift;
    for (unsigned idx_child_in_this_node_p1 = nr_children_received; idx_child_in_this_node_p1 > 0; idx_child_in_this_node_p1--, nr_lift_left_in_this_junior--) {
        while (nr_lift_left_in_this_junior == 0) {
            idx_junior--;
            nr_lift_left_in_this_junior = workspace.init[idx_junior].out.nr_cached_lift;
        }
        const unsigned idx_child_in_this_node = idx_child_in_this_node_p1 - 1;
        const LinkLift* const p_lift = &workspace.init[idx_junior].out.lifted[nr_lift_left_in_this_junior - 1];
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
void task_init(void)
{
    static const uintptr_t initial_pairs = (uintptr_t)DPU_MRAM_HEAP_POINTER + 8;

    _Static_assert(TASK_INIT_NR_TASKLETS > 0, "TASK_INIT_NR_TASKLETS > 0");
    if (me() < TASK_INIT_NR_TASKLETS) {
        InitWorkspace* const wks = &workspace.init[me()];

        const unsigned nr_pairs = input_header.init.nr_pairs;
        if (nr_pairs == 0) {
            if (me() == 0) {
                cold_height = 0;
                cold_root_numKeys = 0;
            }
            return;
        }

        assert(nr_pairs <= (MAX_NR_PAIRS * MAX_NUM_NODES_IN_DPU));
        unsigned nr_nodes = DIV_NR_PAIRS_BY_MAX_NR_PAIRS(nr_pairs + MAX_NR_PAIRS - 1);
        Node* node_cache = (nr_nodes == 1 ? &cold_root : &wks->nodes[0]);

        // Distribute the leaf initialization task among the tasklets
        const unsigned nr_leaves_per_tasklet = DIV_NR_NODES_BY_TASK_INIT_NR_TASKLETS(nr_nodes),
                       nr_remainder_leaves = nr_nodes - nr_leaves_per_tasklet * TASK_INIT_NR_TASKLETS,
                       nr_leaves_for_me = distribution_length(nr_leaves_per_tasklet, nr_remainder_leaves, me());
        unsigned idx_node_begin = distribution_begin(nr_leaves_per_tasklet, nr_remainder_leaves, me()),
                 idx_node_end = idx_node_begin + nr_leaves_for_me;

        // Correspondence between the distributed leaves and KV pairs
        const bool is_2nd_last_node_not_full = nr_nodes > 1u && MAX_NR_PAIRS * (nr_nodes - 1u) + MIN_NR_PAIRS > nr_pairs;
        unsigned idx_pair = INIT_idx_leaf_to_idx_pair(idx_node_begin, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
        const uintptr_t pairs_for_me = initial_pairs + sizeof(KVPair) * idx_pair;

        // Distribute the initialization task of 2nd layer among the tasklets
        unsigned nr_parents = DIV_NR_NODES_BY_MAX_NR_CHILDREN(nr_nodes + MAX_NR_CHILDREN - 1),
                 idx_parent_begin = INIT_idx_child_to_idx_parent(idx_node_begin, nr_nodes, nr_parents),
                 idx_parent_end = INIT_idx_child_to_idx_parent(idx_node_end, nr_nodes, nr_parents);

        // Correspondence between the distributed 2nd layer and leaves
        bool is_2nd_last_parent_not_full = nr_parents > 1u && MAX_NR_CHILDREN * (nr_parents - 1u) + MIN_NR_CHILDREN > nr_nodes;
        unsigned idx_node_begin_used_by_me = INIT_idx_parent_to_idx_child(idx_parent_begin, nr_parents, is_2nd_last_parent_not_full, nr_nodes),
                 idx_node_end_used_by_me = INIT_idx_parent_to_idx_child(idx_parent_end, nr_parents, is_2nd_last_parent_not_full, nr_nodes);

        // Data move required to resolve the mismatch in the distribution of the leaf and 2nd layer.
        bool is_any_node_sent_from_junior_to_senior = idx_node_end_used_by_me < idx_node_begin;
        const unsigned idx_node_begin_sent_to_senior = (is_any_node_sent_from_junior_to_senior ? idx_node_begin : idx_node_end_used_by_me),
                       nr_nodes_not_sent = idx_node_begin_sent_to_senior - idx_node_begin;

        // Cursors on WRAM cache
        unsigned idx_pair_cache = TASK_INIT_NR_CACHED_KVPAIRS;
        unsigned idx_node_cache = 0;
        // To place (idx_node_begin_sent_to_senior)-th node in wks->out.lifted[0], where should (idx_node_begin)-th be placed?
        //     -> wks->out.lifted[idx_lift_cache_begin]
        unsigned idx_lift_cache_begin = DIV_NR_NODES_BY_TASK_INIT_NR_CACHED_OUTPUT_LIFT(nr_nodes_not_sent + TASK_INIT_NR_CACHED_OUTPUT_LIFT - 1u)
                                            * TASK_INIT_NR_CACHED_OUTPUT_LIFT
                                        - nr_nodes_not_sent,
                 idx_lift_cache = idx_lift_cache_begin;

        // Place the information lifted to the 2nd layer in the place where the KV pairs were
        unsigned lifted_links_offset = (idx_lift_cache_begin % 2) * 4;  // (idx_lift_cache_begin % 2 != 0 ? 4 : 0)
        uintptr_t lifted_links = pairs_for_me + lifted_links_offset;

        if (idx_node_begin < idx_node_end) {
            unsigned idx_node = idx_node_begin;
            unsigned idx_pair_end_for_this_node = INIT_idx_leaf_to_idx_pair(idx_node + 1, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
            NodePtr left_node = (idx_node == 0 ? NODE_NULLPTR : idx_node - 1);
            NodeLink link_to_this_node = {idx_node, idx_pair_end_for_this_node - idx_pair};

            for (; idx_node < idx_node_end; idx_node++) {
                {
                    if (idx_node_cache == TASK_INIT_NR_CACHED_NODES) {
                        mram_write(&node_cache[0], &Deref(idx_node - idx_node_cache), sizeof(Node) * idx_node_cache);
                        idx_node_cache = 0;
                    }

                    for (unsigned idx_pair_in_this_node = 0; idx_pair < idx_pair_end_for_this_node; idx_pair++, idx_pair_in_this_node++) {
                        if (idx_pair_cache == TASK_INIT_NR_CACHED_KVPAIRS) {
                            mram_read((__mram_ptr KVPair*)(initial_pairs + sizeof(KVPair) * idx_pair), &wks->in.pairs[0],
                                sizeof(KVPair) * TASK_INIT_NR_CACHED_KVPAIRS);
                            idx_pair_cache = 0;
                        }
                        node_cache[idx_node_cache].lf.keys[idx_pair_in_this_node] = wks->in.pairs[idx_pair_cache].key;
                        node_cache[idx_node_cache].lf.values[idx_pair_in_this_node] = wks->in.pairs[idx_pair_cache].value;
                        idx_pair_cache++;
                    }
                    node_cache[idx_node_cache].lf.left = left_node;

                    const key_uint64_t first_key = node_cache[idx_node_cache].lf.keys[0];
                    wks->out.lifted[idx_lift_cache] = (LinkLift){{(uint32_t)(first_key >> 32), (uint32_t)first_key}, link_to_this_node};
                    idx_lift_cache++;

                    if (idx_lift_cache == TASK_INIT_NR_CACHED_OUTPUT_LIFT) {
                        _Static_assert((TASK_INIT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TASK_INIT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                        const uintptr_t off_for_alignment = (idx_lift_cache_begin % 2) * 4;
                        const unsigned size = sizeof(LinkLift) * (idx_lift_cache - idx_lift_cache_begin) + off_for_alignment;
                        mram_write((void*)((uintptr_t)(&wks->out.lifted[idx_lift_cache_begin]) - off_for_alignment),
                            (__mram_ptr void*)(lifted_links + sizeof(LinkLift) * (idx_node + 1 - idx_node_begin) - size),
                            size);
                        idx_lift_cache_begin = idx_lift_cache = 0;
                    }
                }

                idx_pair_end_for_this_node = INIT_idx_leaf_to_idx_pair((idx_node + 1) + 1, nr_nodes, is_2nd_last_node_not_full, nr_pairs);
                left_node = idx_node;
                link_to_this_node = (NodeLink){idx_node + 1, idx_pair_end_for_this_node - idx_pair};

                {
                    node_cache[idx_node_cache].lf.right = link_to_this_node;
                    idx_node_cache++;
                }
            }
            if (idx_node_end == nr_nodes) {
                node_cache[idx_node_cache - 1].lf.right = NODELINK_NULLPTR;
            }
            if (nr_nodes != 1) {  // otherwise, directly written to root in WRAM
                mram_write(&node_cache[0], &Deref(idx_node_end - idx_node_cache), sizeof(Node) * idx_node_cache);
            }
        }


        uint8_t tmp_height = 0;
        NodePtr nr_nodes_in_lower = nr_nodes;
        for (;; tmp_height++, nr_nodes_in_lower += nr_nodes) {
            _Static_assert(TASK_INIT_NR_CACHED_OUTPUT_LIFT >= MAX_NR_CHILDREN - 1, "TASK_INIT_NR_CACHED_OUTPUT_LIFT >= MAX_NR_CHILDREN - 1");
            wks->out.nr_cached_lift = idx_lift_cache;

            if (nr_nodes == 1) {
                if (idx_node_begin != idx_node_end) {
                    cold_height = tmp_height;
                    cold_root_numKeys = wks->out.lifted[TASK_INIT_NR_CACHED_OUTPUT_LIFT - 1].child.numKeys;
#ifdef TASK_INIT_CHECK
                    TreeCheckStructure(&cold_root, cold_height, cold_root_numKeys);
#endif
                }
                return;
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
            idx_parent_begin = INIT_idx_child_to_idx_parent(idx_node_begin, nr_nodes, nr_parents);
            idx_parent_end = INIT_idx_child_to_idx_parent(idx_node_end, nr_nodes, nr_parents);

            // Correspondence between the distributions of this layer and the next layer up
            is_2nd_last_parent_not_full = nr_parents > 1u && MAX_NR_CHILDREN * (nr_parents - 1u) + MIN_NR_CHILDREN > nr_nodes;
            idx_node_begin_used_by_me = INIT_idx_parent_to_idx_child(idx_parent_begin, nr_parents, is_2nd_last_parent_not_full, nr_nodes);
            idx_node_end_used_by_me = INIT_idx_parent_to_idx_child(idx_parent_end, nr_parents, is_2nd_last_parent_not_full, nr_nodes);

            // Data move required to resolve the mismatch in the distribution of this layer and the next layer up
            is_any_node_sent_from_junior_to_senior = idx_node_end_used_by_me < idx_node_begin;
            idx_lift_cache = 0;  // reset as default

            if (is_any_child_sent_from_junior_to_senior) {
                INIT_wait_for_out_lifted_ready();
                INIT_notify_ready_for_out_lifted();
                INIT_wait_for_end_of_use_of_out_lifted();
                INIT_notify_end_of_use_of_out_lifted();
                continue;

            } else {
                if (idx_child_end != idx_child_end_from_me) {
                    INIT_notify_ready_for_out_lifted();
                }

                if (nr_nodes == 1) {
                    node_cache = &cold_root;
                }

                key_uint64_t key_min_subtree;

                const unsigned nr_children_received = idx_child_begin_from_me - idx_child_begin;
                if (nr_children_received != 0) {
                    INIT_wait_for_out_lifted_ready();
                    INIT_receive_lifted_links_from_junior(nr_children_received, &node_cache[0], &key_min_subtree);
                    INIT_notify_end_of_use_of_out_lifted();
                }
                if (idx_child_end != idx_child_end_from_me) {
                    INIT_wait_for_end_of_use_of_out_lifted();
                }

                if (idx_node_begin < idx_node_end) {
                    // Correspondence between the distributed nodes and children
                    unsigned idx_child = idx_child_begin_from_me;

                    // Data move required to resolve the mismatch in the distribution of this layer and the next layer up
                    const unsigned idx_node_begin_sent_to_senior = (is_any_node_sent_from_junior_to_senior ? idx_node_begin : idx_node_end_used_by_me),
                                   nr_nodes_not_sent = idx_node_begin_sent_to_senior - idx_node_begin;

                    // Cursors on WRAM cache
                    unsigned idx_child_cache = (lifted_links_offset != 0);
                    unsigned idx_node_cache = 0;
                    // To place (idx_node_begin_sent_to_senior)-th node in wks->out.lifted[0], where should (idx_node_begin)-th be placed?
                    //     -> wks->out.lifted[idx_lift_cache_begin]
                    unsigned idx_lift_cache_begin = DIV_NR_NODES_BY_TASK_INIT_NR_CACHED_OUTPUT_LIFT(nr_nodes_not_sent + TASK_INIT_NR_CACHED_OUTPUT_LIFT - 1u)
                                                        * TASK_INIT_NR_CACHED_OUTPUT_LIFT
                                                    - nr_nodes_not_sent;
                    idx_lift_cache = idx_lift_cache_begin;

                    const unsigned incoming_links = lifted_links;
                    {  // Fetch the links to children before rewriting lifted_links_offset
                        _Static_assert((TASK_INIT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TASK_INIT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                        mram_read((__mram_ptr void*)(incoming_links - lifted_links_offset),
                            (void*)((uintptr_t)(&wks->in.lifted[idx_child_cache]) - lifted_links_offset),
                            sizeof(LinkLift) * (TASK_INIT_NR_CACHED_INPUT_LIFT - idx_child_cache) + lifted_links_offset);
                    }
                    // Place the information lifted to the 2nd layer in the place where the KV pairs were
                    lifted_links_offset = (idx_lift_cache_begin % 2) * 4;
                    lifted_links = pairs_for_me + lifted_links_offset;

                    unsigned idx_child_in_this_node = nr_children_received;

                    for (unsigned idx_node = idx_node_begin; idx_node < idx_node_end; idx_node++) {
                        if (idx_node_cache == TASK_INIT_NR_CACHED_NODES) {
                            mram_write(&node_cache[0], &Deref(nr_nodes_in_lower + idx_node - idx_node_cache), sizeof(Node) * idx_node_cache);
                            idx_node_cache = 0;
                        }

                        const unsigned idx_child_end_for_this_node = INIT_idx_parent_to_idx_child(idx_node + 1, nr_nodes, is_2nd_last_node_not_full, nr_children);

                        for (; idx_child < idx_child_end_for_this_node; idx_child++, idx_child_in_this_node++) {
                            if (idx_child_cache == TASK_INIT_NR_CACHED_INPUT_LIFT) {
                                _Static_assert((TASK_INIT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TASK_INIT_NR_CACHED_INPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                                mram_read((__mram_ptr void*)(incoming_links + sizeof(LinkLift) * (idx_child - idx_child_begin_from_me)),
                                    &wks->in.lifted[0],
                                    sizeof(LinkLift) * TASK_INIT_NR_CACHED_INPUT_LIFT);
                                idx_child_cache = 0;
                            }
                            const LinkLift* const p_lift = &wks->in.lifted[idx_child_cache];
                            const key_uint64_t key = ((key_uint64_t)p_lift->key_parts[0] << 32) + p_lift->key_parts[1];
                            if (idx_child_in_this_node == 0) {
                                key_min_subtree = key;
                            } else {
                                node_cache[idx_node_cache].inl.keys[idx_child_in_this_node - 1] = key;
                            }
                            node_cache[idx_node_cache].inl.children[idx_child_in_this_node] = p_lift->child;
                            idx_child_cache++;
                        }
                        idx_node_cache++;

                        const NodeLink link_to_this_node = {nr_nodes_in_lower + idx_node, idx_child_in_this_node - 1};
                        idx_child_in_this_node = 0;

                        wks->out.lifted[idx_lift_cache] = (LinkLift){{(uint32_t)(key_min_subtree >> 32), (uint32_t)key_min_subtree}, link_to_this_node};
                        idx_lift_cache++;
                        if (idx_lift_cache == TASK_INIT_NR_CACHED_OUTPUT_LIFT) {
                            _Static_assert((TASK_INIT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0, "(TASK_INIT_NR_CACHED_OUTPUT_LIFT * sizeof(LinkLift)) % 8 == 0");
                            const uintptr_t off_for_alignment = (idx_lift_cache_begin % 2) * 4;
                            const unsigned size = sizeof(LinkLift) * (idx_lift_cache - idx_lift_cache_begin) + off_for_alignment;
                            mram_write((void*)((uintptr_t)(&wks->out.lifted[idx_lift_cache_begin]) - off_for_alignment),
                                (__mram_ptr void*)(lifted_links + sizeof(LinkLift) * (idx_node + 1 - idx_node_begin) - size),
                                size);
                            idx_lift_cache_begin = idx_lift_cache = 0;
                        }
                    }
                    if (nr_nodes != 1) {  // otherwise, directly written to root in WRAM
                        mram_write(&node_cache[0], &Deref(nr_nodes_in_lower + idx_node_end - idx_node_cache), sizeof(Node) * idx_node_cache);
                    }
                }
            }
        }
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
    }
    const NodeLink right = leaf.lf.right;
    if (right.ptr != NODELINK_NULLPTR.ptr) {
        union {
            NodePtr ptr_from_right;
            __dma_aligned uint64_t aligner;
        } cache;
        mram_read(&Deref(right.ptr).lf.left, &cache, 8);
        if (cache.ptr_from_right != link.ptr) {
            success = false;
            printf("Node[%u].right->left == %u != %u\n", link.ptr, cache.ptr_from_right, link.ptr);
        }
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

static bool TreeCheckStructure(Node* root, unsigned height, unsigned root_numKeys)
{
    bool success = true;

    struct {
        NodeLink link;
        unsigned nr_visited_children;
    } stack[MAX_HEIGHT];
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
