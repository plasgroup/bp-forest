#include "bplustree.h"

#include "allocator.h"
#include "common.h"
#include "tree.h"
#include "workload_types.h"

#include <attributes.h>
#include <mram.h>

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>


#define MAX_HEIGHT (CEIL_LOG2_UINT32((uint32_t)(MAX_NUM_NODES_IN_DPU)) / FLOOR_LOG2_UINT32((uint32_t)(MAX_NR_CHILDREN)) + 1)

NodePtr root;

__host uint32_t num_kvpairs;


// binary search
#ifndef USE_LINEAR_SEARCH
static unsigned findUpperBound(key_int64_t __mram_ptr* keys, unsigned size, key_int64_t key)
{
    int l = -1, r = (int)size;
    while (l < r - 1) {
        int mid = (l + r) / 2;
        if (keys[mid] > key)
            r = mid;
        else
            l = mid;
    }
    return (unsigned)r;
}
__attribute__((unused)) static unsigned findUpperBoundWRAM(key_int64_t* keys, unsigned size, key_int64_t key)
{
    int l = -1, r = (int)size;
    while (l < r - 1) {
        int mid = (l + r) / 2;
        if (keys[mid] > key)
            r = mid;
        else
            l = mid;
    }
    return (unsigned)r;
}
#endif

#ifdef USE_LINEAR_SEARCH
// linear search
static unsigned findUpperBound(key_int64_t __mram_ptr* keys, unsigned size, key_int64_t key)
{
    unsigned ret = 0;
    for (; ret < size; ret++) {
        if (keys[ret] > key)
            return ret;
    }
    return ret;
}
#endif
void init_Tree(void)
{
    Allocator_reset();
    root = Allocate_node();
    Deref(root).header.isLeaf = true;
    Deref(root).header.numKeys = 0;
    Deref(root).body.lf.right = NODE_NULLPTR;
    Deref(root).body.lf.left = NODE_NULLPTR;
    num_kvpairs = 0;
}
/**
 * @brief 
 * If key already exists in the tree, update value and return true. 
 * If not, insert key and return false.
 * @param key key to insert
 * @param value value related to key
 * @return Whether key is updated
 */
void TreeInsert(__dma_aligned key_int64_t key, __dma_aligned value_ptr_t value)
{
    uint8_t idx_child_cache[MAX_HEIGHT];
    unsigned depth = 0;

    NodePtr node = root;
    while (!Deref(node).header.isLeaf) {
        const unsigned idx_child = findUpperBound(Deref(node).body.inl.keys, Deref(node).header.numKeys, key);
        idx_child_cache[depth++] = (uint8_t)idx_child;
        node = Deref(node).body.inl.children[idx_child].ptr;
    }

    const unsigned idx_to_insert = findUpperBound(Deref(node).body.lf.keys, Deref(node).header.numKeys, key);
    if (idx_to_insert != 0 && Deref(node).body.lf.keys[idx_to_insert - 1] == key) {
        Deref(node).body.lf.values[idx_to_insert - 1] = value;
        return;
    }
    num_kvpairs++;

    if (Deref(node).header.numKeys != MAX_NR_PAIRS) {
        if (idx_to_insert == Deref(node).header.numKeys) {
            mram_write(&key, &Deref(node).body.lf.keys[idx_to_insert], sizeof(key_int64_t));
            mram_write(&value, &Deref(node).body.lf.values[idx_to_insert], sizeof(value_ptr_t));
        } else {
            {
                __dma_aligned key_int64_t moved_keys[MAX_NR_PAIRS];
                mram_read(&Deref(node).body.lf.keys[idx_to_insert], &moved_keys[1], sizeof(key_int64_t) * (Deref(node).header.numKeys - idx_to_insert));
                moved_keys[0] = key;
                mram_write(&moved_keys[0], &Deref(node).body.lf.keys[idx_to_insert], sizeof(key_int64_t) * (Deref(node).header.numKeys - idx_to_insert + 1));
            }
            {
                __dma_aligned value_ptr_t moved_values[MAX_NR_PAIRS];
                mram_read(&Deref(node).body.lf.values[idx_to_insert], &moved_values[1], sizeof(value_ptr_t) * (Deref(node).header.numKeys - idx_to_insert));
                moved_values[0] = value;
                mram_write(&moved_values[0], &Deref(node).body.lf.values[idx_to_insert], sizeof(value_ptr_t) * (Deref(node).header.numKeys - idx_to_insert + 1));
            }
        }
        Deref(node).header.numKeys += 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
        if (depth != 0) {
            Deref(Deref(node).header.parent).body.inl.children[idx_child_cache[depth - 1]].numKeys = Deref(node).header.numKeys;
        }
#endif

    } else {  // split leaf
        uint8_t node_numKeys = (MAX_NR_PAIRS + 2) / 2, new_node_numKeys = (MAX_NR_PAIRS + 1) / 2;
        bool is_new_node_leaf = true;

        NodePtr new_node = Allocate_node();
        Deref(new_node).header.isLeaf = true;
        Deref(new_node).header.numKeys = new_node_numKeys;
        // Deref(new_node).header.parent = ...  // it depends on whether the parent is to be splitted
        Deref(new_node).body.lf.right = Deref(node).body.lf.right;
        if (Deref(new_node).body.lf.right != NODE_NULLPTR) {
            Deref(Deref(new_node).body.lf.right).body.lf.left = new_node;
        }
        Deref(new_node).body.lf.left = node;

        Deref(node).header.numKeys = node_numKeys;
        Deref(node).body.lf.right = new_node;

        if (idx_to_insert >= node_numKeys) {
            {
                __dma_aligned key_int64_t moved_keys[/* new_node_numKeys */ (MAX_NR_PAIRS + 1) / 2];
                mram_read(&Deref(node).body.lf.keys[node_numKeys], &moved_keys[0], sizeof(key_int64_t) * (new_node_numKeys - 1));
                memmove(&moved_keys[idx_to_insert - node_numKeys + 1], &moved_keys[idx_to_insert - node_numKeys], MAX_NR_PAIRS - idx_to_insert);
                moved_keys[idx_to_insert - node_numKeys] = key;
                key = moved_keys[0];
                mram_write(&moved_keys[0], &Deref(new_node).body.lf.keys[0], sizeof(key_int64_t) * new_node_numKeys);
            }
            {
                __dma_aligned value_ptr_t moved_values[/* new_node_numKeys */ (MAX_NR_PAIRS + 1) / 2];
                mram_read(&Deref(node).body.lf.values[node_numKeys], &moved_values[0], sizeof(value_ptr_t) * (new_node_numKeys - 1));
                memmove(&moved_values[idx_to_insert - node_numKeys + 1], &moved_values[idx_to_insert - node_numKeys], MAX_NR_PAIRS - idx_to_insert);
                moved_values[idx_to_insert - node_numKeys] = value;
                mram_write(&moved_values[0], &Deref(new_node).body.lf.values[0], sizeof(value_ptr_t) * new_node_numKeys);
            }
        } else {
            {
                __dma_aligned key_int64_t moved_keys[MAX_NR_PAIRS + 1];
                mram_read(&Deref(node).body.lf.keys[idx_to_insert], &moved_keys[1], sizeof(key_int64_t) * (MAX_NR_PAIRS - idx_to_insert));
                moved_keys[0] = key;
                key = moved_keys[node_numKeys - 1 - idx_to_insert];
                mram_write(&moved_keys[node_numKeys - 1 - idx_to_insert], &Deref(new_node).body.lf.keys[0], sizeof(key_int64_t) * new_node_numKeys);
                mram_write(&moved_keys[0], &Deref(node).body.lf.keys[idx_to_insert], sizeof(key_int64_t) * (node_numKeys - idx_to_insert));
            }
            {
                __dma_aligned value_ptr_t moved_values[MAX_NR_PAIRS + 1];
                mram_read(&Deref(node).body.lf.values[idx_to_insert], &moved_values[1], sizeof(value_ptr_t) * (MAX_NR_PAIRS - idx_to_insert));
                moved_values[0] = value;
                mram_write(&moved_values[node_numKeys - 1 - idx_to_insert], &Deref(new_node).body.lf.values[0], sizeof(value_ptr_t) * new_node_numKeys);
                mram_write(&moved_values[0], &Deref(node).body.lf.values[idx_to_insert], sizeof(value_ptr_t) * (node_numKeys - idx_to_insert));
            }
        }

        for (;; depth--) {
            if (depth == 0) {
                const NodePtr new_root = Allocate_node();
                Deref(new_root).header.isLeaf = false;
                Deref(new_root).header.numKeys = 1;
                Deref(new_root).body.inl.keys[0] = key;
                Deref(new_root).body.inl.children[0].ptr = node;
                Deref(new_root).body.inl.children[1].ptr = new_node;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                Deref(new_root).body.inl.children[0].numKeys = node_numKeys;
                Deref(new_root).body.inl.children[0].isLeaf = is_new_node_leaf;
                Deref(new_root).body.inl.children[1].numKeys = new_node_numKeys;
                Deref(new_root).body.inl.children[1].isLeaf = is_new_node_leaf;
#endif

                root = new_root;
                Deref(node).header.parent = Deref(new_node).header.parent = new_root;

                return;
            }

            const NodePtr parent = Deref(node).header.parent;
            const unsigned idx_to_insert = idx_child_cache[depth - 1];

            if (Deref(parent).header.numKeys != MAX_NR_CHILDREN - 1) {
                if (idx_to_insert == Deref(parent).header.numKeys) {
                    mram_write(&key, &Deref(parent).body.inl.keys[idx_to_insert], sizeof(key_int64_t));

#ifdef CACHE_CHILD_HEADER_IN_LINK
                    __dma_aligned ChildInfo buf[16 / sizeof(ChildInfo)];
                    const unsigned idx_to_change = idx_to_insert,
                                   idx_to_write = idx_to_change / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA,
                                   idx_to_change_in_buf = idx_to_change % ALIGNOF_CHILDINFO_DMA,
                                   bytes_to_write = sizeof(ChildInfo) * (idx_to_change_in_buf == ALIGNOF_CHILDINFO_DMA - 1 ? ALIGNOF_CHILDINFO_DMA * 2 : ALIGNOF_CHILDINFO_DMA);
                    if (idx_to_change_in_buf != 0) {
                        mram_read(&Deref(parent).body.inl.children[idx_to_write], &buf[0], sizeof(ChildInfo) * ALIGNOF_CHILDINFO_DMA);
                    }
                    buf[idx_to_change_in_buf].ptr = node;
                    buf[idx_to_change_in_buf].numKeys = node_numKeys;
                    buf[idx_to_change_in_buf].isLeaf = is_new_node_leaf;
                    buf[idx_to_change_in_buf + 1].ptr = new_node;
                    buf[idx_to_change_in_buf + 1].numKeys = new_node_numKeys;
                    buf[idx_to_change_in_buf + 1].isLeaf = is_new_node_leaf;
                    mram_write(&buf[0], &Deref(parent).body.inl.children[idx_to_change], bytes_to_write);
#else  /* CACHE_CHILD_HEADER_IN_LINK */
                    __dma_aligned ChildInfo buf[8 / sizeof(ChildInfo)];
                    const unsigned idx_new_child = idx_to_insert + 1,
                                   nr_children_to_write = 8 / sizeof(ChildInfo),
                                   idx_to_write = idx_new_child / nr_children_to_write * nr_children_to_write,
                                   idx_new_child_in_buf = idx_new_child % nr_children_to_write;
                    if (idx_new_child_in_buf == 0) {
                        buf[0].ptr = new_node;
                    } else {
                        mram_read(&Deref(parent).body.inl.children[idx_to_write], &buf[0], sizeof(buf));
                        buf[idx_new_child_in_buf].ptr = new_node;
                    }
                    mram_write(&buf[0], &Deref(parent).body.inl.children[idx_to_write], sizeof(buf));
#endif /* CACHE_CHILD_HEADER_IN_LINK */

                } else {
                    {
                        __dma_aligned key_int64_t moved_keys[MAX_NR_CHILDREN - 1];
                        mram_read(&Deref(parent).body.inl.keys[idx_to_insert], &moved_keys[1], sizeof(key_int64_t) * (Deref(parent).header.numKeys - idx_to_insert));
                        moved_keys[0] = key;
                        mram_write(&moved_keys[0], &Deref(parent).body.inl.keys[idx_to_insert], sizeof(key_int64_t) * (Deref(parent).header.numKeys - idx_to_insert + 1));
                    }
                    {
                        __dma_aligned ChildInfo moved_children[(MAX_NR_CHILDREN + ALIGNOF_CHILDINFO_DMA - 1) / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA];
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        const unsigned idx_to_change = idx_to_insert,
                                       idx_to_readwrite = idx_to_change / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA,
                                       idx_to_change_in_buf = idx_to_change % ALIGNOF_CHILDINFO_DMA,
                                       elems_to_read = Deref(parent).header.numKeys + 1 - idx_to_change,
                                       bytes_to_read = sizeof(ChildInfo) * ((elems_to_read + ALIGNOF_CHILDINFO_DMA - 1) / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA),
                                       elems_to_write = Deref(parent).header.numKeys + 2 - idx_to_readwrite,
                                       bytes_to_write = sizeof(ChildInfo) * ((elems_to_write + ALIGNOF_CHILDINFO_DMA - 1) / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA);
                        mram_read(&Deref(parent).body.inl.children[idx_to_readwrite], &moved_children[0], bytes_to_read);
                        memmove(&moved_children[idx_to_change_in_buf + 2], &moved_children[idx_to_change_in_buf + 1], sizeof(ChildInfo) * (elems_to_read - idx_to_change_in_buf - 1));
                        moved_children[idx_to_change_in_buf].numKeys = node_numKeys;
                        moved_children[idx_to_change_in_buf + 1].ptr = new_node;
                        moved_children[idx_to_change_in_buf + 1].numKeys = new_node_numKeys;
                        moved_children[idx_to_change_in_buf + 1].isLeaf = is_new_node_leaf;
                        mram_write(&moved_children[0], &Deref(parent).body.inl.children[idx_to_readwrite], bytes_to_write);
#else  /* CACHE_CHILD_HEADER_IN_LINK */
                        const unsigned idx_to_change = idx_to_insert + 1,
                                       idx_to_readwrite = idx_to_change / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA,
                                       idx_to_change_in_buf = idx_to_change % ALIGNOF_CHILDINFO_DMA,
                                       elems_to_read = Deref(parent).header.numKeys + 1 - idx_to_readwrite,
                                       bytes_to_read = sizeof(ChildInfo) * ((elems_to_read + ALIGNOF_CHILDINFO_DMA - 1) / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA),
                                       elems_to_write = Deref(parent).header.numKeys + 2 - idx_to_readwrite,
                                       bytes_to_write = sizeof(ChildInfo) * ((elems_to_write + ALIGNOF_CHILDINFO_DMA - 1) / ALIGNOF_CHILDINFO_DMA * ALIGNOF_CHILDINFO_DMA);
                        mram_read(&Deref(parent).body.inl.children[idx_to_readwrite], &moved_children[0], bytes_to_read);
                        memmove(&moved_children[idx_to_change_in_buf + 1], &moved_children[idx_to_change_in_buf], sizeof(ChildInfo) * (elems_to_read - idx_to_change_in_buf));
                        moved_children[idx_to_change_in_buf].ptr = new_node;
                        mram_write(&moved_children[0], &Deref(parent).body.inl.children[idx_to_readwrite], bytes_to_write);
#endif /* CACHE_CHILD_HEADER_IN_LINK */
                    }
                }
                Deref(parent).header.numKeys += 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                if (depth != 1) {
                    Deref(Deref(parent).header.parent).body.inl.children[idx_child_cache[depth - 2]].numKeys = Deref(parent).header.numKeys;
                }
#endif

                Deref(new_node).header.parent = parent;
                return;

            } else {
                const uint8_t parent_numKeys = MAX_NR_CHILDREN / 2, new_parent_numKeys = (MAX_NR_CHILDREN - 1) / 2;

                const NodePtr new_parent = Allocate_node();
                Deref(new_parent).header.isLeaf = false;
                Deref(new_parent).header.numKeys = new_parent_numKeys;
                // Deref(new_parent).header.parent = ...  // it depends on whether the grandparent is to be splitted

                Deref(parent).header.numKeys = parent_numKeys;

                // split: distribute keys and children as follows
                //     input:
                //         keys:      parent.key[0..(idx_to_insert - 1)] ++ [key] ++ parent.key[idx_to_insert..(MAX_CHILD - 1)]
                //         children:  parent.children[0..idx_to_insert] ++ [new_node] ++ parent.children[(idx_to_insert + 1)..MAX_CHILD]
                //     output:
                //         keys:      parent.key[0..(parent_numKeys - 1)] ++ [key] ++ new_parent.key[0..(new_parent_numKeys - 1)]
                //         children:  parent.children[0..parent_numKeys] ++ new_parent.children[0..new_parent_numKeys]
                if (idx_to_insert >= parent_numKeys) {
                    uint8_t idx_dest = new_parent_numKeys;
                    for (unsigned idx_src = MAX_NR_CHILDREN - 1; idx_src > idx_to_insert; idx_src--, idx_dest--) {
                        Deref(new_parent).body.inl.children[idx_dest] = Deref(parent).body.inl.children[idx_src];
                        Deref(Deref(new_parent).body.inl.children[idx_dest].ptr).header.parent = new_parent;
                    }
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    Deref(new_parent).body.inl.children[idx_dest].numKeys = new_node_numKeys;
                    Deref(new_parent).body.inl.children[idx_dest].isLeaf = is_new_node_leaf;
#endif
                    Deref(new_parent).body.inl.children[idx_dest--].ptr = new_node;
                    Deref(new_node).header.parent = new_parent;
                    if (idx_to_insert != parent_numKeys) {
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        Deref(new_parent).body.inl.children[idx_dest].numKeys = node_numKeys;
                        Deref(new_parent).body.inl.children[idx_dest].isLeaf = is_new_node_leaf;
#endif
                        Deref(new_parent).body.inl.children[idx_dest--].ptr = node;
                        Deref(node).header.parent = new_parent;
                    }
                    for (unsigned idx_src = idx_to_insert - 1; idx_src > parent_numKeys; idx_src--, idx_dest--) {
                        Deref(new_parent).body.inl.children[idx_dest] = Deref(parent).body.inl.children[idx_src];
                        Deref(Deref(new_parent).body.inl.children[idx_dest].ptr).header.parent = new_parent;
                    }
                } else {
                    for (uint8_t idx_src = parent_numKeys, idx_dest = 0; idx_dest <= new_parent_numKeys; idx_src++, idx_dest++) {
                        Deref(new_parent).body.inl.children[idx_dest] = Deref(parent).body.inl.children[idx_src];
                        Deref(Deref(new_parent).body.inl.children[idx_dest].ptr).header.parent = new_parent;
                    }
                    for (unsigned idx_src = parent_numKeys - 1; idx_src > idx_to_insert; idx_src--) {
                        Deref(parent).body.inl.children[idx_src + 1] = Deref(parent).body.inl.children[idx_src];
                    }
                    Deref(parent).body.inl.children[idx_to_insert + 1].ptr = new_node;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    Deref(parent).body.inl.children[idx_to_insert + 1].numKeys = new_node_numKeys;
                    Deref(parent).body.inl.children[idx_to_insert + 1].isLeaf = is_new_node_leaf;
#endif
                    Deref(new_node).header.parent = parent;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    Deref(parent).body.inl.children[idx_to_insert].numKeys = node_numKeys;
                    Deref(parent).body.inl.children[idx_to_insert].isLeaf = is_new_node_leaf;
#endif
                }

                if (idx_to_insert > parent_numKeys) {
                    uint8_t idx_dest = new_parent_numKeys - 1;
                    for (uint8_t idx_src = MAX_NR_CHILDREN - 1; idx_src > idx_to_insert; idx_src--, idx_dest--) {
                        Deref(new_parent).body.inl.keys[idx_dest] = Deref(parent).body.inl.keys[idx_src - 1];
                    }
                    Deref(new_parent).body.inl.keys[idx_dest--] = key;
                    for (unsigned idx_src = idx_to_insert - 1; idx_src > parent_numKeys; idx_src--, idx_dest--) {
                        Deref(new_parent).body.inl.keys[idx_dest] = Deref(parent).body.inl.keys[idx_src];
                    }
                    key = Deref(parent).body.inl.keys[parent_numKeys];
                } else {
                    for (uint8_t idx_src = parent_numKeys, idx_dest = 0; idx_src < MAX_NR_CHILDREN - 1; idx_src++, idx_dest++) {
                        Deref(new_parent).body.inl.keys[idx_dest] = Deref(parent).body.inl.keys[idx_src];
                    }
                    if (idx_to_insert != parent_numKeys) {
                        const key_int64_t to_grandparent = Deref(parent).body.inl.keys[parent_numKeys - 1];
                        for (unsigned idx_dest = parent_numKeys - 1; idx_dest > idx_to_insert; idx_dest--) {
                            Deref(parent).body.inl.keys[idx_dest] = Deref(parent).body.inl.keys[idx_dest - 1];
                        }
                        Deref(parent).body.inl.keys[idx_to_insert] = key;
                        key = to_grandparent;
                    }
                }

                node = parent;
                node_numKeys = parent_numKeys;
                new_node = new_parent;
                new_node_numKeys = new_parent_numKeys;
                is_new_node_leaf = false;
            }
        }
    }
}

#ifdef EXPLICIT_DMA_IN_GET
#if defined(DMA_WHOLE_NODE)
value_ptr_t TreeGet(key_int64_t key)
{
    __dma_aligned Node cache;

    NodePtr node = root;
    mram_read(&Deref(node), &cache, sizeof(Node));
    while (!cache.header.isLeaf) {
        node = cache.body.inl.children[findUpperBoundWRAM(cache.body.inl.keys, cache.header.numKeys, key)].ptr;
        mram_read(&Deref(node), &cache, sizeof(Node));
    }

    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(cache.body.lf.keys, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && cache.body.lf.keys[idx_pair_plus_1 - 1] == key) {
        return cache.body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#elif defined(DMA_WHOLE_KEY_ARRAY)
value_ptr_t TreeGet(key_int64_t key)
{
    __dma_aligned NodeHeaderAndKeys cache;

    NodePtr node = root;
    mram_read(&Deref(node), &cache, sizeof(NodeHeaderAndKeys));
    while (!cache.header.isLeaf) {
        node = Deref(node).body.inl.children[findUpperBoundWRAM(cache.body.inl.keys, cache.header.numKeys, key)].ptr;
        mram_read(&Deref(node), &cache, sizeof(NodeHeaderAndKeys));
    }

    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(cache.body.lf.keys, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && cache.body.lf.keys[idx_pair_plus_1 - 1] == key) {
        return Deref(node).body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#elif defined(DMA_VALID_KEYS)
#ifdef CACHE_CHILD_HEADER_IN_LINK
value_ptr_t TreeGet(key_int64_t key)
{
    union __dma_aligned {  // same structure
        NodeHeader header;
        ChildInfo child;
        char size_adjuster[8];
    } cache;

    NodePtr node = root;
    mram_read(&Deref(node).header, &cache, sizeof(cache));
    while (!cache.header.isLeaf) {
        __dma_aligned key_int64_t keys_cache[MAX_NR_CHILDREN - 1];
        mram_read(&Deref(node).body.inl.keys[0], &keys_cache[0], sizeof(key_int64_t) * cache.header.numKeys);

        mram_read(&Deref(node).body.inl.children[findUpperBoundWRAM(keys_cache, cache.header.numKeys, key)], &cache, sizeof(cache));
        node = cache.child.ptr;
    }

    __dma_aligned key_int64_t keys_cache[MAX_NR_PAIRS];
    mram_read(&Deref(node).body.lf.keys[0], &keys_cache[0], sizeof(key_int64_t) * cache.header.numKeys);
    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(keys_cache, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && keys_cache[idx_pair_plus_1 - 1] == key) {
        return Deref(node).body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#else  /* CACHE_CHILD_HEADER_IN_LINK */
value_ptr_t TreeGet(key_int64_t key)
{
    union __dma_aligned {
        NodeHeader header;
        char size_adjuster[8];
    } cache;

    NodePtr node = root;
    mram_read(&Deref(node).header, &cache, sizeof(cache));
    while (!cache.header.isLeaf) {
        __dma_aligned key_int64_t keys_cache[MAX_NR_CHILDREN - 1];
        mram_read(&Deref(node).body.inl.keys[0], &keys_cache[0], sizeof(key_int64_t) * cache.header.numKeys);

        node = Deref(node).body.inl.children[findUpperBoundWRAM(keys_cache, cache.header.numKeys, key)].ptr;
        mram_read(&Deref(node).header, &cache, sizeof(cache));
    }

    __dma_aligned key_int64_t keys_cache[MAX_NR_PAIRS];
    mram_read(&Deref(node).body.lf.keys[0], &keys_cache[0], sizeof(key_int64_t) * cache.header.numKeys);
    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(keys_cache, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && keys_cache[idx_pair_plus_1 - 1] == key) {
        return Deref(node).body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#endif /* CACHE_CHILD_HEADER_IN_LINK */
#endif /* DMA_VALID_KEYS */
#else  /* EXPLICIT_DMA_IN_GET */
/**
 * @brief 
 * get a value related to the key.
 * @param key key
 * @return value related to the key
 */
value_ptr_t TreeGet(key_int64_t key)
{
    NodePtr node = root;
    while (!Deref(node).header.isLeaf) {
        node = Deref(node).body.inl.children[findUpperBound(Deref(node).body.inl.keys, Deref(node).header.numKeys, key)].ptr;
    }

    const unsigned idx_pair_plus_1 = findUpperBound(Deref(node).body.lf.keys, Deref(node).header.numKeys, key);
    if (idx_pair_plus_1 != 0 && Deref(node).body.lf.keys[idx_pair_plus_1 - 1] == key) {
        return Deref(node).body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#endif /* EXPLICIT_DMA_IN_GET */

/**
 * @brief
 * get the pair with the smallest key greater than the given key.
 * @param key key
 * @return value related to the key
 */
// KVPair TreeSucc(key_int64_t key)
// {
// }


void TreeSerialize(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest)
{
    NodePtr leaf = root;
    while (!Deref(leaf).header.isLeaf) {
        leaf = Deref(leaf).body.inl.children[0].ptr;
    }

    do {
        for (uint8_t i = 0; i < Deref(leaf).header.numKeys; i++) {
            *(keys_dest++) = Deref(leaf).body.lf.keys[i];
            *(values_dest++) = Deref(leaf).body.lf.values[i];
        }
        leaf = Deref(leaf).body.lf.right;
    } while (leaf != NODE_NULLPTR);

    init_Tree();
}

uint32_t TreeExtractFirstPairs(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest, key_int64_t delimiter)
{
    NodePtr leaf = root;
    while (!Deref(leaf).header.isLeaf) {
        leaf = Deref(leaf).body.inl.children[0].ptr;
    }

    uint32_t nr_serialized = 0;

    //--- copy&deletion: While copying keys and values in order from the left end of the tree, delete empty nodes ---//
    for (;;) {
        if (Deref(leaf).body.lf.keys[Deref(leaf).header.numKeys - 1] < delimiter) {  // copy all the pairs
            nr_serialized += Deref(leaf).header.numKeys;
            for (uint8_t i = 0; i < Deref(leaf).header.numKeys; i++) {
                *(keys_dest++) = Deref(leaf).body.lf.keys[i];
                *(values_dest++) = Deref(leaf).body.lf.values[i];
            }
            NodePtr deleted = leaf;
            leaf = Deref(leaf).body.lf.right;
            for (;;) {
                if (deleted == root) {  // the whole tree get empty
                    Deref(deleted).header.isLeaf = true;
                    Deref(deleted).header.numKeys = 0;
                    Deref(deleted).body.lf.right = Deref(deleted).body.lf.left = NODE_NULLPTR;
                    num_kvpairs -= nr_serialized;
                    return nr_serialized;
                } else {
                    const NodePtr parent = Deref(deleted).header.parent;
                    const bool is_last_child = (Deref(parent).body.inl.children[Deref(parent).header.numKeys].ptr == deleted);
                    Free_node(deleted);
                    if (is_last_child) {
                        deleted = parent;
                    } else {
                        break;
                    }
                }
            }
        } else {  // copy some of the pairs
            uint8_t n_move = 0;
            for (; Deref(leaf).body.lf.keys[n_move] < delimiter; n_move++) {  // TODO: binary search & bulk copy
                *(keys_dest++) = Deref(leaf).body.lf.keys[n_move];
                *(values_dest++) = Deref(leaf).body.lf.values[n_move];
            }
            if (n_move != 0) {
                nr_serialized += n_move;
                const uint8_t orig_numKeys = Deref(leaf).header.numKeys;
                uint8_t i = 0;
                for (; n_move < orig_numKeys; i++, n_move++) {
                    Deref(leaf).body.lf.keys[i] = Deref(leaf).body.lf.keys[n_move];
                    Deref(leaf).body.lf.values[i] = Deref(leaf).body.lf.values[n_move];
                }
                Deref(leaf).header.numKeys = i;
            }
            Deref(leaf).body.lf.left = NODE_NULLPTR;
            break;
        }
    }

    //--- hole-filling: Ensure no internal node points to the deleted node as a child ---//
    for (NodePtr node = leaf; node != root;) {
        const NodePtr parent = Deref(node).header.parent;
        if (Deref(parent).body.inl.children[0].ptr != node) {
            unsigned live_child_idx = findUpperBound(Deref(parent).body.inl.keys, Deref(parent).header.numKeys, delimiter);
            const uint8_t orig_numKeys = Deref(parent).header.numKeys;
            uint8_t i = 0;
            for (; live_child_idx < orig_numKeys; i++, live_child_idx++) {
                Deref(parent).body.inl.keys[i] = Deref(parent).body.inl.keys[live_child_idx];
                Deref(parent).body.inl.children[i] = Deref(parent).body.inl.children[live_child_idx];
            }
            Deref(parent).body.inl.children[i] = Deref(parent).body.inl.children[live_child_idx];
            Deref(parent).header.numKeys = i;
        }
#ifdef CACHE_CHILD_HEADER_IN_LINK
        Deref(parent).body.inl.children[0].numKeys = Deref(node).header.numKeys;
#endif
        node = parent;
    }

    //--- regularization: Ensure no node has too few contents ---//
    while (Deref(root).header.numKeys == 0) {  // while the root has too few contents
        const NodePtr child = Deref(root).body.inl.children[0].ptr;
        Free_node(root);
        root = child;
    }
    if (root != leaf) {
        for (NodePtr parent = root;;) {
            const NodePtr node = Deref(parent).body.inl.children[0].ptr;
            if (node == leaf) {
                if (Deref(node).header.numKeys < MIN_NR_PAIRS) {
                    const NodePtr sibling = Deref(parent).body.inl.children[1].ptr;
                    const unsigned sum_num_pairs = Deref(node).header.numKeys + Deref(sibling).header.numKeys;
                    if (sum_num_pairs >= MIN_NR_PAIRS * 2) {  // move kv-pairs from the sibling
                        const uint8_t n_move = MIN_NR_PAIRS - Deref(node).header.numKeys;
                        for (uint8_t src = 0, dest = Deref(node).header.numKeys; src < n_move; src++, dest++) {
                            Deref(node).body.lf.keys[dest] = Deref(sibling).body.lf.keys[src];
                            Deref(node).body.lf.values[dest] = Deref(sibling).body.lf.values[src];
                        }
                        Deref(node).header.numKeys = MIN_NR_PAIRS;
                        Deref(parent).body.inl.keys[0] = Deref(sibling).body.lf.keys[n_move];
                        for (uint8_t src = n_move, dest = 0; src < Deref(sibling).header.numKeys; src++, dest++) {
                            Deref(sibling).body.lf.keys[dest] = Deref(sibling).body.lf.keys[src];
                            Deref(sibling).body.lf.values[dest] = Deref(sibling).body.lf.values[src];
                        }
                        Deref(sibling).header.numKeys = (uint8_t)(sum_num_pairs - MIN_NR_PAIRS);
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        Deref(parent).body.inl.children[0].ptr = node;
                        Deref(parent).body.inl.children[0].numKeys = MIN_NR_PAIRS;
                        Deref(parent).body.inl.children[0].isLeaf = true;
                        Deref(parent).body.inl.children[1].ptr = sibling;
                        Deref(parent).body.inl.children[1].numKeys = (uint8_t)(sum_num_pairs - MIN_NR_PAIRS);
                        Deref(parent).body.inl.children[1].isLeaf = true;
#endif

                    } else {  // merge node and the sibling
                        for (unsigned src = Deref(sibling).header.numKeys - 1, dest = sum_num_pairs - 1; dest >= Deref(node).header.numKeys; src--, dest--) {
                            Deref(sibling).body.lf.keys[dest] = Deref(sibling).body.lf.keys[src];
                            Deref(sibling).body.lf.values[dest] = Deref(sibling).body.lf.values[src];
                        }
                        for (uint8_t i = 0; i < Deref(node).header.numKeys; i++) {
                            Deref(sibling).body.lf.keys[i] = Deref(node).body.lf.keys[i];
                            Deref(sibling).body.lf.values[i] = Deref(node).body.lf.values[i];
                        }
                        Deref(sibling).header.numKeys = (uint8_t)sum_num_pairs;
                        Deref(sibling).body.lf.left = NODE_NULLPTR;
                        Free_node(node);

                        if (Deref(parent).header.numKeys <= 1) {
                            Free_node(parent);
                            root = sibling;
                        } else {
                            Deref(parent).body.inl.children[0].ptr = sibling;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            Deref(parent).body.inl.children[0].numKeys = (uint8_t)sum_num_pairs;
                            Deref(parent).body.inl.children[0].isLeaf = true;
#endif
                            uint8_t i = 1;
                            for (; i < Deref(parent).header.numKeys; i++) {
                                Deref(parent).body.inl.keys[i - 1] = Deref(parent).body.inl.keys[i];
                                Deref(parent).body.inl.children[i] = Deref(parent).body.inl.children[i + 1];
                            }
                            Deref(parent).header.numKeys = i - 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            Deref(Deref(parent).header.parent).body.inl.children[0].ptr = parent;
                            Deref(Deref(parent).header.parent).body.inl.children[0].numKeys = i - 1;
                            Deref(Deref(parent).header.parent).body.inl.children[0].isLeaf = false;
#endif
                        }
                    }
                }
                break;
            } else {
                if (Deref(node).header.numKeys < MIN_NR_KEYS + 1) {
                    const NodePtr sibling = Deref(parent).body.inl.children[1].ptr;
                    const unsigned sum_num_keys = Deref(node).header.numKeys + Deref(sibling).header.numKeys;
                    if (sum_num_keys >= MIN_NR_KEYS * 2 + 1) {  // move kv-pairs from the sibling
                        const uint8_t n_move = MIN_NR_KEYS + 1 - Deref(node).header.numKeys;
                        for (uint8_t i = 0; i < n_move; i++) {
                            Deref(Deref(sibling).body.inl.children[i].ptr).header.parent = node;
                        }
                        Deref(node).body.inl.keys[Deref(node).header.numKeys] = Deref(parent).body.inl.keys[0];
                        for (uint8_t src = 0, dest = Deref(node).header.numKeys + 1; src < n_move - 1; src++, dest++) {
                            Deref(node).body.inl.keys[dest] = Deref(sibling).body.inl.keys[src];
                        }
                        Deref(parent).body.inl.keys[0] = Deref(sibling).body.inl.keys[n_move - 1];
                        for (uint8_t src = n_move, dest = 0; src < Deref(sibling).header.numKeys; src++, dest++) {
                            Deref(sibling).body.inl.keys[dest] = Deref(sibling).body.inl.keys[src];
                        }

                        for (uint8_t src = 0, dest = Deref(node).header.numKeys + 1; src < n_move; src++, dest++) {
                            Deref(node).body.inl.children[dest] = Deref(sibling).body.inl.children[src];
                        }
                        for (uint8_t src = n_move, dest = 0; src < Deref(sibling).header.numKeys + 1; src++, dest++) {
                            Deref(sibling).body.inl.children[dest] = Deref(sibling).body.inl.children[src];
                        }

                        Deref(node).header.numKeys = MIN_NR_KEYS + 1;
                        Deref(sibling).header.numKeys = (uint8_t)(sum_num_keys - (MIN_NR_KEYS + 1));
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        Deref(parent).body.inl.children[0].ptr = node;
                        Deref(parent).body.inl.children[0].numKeys = MIN_NR_KEYS + 1;
                        Deref(parent).body.inl.children[0].isLeaf = false;
                        Deref(parent).body.inl.children[1].ptr = sibling;
                        Deref(parent).body.inl.children[1].numKeys = (uint8_t)(sum_num_keys - (MIN_NR_KEYS + 1));
                        Deref(parent).body.inl.children[1].isLeaf = false;
#endif

                    } else {  // merge node and the sibling
                        _Static_assert(MAX_NR_CHILDREN % 2 == 0, "Eager merging requires even number for MAX_NR_CHILDREN");
                        for (uint8_t i = 0; i < Deref(node).header.numKeys + 1; i++) {
                            Deref(Deref(node).body.inl.children[i].ptr).header.parent = sibling;
                        }
                        for (unsigned src = Deref(sibling).header.numKeys - 1, dest = sum_num_keys; dest >= Deref(node).header.numKeys + 1; src--, dest--) {
                            Deref(sibling).body.inl.keys[dest] = Deref(sibling).body.inl.keys[src];
                        }
                        Deref(sibling).body.inl.keys[Deref(node).header.numKeys] = Deref(parent).body.inl.keys[0];
                        for (uint8_t i = 0; i < Deref(node).header.numKeys; i++) {
                            Deref(sibling).body.inl.keys[i] = Deref(node).body.inl.keys[i];
                        }

                        for (unsigned src = Deref(sibling).header.numKeys, dest = sum_num_keys + 1; dest >= Deref(node).header.numKeys + 1; src--, dest--) {
                            Deref(sibling).body.inl.children[dest] = Deref(sibling).body.inl.children[src];
                        }
                        for (uint8_t i = 0; i < Deref(node).header.numKeys + 1; i++) {
                            Deref(sibling).body.inl.children[i] = Deref(node).body.inl.children[i];
                        }
                        Deref(sibling).header.numKeys = (uint8_t)(sum_num_keys + 1);
                        Free_node(node);

                        if (Deref(parent).header.numKeys <= 1) {
                            Free_node(parent);
                            root = sibling;
                        } else {
                            Deref(parent).body.inl.children[0].ptr = sibling;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            Deref(parent).body.inl.children[0].numKeys = (uint8_t)(sum_num_keys + 1);
                            Deref(parent).body.inl.children[0].isLeaf = false;
#endif
                            uint8_t i = 1;
                            for (; i < Deref(parent).header.numKeys; i++) {
                                Deref(parent).body.inl.keys[i - 1] = Deref(parent).body.inl.keys[i];
                                Deref(parent).body.inl.children[i] = Deref(parent).body.inl.children[i + 1];
                            }
                            Deref(parent).header.numKeys = i - 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            Deref(Deref(parent).header.parent).body.inl.children[0].ptr = parent;
                            Deref(Deref(parent).header.parent).body.inl.children[0].numKeys = i - 1;
                            Deref(Deref(parent).header.parent).body.inl.children[0].isLeaf = false;
#endif
                        }
                        parent = sibling;
                        continue;
                    }
                }
                parent = node;
            }
        }
    }
    num_kvpairs -= nr_serialized;
    return nr_serialized;
}

key_int64_t TreeNthKeyFromLeft(uint32_t nth)
{
    NodePtr leaf = root;
    while (!Deref(leaf).header.isLeaf) {
        leaf = Deref(leaf).body.inl.children[0].ptr;
    }

    for (;; leaf = Deref(leaf).body.lf.right) {
        if (nth < Deref(leaf).header.numKeys) {
            return Deref(leaf).body.lf.keys[nth];
        } else {
            nth -= Deref(leaf).header.numKeys;
        }
    }
    return 0;
}
key_int64_t TreeNthKeyFromRight(uint32_t nth)
{
    NodePtr leaf = root;
    while (!Deref(leaf).header.isLeaf) {
        leaf = Deref(leaf).body.inl.children[Deref(leaf).header.numKeys].ptr;
    }

    for (;; leaf = Deref(leaf).body.lf.left) {
        if (nth < Deref(leaf).header.numKeys) {
            return Deref(leaf).body.lf.keys[Deref(leaf).header.numKeys - 1 - nth];
        } else {
            nth -= Deref(leaf).header.numKeys;
        }
    }
}

void TreeInsertSortedPairsToLeft(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs)
{
    for (uint32_t i = 0; i < nr_pairs; i++) {
        TreeInsert(keys_src[i], values_src[i]);
    }
}
void TreeInsertSortedPairsToRight(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs)
{
    for (uint32_t i = 0; i < nr_pairs; i++) {
        TreeInsert(keys_src[i], values_src[i]);
    }
}


#ifdef DEBUG_ON
#define QUEUE_SIZE (MAX_NUM_NODES_IN_DPU)
#include "node_queue.h"

void showNode(NodePtr cur, int nodeNo)
{  // show single node
    printf("[Node No. %d]\n", nodeNo);
    if (Deref(cur).header.isLeaf == true) {
        cur == root ? printf("this is a Root LeafNode (addr %p)\n", cur)
                    : printf("this is a LeafNode (addr %p)\n", cur);
        printf("0. parent: %x\n", Deref(cur).header.parent);
        printf("1. number of keys: %d\n", Deref(cur).header.numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < Deref(cur).header.numKeys; i++) {
            printf("%lx ", Deref(cur).body.lf.keys[i]);
        }
        printf("]\n");
        printf("3. value pointers:[ ");
        for (int i = 0; i < Deref(cur).header.numKeys; i++) {
            printf("%lx ", Deref(cur).body.lf.values[i]);
        }
        printf("]\n");
        printf("4. leaf connections, left: %x right: %x\n", Deref(cur).body.lf.left,
            Deref(cur).body.lf.right);
    } else {
        cur == root ? printf("this is a Root InternalNode (addr %x)\n", cur)
                    : printf("this is an InternalNode (addr %x)\n", cur);
        printf("0. parent: %x\n", Deref(cur).header.parent);
        printf("1. number of keys: %d\n", Deref(cur).header.numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < Deref(cur).header.numKeys; i++) {
            printf("%lx ", Deref(cur).body.inl.keys[i]);
        }
        printf("]\n");
        printf("3. children:[ ");
        for (int i = 0; i <= Deref(cur).header.numKeys; i++) {
            printf("%x ", Deref(cur).body.inl.children[i].ptr);
        }
        printf("]\n");
    }
    printf("\n");
}

void TreePrintLeaves()
{
    NodePtr leaf = root;
    while (!Deref(leaf).header.isLeaf) {
        leaf = Deref(leaf).body.inl.children[0].ptr;
    }

    int cnt = 0;
    while (leaf != NODE_NULLPTR) {
        showNode(leaf, cnt);
        leaf = Deref(leaf).body.lf.right;
        cnt++;
    }
    printf("\n");
}

void TreePrintKeys()
{
    NodePtr leaf = root;
    while (!Deref(leaf).header.isLeaf) {
        leaf = Deref(leaf).body.inl.children[0].ptr;
    }

    while (leaf != NODE_NULLPTR) {
        for (uint8_t i = 0; i < Deref(leaf).header.numKeys; i++) {
            printf("%lx ", Deref(leaf).body.lf.keys[i]);
        }
        leaf = Deref(leaf).body.lf.right;
    }
    printf("\n");
}
bool TreeCheckStructure()
{
    bool success = true;
    initQueue();
    enqueue(root);
    for (int nodeNo = 0; !isQueueEmpty(); nodeNo++) {
        NodePtr cur = dequeue();
        if (Deref(cur).header.isLeaf) {
            const NodePtr left = Deref(cur).body.lf.left, right = Deref(cur).body.lf.right;
            if (left != NODE_NULLPTR) {
                if (!Deref(left).header.isLeaf) {
                    success = false;
                    printf("Node[%d]: %p is a leaf but %p->left (%p) is not.\n", nodeNo, cur, cur, left);
                }
                if (Deref(left).body.lf.right != cur) {
                    success = false;
                    printf("Node[%d]: %p->left->right == %p != %p\n", nodeNo, cur, Deref(left).body.lf.right, cur);
                }
            }
            if (right != NODE_NULLPTR) {
                if (!Deref(right).header.isLeaf) {
                    success = false;
                    printf("Node[%d]: %p is a leaf but %p->right (%p) is not.\n", nodeNo, cur, cur, right);
                }
                if (Deref(right).body.lf.left != cur) {
                    success = false;
                    printf("Node[%d]: %p->right->left == %p != %p\n", nodeNo, cur, Deref(right).body.lf.left, cur);
                }
            }
        } else {
            for (uint8_t i = 0; i <= Deref(cur).header.numKeys; i++) {
                const ChildInfo child = Deref(cur).body.inl.children[i];
                if (child.ptr == NODE_NULLPTR) {
                    success = false;
                    printf("Node[%d]: %p->children[%u] == null\n", nodeNo, cur, i);
                } else {
                    if (Deref(child.ptr).header.parent != cur) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].ptr->header.parent == %p != %p\n", nodeNo, cur, i, Deref(child.ptr).header.parent, cur);
                    }
                    if (i != 0 && Deref(cur).body.inl.keys[i - 1] > (Deref(child.ptr).header.isLeaf ? Deref(child.ptr).body.lf.keys : Deref(child.ptr).body.inl.keys)[0]) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].ptr->header.numKeys < %p->key[%u]\n", nodeNo, cur, i, cur, i - 1);
                    }
                    if (i != Deref(cur).header.numKeys && (Deref(child.ptr).header.isLeaf ? Deref(child.ptr).body.lf.keys : Deref(child.ptr).body.inl.keys)[Deref(child.ptr).header.numKeys - 1] >= Deref(cur).body.inl.keys[i]) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].ptr->header.numKeys >= %p->key[%u]\n", nodeNo, cur, i, cur, i);
                    }
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    if (child.numKeys != Deref(child.ptr).header.numKeys) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].numKeys == %u != %u\n", nodeNo, cur, i, child.numKeys, Deref(child.ptr).header.numKeys);
                    }
                    if (child.isLeaf != Deref(child.ptr).header.isLeaf) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].isLeaf == %u != %u\n", nodeNo, cur, i, child.isLeaf, Deref(child.ptr).header.isLeaf);
                    }
#endif /* CACHE_CHILD_HEADER_IN_LINK */
                }
            }
            for (int i = 0; i <= Deref(cur).header.numKeys; i++) {
                enqueue(Deref(cur).body.inl.children[i].ptr);
            }
        }
    }
    return success;
}

void TreePrintRoot()
{
    printf("rootNode\n");
    showNode(root, 0);
}

void TreePrintAll()
{  // show all node (BFS)
    int nodeNo = 0;
    initQueue();
    enqueue(root);
    while (!isQueueEmpty()) {
        NodePtr cur = dequeue();
        showNode(cur, nodeNo);
        nodeNo++;
        if (!Deref(cur).header.isLeaf) {
            for (int i = 0; i <= Deref(cur).header.numKeys; i++) {
                enqueue(Deref(cur).body.inl.children[i].ptr);
            }
        }
    }
}

#endif /* DEBUG_ON */
