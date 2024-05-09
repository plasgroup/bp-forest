#include "bplustree.h"
#include "allocator.h"
#include "common.h"
#include "workload_types.h"

#include <assert.h>
#include <mram.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>


#define MAX_HEIGHT (CEIL_LOG2_UINT32((uint32_t)(MAX_NUM_NODES_IN_SEAT)) / FLOOR_LOG2_UINT32((uint32_t)(MAX_NR_CHILDREN)) + 1)

MBPTptr root;

__host uint32_t num_kvpairs;

// #define USE_LINEAR_SEARCH

#ifdef DEBUG_ON
#define QUEUE_SIZE (MAX_NR_CHILDREN * MAX_HEIGHT)
typedef struct Queue {  // queue for showing all nodes by BFS
    int tail;
    int head;
    MBPTptr ptrs[QUEUE_SIZE];
} Queue_t;

Queue_t queue;

inline void initQueue()
{
    queue.head = 0;
    queue.tail = 0;
    // printf("queue is initialized\n");
}

inline void enqueue(MBPTptr input)
{
    if ((queue.tail + 1) % (int)QUEUE_SIZE == queue.head) {
        printf("queue is full\n");
        return;
    }
    queue.ptrs[queue.tail] = input;
    queue.tail = (queue.tail + 1) % (int)QUEUE_SIZE;
    // printf("%p is enqueued\n",input);
}

inline bool isQueueEmpty()
{
    return queue.tail == queue.head;
}

inline MBPTptr dequeue()
{
    if (isQueueEmpty()) {
        printf("queue is empty\n");
        return NODE_NULLPTR;
    }
    MBPTptr ret = queue.ptrs[queue.head];
    queue.head = (queue.head + 1) % (int)QUEUE_SIZE;
    // printf("%p is dequeued\n",ret);
    return ret;
}

void showNode(MBPTptr, int);
#endif

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
static unsigned findUpperBoundWRAM(key_int64_t* keys, unsigned size, key_int64_t key)
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
void init_BPTree(void)
{
    root = Allocator_reset();
    root->header.isLeaf = true;
    root->header.numKeys = 0;
    root->body.lf.right = NODE_NULLPTR;
    root->body.lf.left = NODE_NULLPTR;
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
void BPTreeInsert(__dma_aligned key_int64_t key, __dma_aligned value_ptr_t value)
{
    uint8_t idx_child_cache[MAX_HEIGHT];
    unsigned depth = 0;

    MBPTptr node = root;
    while (!node->header.isLeaf) {
        const unsigned idx_child = findUpperBound(node->body.inl.keys, node->header.numKeys, key);
        idx_child_cache[depth++] = (uint8_t)idx_child;
        node = node->body.inl.children[idx_child].ptr;
    }

    const unsigned idx_to_insert = findUpperBound(node->body.lf.keys, node->header.numKeys, key);
    if (idx_to_insert != 0 && node->body.lf.keys[idx_to_insert - 1] == key) {
        node->body.lf.values[idx_to_insert - 1] = value;
        return;
    }
    num_kvpairs++;

    if (node->header.numKeys != MAX_NR_PAIRS) {
        if (idx_to_insert == node->header.numKeys) {
            mram_write(&key, &node->body.lf.keys[idx_to_insert], sizeof(key_int64_t));
            mram_write(&value, &node->body.lf.values[idx_to_insert], sizeof(value_ptr_t));
        } else {
            {
                __dma_aligned key_int64_t moved_keys[MAX_NR_PAIRS];
                mram_read(&node->body.lf.keys[idx_to_insert], &moved_keys[1], sizeof(key_int64_t) * (node->header.numKeys - idx_to_insert));
                moved_keys[0] = key;
                mram_write(&moved_keys[0], &node->body.lf.keys[idx_to_insert], sizeof(key_int64_t) * (node->header.numKeys - idx_to_insert + 1));
            }
            {
                __dma_aligned value_ptr_t moved_values[MAX_NR_PAIRS];
                mram_read(&node->body.lf.values[idx_to_insert], &moved_values[1], sizeof(value_ptr_t) * (node->header.numKeys - idx_to_insert));
                moved_values[0] = value;
                mram_write(&moved_values[0], &node->body.lf.values[idx_to_insert], sizeof(value_ptr_t) * (node->header.numKeys - idx_to_insert + 1));
            }
        }
        node->header.numKeys += 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
        if (depth != 0) {
            node->header.parent->body.inl.children[idx_child_cache[depth - 1]].numKeys = node->header.numKeys;
        }
#endif

    } else {  // split leaf
        unsigned node_numKeys = (MAX_NR_PAIRS + 2) / 2, new_node_numKeys = (MAX_NR_PAIRS + 1) / 2;
        bool is_new_node_leaf = true;

        MBPTptr new_node = Allocate_node();
        new_node->header.isLeaf = true;
        new_node->header.numKeys = new_node_numKeys;
        // new_node->header.parent = ...  // it depends on whether the parent is to be splitted
        new_node->body.lf.right = node->body.lf.right;
        if (new_node->body.lf.right != NODE_NULLPTR) {
            new_node->body.lf.right->body.lf.left = new_node;
        }
        new_node->body.lf.left = node;

        node->header.numKeys = node_numKeys;
        node->body.lf.right = new_node;

        if (idx_to_insert >= node_numKeys) {
            {
                __dma_aligned key_int64_t moved_keys[/* new_node_numKeys */ (MAX_NR_PAIRS + 1) / 2];
                mram_read(&node->body.lf.keys[node_numKeys], &moved_keys[0], sizeof(key_int64_t) * (new_node_numKeys - 1));
                memmove(&moved_keys[idx_to_insert - node_numKeys + 1], &moved_keys[idx_to_insert - node_numKeys], MAX_NR_PAIRS - idx_to_insert);
                moved_keys[idx_to_insert - node_numKeys] = key;
                key = moved_keys[0];
                mram_write(&moved_keys[0], &new_node->body.lf.keys[0], sizeof(key_int64_t) * new_node_numKeys);
            }
            {
                __dma_aligned value_ptr_t moved_values[/* new_node_numKeys */ (MAX_NR_PAIRS + 1) / 2];
                mram_read(&node->body.lf.values[node_numKeys], &moved_values[0], sizeof(value_ptr_t) * (new_node_numKeys - 1));
                memmove(&moved_values[idx_to_insert - node_numKeys + 1], &moved_values[idx_to_insert - node_numKeys], MAX_NR_PAIRS - idx_to_insert);
                moved_values[idx_to_insert - node_numKeys] = value;
                mram_write(&moved_values[0], &new_node->body.lf.values[0], sizeof(value_ptr_t) * new_node_numKeys);
            }
        } else {
            {
                __dma_aligned key_int64_t moved_keys[MAX_NR_PAIRS + 1];
                mram_read(&node->body.lf.keys[idx_to_insert], &moved_keys[1], sizeof(key_int64_t) * (MAX_NR_PAIRS - idx_to_insert));
                moved_keys[0] = key;
                key = moved_keys[node_numKeys - 1 - idx_to_insert];
                mram_write(&moved_keys[node_numKeys - 1 - idx_to_insert], &new_node->body.lf.keys[0], sizeof(key_int64_t) * new_node_numKeys);
                mram_write(&moved_keys[0], &node->body.lf.keys[idx_to_insert], sizeof(key_int64_t) * (node_numKeys - idx_to_insert));
            }
            {
                __dma_aligned value_ptr_t moved_values[MAX_NR_PAIRS + 1];
                mram_read(&node->body.lf.values[idx_to_insert], &moved_values[1], sizeof(value_ptr_t) * (MAX_NR_PAIRS - idx_to_insert));
                moved_values[0] = value;
                mram_write(&moved_values[node_numKeys - 1 - idx_to_insert], &new_node->body.lf.values[0], sizeof(value_ptr_t) * new_node_numKeys);
                mram_write(&moved_values[0], &node->body.lf.values[idx_to_insert], sizeof(value_ptr_t) * (node_numKeys - idx_to_insert));
            }
        }

        for (;; depth--) {
            if (depth == 0) {
                const MBPTptr new_root = Allocate_node();
                new_root->header.isLeaf = false;
                new_root->header.numKeys = 1;
                new_root->body.inl.keys[0] = key;
                new_root->body.inl.children[0].ptr = node;
                new_root->body.inl.children[1].ptr = new_node;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                new_root->body.inl.children[0].numKeys = node_numKeys;
                new_root->body.inl.children[0].isLeaf = is_new_node_leaf;
                new_root->body.inl.children[1].numKeys = new_node_numKeys;
                new_root->body.inl.children[1].isLeaf = is_new_node_leaf;
#endif

                root = new_root;
                node->header.parent = new_node->header.parent = new_root;

                return;
            }

            const MBPTptr parent = node->header.parent;
            const unsigned idx_to_insert = idx_child_cache[depth - 1];

            if (parent->header.numKeys != MAX_NR_CHILDREN - 1) {
                if (idx_to_insert == parent->header.numKeys) {
                    mram_write(&key, &parent->body.inl.keys[idx_to_insert], sizeof(key_int64_t));

#ifdef CACHE_CHILD_HEADER_IN_LINK
                    ChildInfo buf[2];
                    buf[0].ptr = node;
                    buf[0].numKeys = node_numKeys;
                    buf[0].isLeaf = is_new_node_leaf;
                    buf[1].ptr = new_node;
                    buf[1].numKeys = new_node_numKeys;
                    buf[1].isLeaf = is_new_node_leaf;
                    mram_write(&buf[0], &parent->body.inl.children[idx_to_insert], sizeof(buf));
#else  /* CACHE_CHILD_HEADER_IN_LINK */
                    __dma_aligned ChildInfo buf[8 / sizeof(ChildInfo)];
                    const unsigned idx_new_child = idx_to_insert + 1,
                                   nr_children_to_write = 8 / sizeof(ChildInfo),
                                   idx_to_write = idx_new_child / nr_children_to_write * nr_children_to_write,
                                   idx_new_child_in_buf = idx_new_child % nr_children_to_write;
                    if (idx_new_child_in_buf == 0) {
                        buf[0].ptr = new_node;
                    } else {
                        mram_read(&parent->body.inl.children[idx_to_write], &buf[0], sizeof(buf));
                        buf[idx_new_child_in_buf].ptr = new_node;
                    }
                    mram_write(&buf[0], &parent->body.inl.children[idx_to_write], sizeof(buf));
#endif /* CACHE_CHILD_HEADER_IN_LINK */

                } else {
                    {
                        __dma_aligned key_int64_t moved_keys[MAX_NR_CHILDREN - 1];
                        mram_read(&parent->body.inl.keys[idx_to_insert], &moved_keys[1], sizeof(key_int64_t) * (parent->header.numKeys - idx_to_insert));
                        moved_keys[0] = key;
                        mram_write(&moved_keys[0], &parent->body.inl.keys[idx_to_insert], sizeof(key_int64_t) * (parent->header.numKeys - idx_to_insert + 1));
                    }
                    {
                        __dma_aligned ChildInfo moved_children[MAX_NR_CHILDREN];
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        mram_read(&parent->body.inl.children[idx_to_insert + 1], &moved_children[2], sizeof(ChildInfo) * (parent->header.numKeys - idx_to_insert));
                        moved_children[0].ptr = node;
                        moved_children[0].numKeys = node_numKeys;
                        moved_children[0].isLeaf = is_new_node_leaf;
                        moved_children[1].ptr = new_node;
                        moved_children[1].numKeys = new_node_numKeys;
                        moved_children[1].isLeaf = is_new_node_leaf;
                        mram_write(&moved_children[0], &parent->body.inl.children[idx_to_insert], sizeof(ChildInfo) * (parent->header.numKeys - idx_to_insert + 2));
#else  /* CACHE_CHILD_HEADER_IN_LINK */
                        const unsigned idx_new_child = idx_to_insert + 1,
                                       alignof_write = 8 / sizeof(ChildInfo),
                                       idx_to_readwrite = idx_new_child / alignof_write * alignof_write,
                                       nr_head_margin = idx_to_readwrite - idx_new_child,
                                       nr_bytes_to_read = (sizeof(ChildInfo) * (parent->header.numKeys + 1 - idx_to_readwrite) + 7) / 8 * 8,
                                       nr_bytes_to_write = (sizeof(ChildInfo) * (parent->header.numKeys + 2 - idx_to_readwrite) + 7) / 8 * 8;
                        mram_read(&parent->body.inl.children[idx_to_readwrite], &moved_children[0], nr_bytes_to_read);
                        memmove(&moved_children[nr_head_margin + 1], &moved_children[nr_head_margin], nr_bytes_to_read - sizeof(ChildInfo) * nr_head_margin);
                        moved_children[nr_head_margin].ptr = new_node;
                        mram_write(&moved_children[0], &parent->body.inl.children[idx_to_readwrite], nr_bytes_to_write);
#endif /* CACHE_CHILD_HEADER_IN_LINK */
                    }
                }
                parent->header.numKeys += 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                if (depth != 1) {
                    parent->header.parent->body.inl.children[idx_child_cache[depth - 2]].numKeys = parent->header.numKeys;
                }
#endif

                new_node->header.parent = parent;
                return;

            } else {
                const unsigned parent_numKeys = MAX_NR_CHILDREN / 2, new_parent_numKeys = (MAX_NR_CHILDREN - 1) / 2;

                const MBPTptr new_parent = Allocate_node();
                new_parent->header.isLeaf = false;
                new_parent->header.numKeys = new_parent_numKeys;
                // new_parent->header.parent = ...  // it depends on whether the grandparent is to be splitted

                parent->header.numKeys = parent_numKeys;

                // split: distribute keys and children as follows
                //     input:
                //         keys:      parent.key[0..(idx_to_insert - 1)] ++ [key] ++ parent.key[idx_to_insert..(MAX_CHILD - 1)]
                //         children:  parent.children[0..idx_to_insert] ++ [new_node] ++ parent.children[(idx_to_insert + 1)..MAX_CHILD]
                //     output:
                //         keys:      parent.key[0..(parent_numKeys - 1)] ++ [key] ++ new_parent.key[0..(new_parent_numKeys - 1)]
                //         children:  parent.children[0..parent_numKeys] ++ new_parent.children[0..new_parent_numKeys]
                if (idx_to_insert >= parent_numKeys) {
                    unsigned idx_dest = new_parent_numKeys;
                    for (unsigned idx_src = MAX_NR_CHILDREN - 1; idx_src > idx_to_insert; idx_src--, idx_dest--) {
                        new_parent->body.inl.children[idx_dest] = parent->body.inl.children[idx_src];
                        new_parent->body.inl.children[idx_dest].ptr->header.parent = new_parent;
                    }
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    new_parent->body.inl.children[idx_dest].numKeys = new_node_numKeys;
                    new_parent->body.inl.children[idx_dest].isLeaf = is_new_node_leaf;
#endif
                    new_parent->body.inl.children[idx_dest--].ptr = new_node;
                    new_node->header.parent = new_parent;
                    if (idx_to_insert != parent_numKeys) {
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        new_parent->body.inl.children[idx_dest].numKeys = node_numKeys;
                        new_parent->body.inl.children[idx_dest].isLeaf = is_new_node_leaf;
#endif
                        new_parent->body.inl.children[idx_dest--].ptr = node;
                        node->header.parent = new_parent;
                    }
                    for (unsigned idx_src = idx_to_insert - 1; idx_src > parent_numKeys; idx_src--, idx_dest--) {
                        new_parent->body.inl.children[idx_dest] = parent->body.inl.children[idx_src];
                        new_parent->body.inl.children[idx_dest].ptr->header.parent = new_parent;
                    }
                } else {
                    for (unsigned idx_src = parent_numKeys, idx_dest = 0; idx_dest <= new_parent_numKeys; idx_src++, idx_dest++) {
                        new_parent->body.inl.children[idx_dest] = parent->body.inl.children[idx_src];
                        new_parent->body.inl.children[idx_dest].ptr->header.parent = new_parent;
                    }
                    for (unsigned idx_src = parent_numKeys - 1; idx_src > idx_to_insert; idx_src--) {
                        parent->body.inl.children[idx_src + 1] = parent->body.inl.children[idx_src];
                    }
                    parent->body.inl.children[idx_to_insert + 1].ptr = new_node;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    parent->body.inl.children[idx_to_insert + 1].numKeys = new_node_numKeys;
                    parent->body.inl.children[idx_to_insert + 1].isLeaf = is_new_node_leaf;
#endif
                    new_node->header.parent = parent;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    parent->body.inl.children[idx_to_insert].numKeys = node_numKeys;
                    parent->body.inl.children[idx_to_insert].isLeaf = is_new_node_leaf;
#endif
                }

                if (idx_to_insert > parent_numKeys) {
                    unsigned idx_dest = new_parent_numKeys - 1;
                    for (unsigned idx_src = MAX_NR_CHILDREN - 1; idx_src > idx_to_insert; idx_src--, idx_dest--) {
                        new_parent->body.inl.keys[idx_dest] = parent->body.inl.keys[idx_src - 1];
                    }
                    new_parent->body.inl.keys[idx_dest--] = key;
                    for (unsigned idx_src = idx_to_insert - 1; idx_src > parent_numKeys; idx_src--, idx_dest--) {
                        new_parent->body.inl.keys[idx_dest] = parent->body.inl.keys[idx_src];
                    }
                    key = parent->body.inl.keys[parent_numKeys];
                } else {
                    for (unsigned idx_src = parent_numKeys, idx_dest = 0; idx_src < MAX_NR_CHILDREN - 1; idx_src++, idx_dest++) {
                        new_parent->body.inl.keys[idx_dest] = parent->body.inl.keys[idx_src];
                    }
                    if (idx_to_insert != parent_numKeys) {
                        const key_int64_t to_grandparent = parent->body.inl.keys[parent_numKeys - 1];
                        for (unsigned idx_dest = parent_numKeys - 1; idx_dest > idx_to_insert; idx_dest--) {
                            parent->body.inl.keys[idx_dest] = parent->body.inl.keys[idx_dest - 1];
                        }
                        parent->body.inl.keys[idx_to_insert] = key;
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
value_ptr_t BPTreeGet(key_int64_t key)
{
    Node cache;

    MBPTptr node = root;
    mram_read(node, &cache, sizeof(Node));
    while (!cache.header.isLeaf) {
        node = cache.body.inl.children[findUpperBoundWRAM(cache.body.inl.keys, cache.header.numKeys, key)].ptr;
        mram_read(node, &cache, sizeof(Node));
    }

    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(cache.body.lf.keys, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && cache.body.lf.keys[idx_pair_plus_1 - 1] == key) {
        return cache.body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#elif defined(DMA_WHOLE_KEY_ARRAY)
value_ptr_t BPTreeGet(key_int64_t key)
{
    NodeHeaderAndKeys cache;

    MBPTptr node = root;
    mram_read(node, &cache, sizeof(NodeHeaderAndKeys));
    while (!cache.header.isLeaf) {
        node = node->body.inl.children[findUpperBoundWRAM(cache.body.inl.keys, cache.header.numKeys, key)].ptr;
        mram_read(node, &cache, sizeof(NodeHeaderAndKeys));
    }

    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(cache.body.lf.keys, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && cache.body.lf.keys[idx_pair_plus_1 - 1] == key) {
        return node->body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#elif defined(DMA_VALID_KEYS)
#ifdef CACHE_CHILD_HEADER_IN_LINK
value_ptr_t BPTreeGet(key_int64_t key)
{
    union {  // same structure
        NodeHeader header;
        ChildInfo child;
    } cache;

    MBPTptr node = root;
    mram_read(&node->header, &cache, sizeof(NodeHeader));
    while (!cache.header.isLeaf) {
        key_int64_t keys_cache[MAX_NR_CHILDREN - 1];
        mram_read(&node->body.inl.keys[0], &keys_cache[0], sizeof(key_int64_t) * cache.header.numKeys);

        mram_read(&node->body.inl.children[findUpperBoundWRAM(keys_cache, cache.header.numKeys, key)], &cache, sizeof(ChildInfo));
        node = cache.child.ptr;
    }

    key_int64_t keys_cache[MAX_NR_PAIRS];
    mram_read(&node->body.lf.keys[0], &keys_cache[0], sizeof(key_int64_t) * cache.header.numKeys);
    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(keys_cache, cache.header.numKeys, key);
    if (idx_pair_plus_1 != 0 && keys_cache[idx_pair_plus_1 - 1] == key) {
        return node->body.lf.values[idx_pair_plus_1 - 1];
    }

    return 0;
}
#else  /* CACHE_CHILD_HEADER_IN_LINK */
value_ptr_t BPTreeGet(key_int64_t key)
{
    NodeHeader header_cache;

    MBPTptr node = root;
    mram_read(&node->header, &header_cache, sizeof(NodeHeader));
    while (!header_cache.isLeaf) {
        key_int64_t keys_cache[MAX_NR_CHILDREN - 1];
        mram_read(&node->body.inl.keys[0], &keys_cache[0], sizeof(key_int64_t) * header_cache.numKeys);

        node = node->body.inl.children[findUpperBoundWRAM(keys_cache, header_cache.numKeys, key)].ptr;
        mram_read(&node->header, &header_cache, sizeof(NodeHeader));
    }

    key_int64_t keys_cache[MAX_NR_PAIRS];
    mram_read(&node->body.lf.keys[0], &keys_cache[0], sizeof(key_int64_t) * header_cache.numKeys);
    const unsigned idx_pair_plus_1 = findUpperBoundWRAM(keys_cache, header_cache.numKeys, key);
    if (idx_pair_plus_1 != 0 && keys_cache[idx_pair_plus_1 - 1] == key) {
        return node->body.lf.values[idx_pair_plus_1 - 1];
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
value_ptr_t BPTreeGet(key_int64_t key)
{
    MBPTptr node = root;
    while (!node->header.isLeaf) {
        node = node->body.inl.children[findUpperBound(node->body.inl.keys, node->header.numKeys, key)].ptr;
    }

    const unsigned idx_pair_plus_1 = findUpperBound(node->body.lf.keys, node->header.numKeys, key);
    if (idx_pair_plus_1 != 0 && node->body.lf.keys[idx_pair_plus_1 - 1] == key) {
        return node->body.lf.values[idx_pair_plus_1 - 1];
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
// KVPair BPTreeSucc(key_int64_t key)
// {
// }


#ifdef DEBUG_ON
void showNode(MBPTptr cur, int nodeNo)
{  // show single node
    printf("[Node No. %d]\n", nodeNo);
    if (cur->header.isLeaf == true) {
        cur == root ? printf("this is a Root LeafNode (addr %p)\n", cur)
                    : printf("this is a LeafNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->header.parent);
        printf("1. number of keys: %d\n", cur->header.numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->header.numKeys; i++) {
            printf("%lx ", cur->body.lf.keys[i]);
        }
        printf("]\n");
        printf("3. value pointers:[ ");
        for (int i = 0; i < cur->header.numKeys; i++) {
            printf("%lx ", cur->body.lf.values[i]);
        }
        printf("]\n");
        printf("4. leaf connections, left: %p right: %p\n", cur->body.lf.left,
            cur->body.lf.right);
    } else {
        cur == root ? printf("this is a Root InternalNode (addr %p)\n", cur)
                    : printf("this is an InternalNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->header.parent);
        printf("1. number of keys: %d\n", cur->header.numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->header.numKeys; i++) {
            printf("%lx ", cur->body.inl.keys[i]);
        }
        printf("]\n");
        printf("3. children:[ ");
        for (int i = 0; i <= cur->header.numKeys; i++) {
            printf("%p ", cur->body.inl.children[i].ptr);
        }
        printf("]\n");
    }
    printf("\n");
}

void BPTreePrintLeaves()
{
    MBPTptr leaf = root;
    while (!leaf->header.isLeaf) {
        leaf = leaf->body.inl.children[0].ptr;
    }

    int cnt = 0;
    while (leaf != NODE_NULLPTR) {
        showNode(leaf, cnt);
        leaf = leaf->body.lf.right;
        cnt++;
    }
    printf("\n");
}

void BPTreePrintKeys()
{
    MBPTptr leaf = root;
    while (!leaf->header.isLeaf) {
        leaf = leaf->body.inl.children[0].ptr;
    }

    int cnt = 0;
    while (leaf != NODE_NULLPTR) {
        for (unsigned i = 0; i < leaf->header.numKeys; i++) {
            printf("%lx ", leaf->body.lf.keys[i]);
        }
        leaf = leaf->body.lf.right;
        cnt++;
    }
    printf("\n");
}
bool BPTreeCheckStructure()
{
    bool success = true;
    initQueue();
    enqueue(root);
    for (int nodeNo = 0; !isQueueEmpty(); nodeNo++) {
        MBPTptr cur = dequeue();
        if (cur->header.isLeaf) {
            const MBPTptr left = cur->body.lf.left, right = cur->body.lf.right;
            if (left != NODE_NULLPTR) {
                if (!left->header.isLeaf) {
                    success = false;
                    printf("Node[%d]: %p is a leaf but %p->left (%p) is not.\n", nodeNo, cur, cur, left);
                }
                if (left->body.lf.right != cur) {
                    success = false;
                    printf("Node[%d]: %p->left->right == %p != %p\n", nodeNo, cur, left->body.lf.right, cur);
                }
            }
            if (right != NODE_NULLPTR) {
                if (!right->header.isLeaf) {
                    success = false;
                    printf("Node[%d]: %p is a leaf but %p->right (%p) is not.\n", nodeNo, cur, cur, right);
                }
                if (right->body.lf.left != cur) {
                    success = false;
                    printf("Node[%d]: %p->right->left == %p != %p\n", nodeNo, cur, right->body.lf.left, cur);
                }
            }
        } else {
            for (unsigned i = 0; i <= cur->header.numKeys; i++) {
                const ChildInfo child = cur->body.inl.children[i];
                if (child.ptr == NODE_NULLPTR) {
                    success = false;
                    printf("Node[%d]: %p->children[%u] == null\n", nodeNo, cur, i);
                } else {
                    if (child.ptr->header.parent != cur) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].ptr->header.parent == %p != %p\n", nodeNo, cur, i, child.ptr->header.parent, cur);
                    }
                    if (i != 0 && cur->body.inl.keys[i - 1] > (child.ptr->header.isLeaf ? child.ptr->body.lf.keys : child.ptr->body.inl.keys)[0]) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].ptr->header.numKeys < %p->key[%u]\n", nodeNo, cur, i, cur, i - 1);
                    }
                    if (i != cur->header.numKeys && (child.ptr->header.isLeaf ? child.ptr->body.lf.keys : child.ptr->body.inl.keys)[child.ptr->header.numKeys - 1] >= cur->body.inl.keys[i]) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].ptr->header.numKeys >= %p->key[%u]\n", nodeNo, cur, i, cur, i);
                    }
#ifdef CACHE_CHILD_HEADER_IN_LINK
                    if (child.numKeys != child.ptr->header.numKeys) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].numKeys == %u != %u\n", nodeNo, cur, i, child.numKeys, child.ptr->header.numKeys);
                    }
                    if (child.isLeaf != child.ptr->header.isLeaf) {
                        success = false;
                        printf("Node[%d]: %p->children[%u].isLeaf == %u != %u\n", nodeNo, cur, i, child.isLeaf, child.ptr->header.isLeaf);
                    }
#endif /* CACHE_CHILD_HEADER_IN_LINK */
                }
            }
            for (int i = 0; i <= cur->header.numKeys; i++) {
                enqueue(cur->body.inl.children[i].ptr);
            }
        }
    }
    return success;
}

void BPTreePrintRoot()
{
    printf("rootNode\n");
    showNode(root, 0);
}

void BPTreePrintAll()
{  // show all node (BFS)
    int nodeNo = 0;
    initQueue();
    enqueue(root);
    while (!isQueueEmpty()) {
        MBPTptr cur = dequeue();
        showNode(cur, nodeNo);
        nodeNo++;
        if (!cur->header.isLeaf) {
            for (int i = 0; i <= cur->header.numKeys; i++) {
                enqueue(cur->body.inl.children[i].ptr);
            }
        }
    }
}

#endif


void BPTreeSerialize(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest)
{
    MBPTptr leaf = root;
    while (!leaf->header.isLeaf) {
        leaf = leaf->body.inl.children[0].ptr;
    }

    do {
        for (unsigned i = 0; i < leaf->header.numKeys; i++) {
            *(keys_dest++) = leaf->body.lf.keys[i];
            *(values_dest++) = leaf->body.lf.values[i];
        }
        leaf = leaf->body.lf.right;
    } while (leaf != NODE_NULLPTR);

    init_BPTree();
}

uint32_t BPTreeExtractFirstPairs(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest, key_int64_t delimiter)
{
    MBPTptr leaf = root;
    while (!leaf->header.isLeaf) {
        leaf = leaf->body.inl.children[0].ptr;
    }

    uint32_t nr_serialized = 0;

    //--- copy&deletion: While copying keys and values in order from the left end of the tree, delete empty nodes ---//
    for (;;) {
        if (leaf->body.lf.keys[leaf->header.numKeys - 1] < delimiter) {  // copy all the pairs
            nr_serialized += leaf->header.numKeys;
            for (unsigned i = 0; i < leaf->header.numKeys; i++) {
                *(keys_dest++) = leaf->body.lf.keys[i];
                *(values_dest++) = leaf->body.lf.values[i];
            }
            MBPTptr deleted = leaf;
            leaf = leaf->body.lf.right;
            for (;;) {
                if (deleted == root) {  // the whole tree get empty
                    deleted->header.isLeaf = true;
                    deleted->header.numKeys = 0;
                    deleted->body.lf.right = deleted->body.lf.left = NODE_NULLPTR;
                    num_kvpairs -= nr_serialized;
                    return nr_serialized;
                } else {
                    const MBPTptr parent = deleted->header.parent;
                    const bool is_last_child = (parent->body.inl.children[parent->header.numKeys].ptr == deleted);
                    Free_node(deleted);
                    if (is_last_child) {
                        deleted = parent;
                    } else {
                        break;
                    }
                }
            }
        } else {  // copy some of the pairs
            unsigned n_move = 0;
            for (; leaf->body.lf.keys[n_move] < delimiter; n_move++) {  // TODO: binary search & bulk copy
                *(keys_dest++) = leaf->body.lf.keys[n_move];
                *(values_dest++) = leaf->body.lf.values[n_move];
            }
            if (n_move != 0) {
                nr_serialized += n_move;
                const unsigned orig_numKeys = leaf->header.numKeys;
                unsigned i = 0;
                for (; n_move < orig_numKeys; i++, n_move++) {
                    leaf->body.lf.keys[i] = leaf->body.lf.keys[n_move];
                    leaf->body.lf.values[i] = leaf->body.lf.values[n_move];
                }
                leaf->header.numKeys = i;
            }
            leaf->body.lf.left = NODE_NULLPTR;
            break;
        }
    }

    //--- hole-filling: Ensure no internal node points to the deleted node as a child ---//
    for (MBPTptr node = leaf; node != root;) {
        const MBPTptr parent = node->header.parent;
        if (parent->body.inl.children[0].ptr != node) {
            unsigned live_child_idx = findUpperBound(parent->body.inl.keys, parent->header.numKeys, delimiter);
            const unsigned orig_numKeys = parent->header.numKeys;
            unsigned i = 0;
            for (; live_child_idx < orig_numKeys; i++, live_child_idx++) {
                parent->body.inl.keys[i] = parent->body.inl.keys[live_child_idx];
                parent->body.inl.children[i] = parent->body.inl.children[live_child_idx];
            }
            parent->body.inl.children[i] = parent->body.inl.children[live_child_idx];
            parent->header.numKeys = i;
        }
#ifdef CACHE_CHILD_HEADER_IN_LINK
        parent->body.inl.children[0].numKeys = node->header.numKeys;
#endif
        node = parent;
    }

    //--- regularization: Ensure no node has too few contents ---//
    while (root->header.numKeys == 0) {  // while the root has too few contents
        const MBPTptr child = root->body.inl.children[0].ptr;
        Free_node(root);
        root = child;
    }
    if (root != leaf) {
        for (MBPTptr parent = root;;) {
            const MBPTptr node = parent->body.inl.children[0].ptr;
            if (node == leaf) {
                const unsigned MIN_NR_PAIRS = (MAX_NR_PAIRS + 1) / 2;
                if (node->header.numKeys < MIN_NR_PAIRS) {
                    const MBPTptr sibling = parent->body.inl.children[1].ptr;
                    const unsigned sum_num_pairs = node->header.numKeys + sibling->header.numKeys;
                    if (sum_num_pairs >= MIN_NR_PAIRS * 2) {  // move kv-pairs from the sibling
                        const unsigned n_move = MIN_NR_PAIRS - node->header.numKeys;
                        for (unsigned src = 0, dest = node->header.numKeys; src < n_move; src++, dest++) {
                            node->body.lf.keys[dest] = sibling->body.lf.keys[src];
                            node->body.lf.values[dest] = sibling->body.lf.values[src];
                        }
                        node->header.numKeys = MIN_NR_PAIRS;
                        parent->body.inl.keys[0] = sibling->body.lf.keys[n_move];
                        for (unsigned src = n_move, dest = 0; src < sibling->header.numKeys; src++, dest++) {
                            sibling->body.lf.keys[dest] = sibling->body.lf.keys[src];
                            sibling->body.lf.values[dest] = sibling->body.lf.values[src];
                        }
                        sibling->header.numKeys = sum_num_pairs - MIN_NR_PAIRS;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        parent->body.inl.children[0].ptr = node;
                        parent->body.inl.children[0].numKeys = MIN_NR_PAIRS;
                        parent->body.inl.children[0].isLeaf = true;
                        parent->body.inl.children[1].ptr = sibling;
                        parent->body.inl.children[1].numKeys = sum_num_pairs - MIN_NR_PAIRS;
                        parent->body.inl.children[1].isLeaf = true;
#endif

                    } else {  // merge node and the sibling
                        for (unsigned src = sibling->header.numKeys - 1, dest = sum_num_pairs - 1; dest >= node->header.numKeys; src--, dest--) {
                            sibling->body.lf.keys[dest] = sibling->body.lf.keys[src];
                            sibling->body.lf.values[dest] = sibling->body.lf.values[src];
                        }
                        for (unsigned i = 0; i < node->header.numKeys; i++) {
                            sibling->body.lf.keys[i] = node->body.lf.keys[i];
                            sibling->body.lf.values[i] = node->body.lf.values[i];
                        }
                        sibling->header.numKeys = sum_num_pairs;
                        sibling->body.lf.left = NODE_NULLPTR;
                        Free_node(node);

                        if (parent->header.numKeys <= 1) {
                            Free_node(parent);
                            root = sibling;
                        } else {
                            parent->body.inl.children[0].ptr = sibling;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            parent->body.inl.children[0].numKeys = sum_num_pairs;
                            parent->body.inl.children[0].isLeaf = true;
#endif
                            unsigned i = 1;
                            for (; i < parent->header.numKeys; i++) {
                                parent->body.inl.keys[i - 1] = parent->body.inl.keys[i];
                                parent->body.inl.children[i] = parent->body.inl.children[i + 1];
                            }
                            parent->header.numKeys = i - 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            parent->header.parent->body.inl.children[0].ptr = parent;
                            parent->header.parent->body.inl.children[0].numKeys = i - 1;
                            parent->header.parent->body.inl.children[0].isLeaf = false;
#endif
                        }
                    }
                }
                break;
            } else {
                const unsigned MIN_NR_KEYS = (MAX_NR_CHILDREN - 1) / 2;
                if (node->header.numKeys < MIN_NR_KEYS + 1) {
                    const MBPTptr sibling = parent->body.inl.children[1].ptr;
                    const unsigned sum_num_keys = node->header.numKeys + sibling->header.numKeys;
                    if (sum_num_keys >= MIN_NR_KEYS * 2 + 1) {  // move kv-pairs from the sibling
                        const unsigned n_move = MIN_NR_KEYS + 1 - node->header.numKeys;
                        for (unsigned i = 0; i < n_move; i++) {
                            sibling->body.inl.children[i].ptr->header.parent = node;
                        }
                        node->body.inl.keys[node->header.numKeys] = parent->body.inl.keys[0];
                        for (unsigned src = 0, dest = node->header.numKeys + 1; src < n_move - 1; src++, dest++) {
                            node->body.inl.keys[dest] = sibling->body.inl.keys[src];
                        }
                        parent->body.inl.keys[0] = sibling->body.inl.keys[n_move - 1];
                        for (unsigned src = n_move, dest = 0; src < sibling->header.numKeys; src++, dest++) {
                            sibling->body.inl.keys[dest] = sibling->body.inl.keys[src];
                        }

                        for (unsigned src = 0, dest = node->header.numKeys + 1; src < n_move; src++, dest++) {
                            node->body.inl.children[dest] = sibling->body.inl.children[src];
                        }
                        for (unsigned src = n_move, dest = 0; src < sibling->header.numKeys + 1; src++, dest++) {
                            sibling->body.inl.children[dest] = sibling->body.inl.children[src];
                        }

                        node->header.numKeys = MIN_NR_KEYS + 1;
                        sibling->header.numKeys = sum_num_keys - (MIN_NR_KEYS + 1);
#ifdef CACHE_CHILD_HEADER_IN_LINK
                        parent->body.inl.children[0].ptr = node;
                        parent->body.inl.children[0].numKeys = MIN_NR_KEYS + 1;
                        parent->body.inl.children[0].isLeaf = false;
                        parent->body.inl.children[1].ptr = sibling;
                        parent->body.inl.children[1].numKeys = sum_num_keys - (MIN_NR_KEYS + 1);
                        parent->body.inl.children[1].isLeaf = false;
#endif

                    } else {  // merge node and the sibling
                        _Static_assert(MAX_NR_CHILDREN % 2 == 0, "Eager merging requires even number for MAX_NR_CHILDREN");
                        for (unsigned i = 0; i < node->header.numKeys + 1; i++) {
                            node->body.inl.children[i].ptr->header.parent = sibling;
                        }
                        for (unsigned src = sibling->header.numKeys - 1, dest = sum_num_keys; dest >= node->header.numKeys + 1; src--, dest--) {
                            sibling->body.inl.keys[dest] = sibling->body.inl.keys[src];
                        }
                        sibling->body.inl.keys[node->header.numKeys] = parent->body.inl.keys[0];
                        for (unsigned i = 0; i < node->header.numKeys; i++) {
                            sibling->body.inl.keys[i] = node->body.inl.keys[i];
                        }

                        for (unsigned src = sibling->header.numKeys, dest = sum_num_keys + 1; dest >= node->header.numKeys + 1; src--, dest--) {
                            sibling->body.inl.children[dest] = sibling->body.inl.children[src];
                        }
                        for (unsigned i = 0; i < node->header.numKeys + 1; i++) {
                            sibling->body.inl.children[i] = node->body.inl.children[i];
                        }
                        sibling->header.numKeys = sum_num_keys + 1;
                        Free_node(node);

                        if (parent->header.numKeys <= 1) {
                            Free_node(parent);
                            root = sibling;
                        } else {
                            parent->body.inl.children[0].ptr = sibling;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            parent->body.inl.children[0].numKeys = sum_num_keys + 1;
                            parent->body.inl.children[0].isLeaf = false;
#endif
                            unsigned i = 1;
                            for (; i < parent->header.numKeys; i++) {
                                parent->body.inl.keys[i - 1] = parent->body.inl.keys[i];
                                parent->body.inl.children[i] = parent->body.inl.children[i + 1];
                            }
                            parent->header.numKeys = i - 1;
#ifdef CACHE_CHILD_HEADER_IN_LINK
                            parent->header.parent->body.inl.children[0].ptr = parent;
                            parent->header.parent->body.inl.children[0].numKeys = i - 1;
                            parent->header.parent->body.inl.children[0].isLeaf = false;
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

key_int64_t BPTreeNthKeyFromLeft(uint32_t nth)
{
    MBPTptr leaf = root;
    while (!leaf->header.isLeaf) {
        leaf = leaf->body.inl.children[0].ptr;
    }

    for (unsigned i = 0; i < 100; i++, leaf = leaf->body.lf.right) {
        if (nth < leaf->header.numKeys) {
            return leaf->body.lf.keys[nth];
        } else {
            nth -= leaf->header.numKeys;
        }
    }
    return 0;
}
key_int64_t BPTreeNthKeyFromRight(uint32_t nth)
{
    MBPTptr leaf = root;
    while (!leaf->header.isLeaf) {
        leaf = leaf->body.inl.children[leaf->header.numKeys].ptr;
    }

    for (;; leaf = leaf->body.lf.left) {
        if (nth < leaf->header.numKeys) {
            return leaf->body.lf.keys[leaf->header.numKeys - 1 - nth];
        } else {
            nth -= leaf->header.numKeys;
        }
    }
}

void BPTreeInsertSortedPairsToLeft(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs)
{
    for (uint32_t i = 0; i < nr_pairs; i++) {
        BPTreeInsert(keys_src[i], values_src[i]);
    }
}
void BPTreeInsertSortedPairsToRight(const key_int64_t __mram_ptr* keys_src, const value_ptr_t __mram_ptr* values_src, uint32_t nr_pairs)
{
    for (uint32_t i = 0; i < nr_pairs; i++) {
        BPTreeInsert(keys_src[i], values_src[i]);
    }
}
