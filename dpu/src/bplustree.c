#include "bplustree.h"
#include "allocator.h"
#include "common.h"
#include "workload_types.h"

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>


MBPTptr root;

__host uint32_t num_kvpairs;

// #define USE_LINEAR_SEARCH

#ifdef DEBUG_ON
#define QUEUE_SIZE (MAX_NR_CHILDREN * 20)
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
#endif

#ifdef USE_LINEAR_SEARCH
// linear search
static unsigned findUpperBound2(key_int64_t __mram_ptr* keys, unsigned size, key_int64_t key)
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
    root->isRoot = true;
    root->isLeaf = true;
    root->numKeys = 0;
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
void BPTreeInsert(key_int64_t key, value_ptr_t value)
{
    MBPTptr node = root;
    while (!node->isLeaf) {
        node = node->body.inl.children[findUpperBound(node->body.inl.keys, node->numKeys, key)];
    }

    const unsigned idx_to_insert = findUpperBound(node->body.lf.keys, node->numKeys, key);
    if (idx_to_insert != 0 && node->body.lf.keys[idx_to_insert - 1] == key) {
        node->body.lf.values[idx_to_insert - 1] = value;
        return;
    }
    num_kvpairs++;

    if (node->numKeys != MAX_NR_PAIRS) {
        for (unsigned idx_dest = node->numKeys; idx_dest > idx_to_insert; idx_dest--) {
            node->body.lf.keys[idx_dest] = node->body.lf.keys[idx_dest - 1];
            node->body.lf.values[idx_dest] = node->body.lf.values[idx_dest - 1];
        }
        node->body.lf.keys[idx_to_insert] = key;
        node->body.lf.values[idx_to_insert] = value;
        node->numKeys += 1;

    } else {  // split leaf
        const unsigned node_numKeys = (MAX_NR_PAIRS + 2) / 2, new_node_numKeys = (MAX_NR_PAIRS + 1) / 2;

        MBPTptr new_node = Allocate_node();
        new_node->isRoot = false;
        new_node->isLeaf = true;
        new_node->numKeys = new_node_numKeys;
        // new_node->parent = ...  // it depends on whether the parent is to be splitted
        new_node->body.lf.right = node->body.lf.right;
        if (new_node->body.lf.right != NODE_NULLPTR) {
            new_node->body.lf.right->body.lf.left = new_node;
        }
        new_node->body.lf.left = node;

        node->numKeys = node_numKeys;
        node->body.lf.right = new_node;

        if (idx_to_insert >= node_numKeys) {
            unsigned idx_dest = new_node_numKeys - 1;
            for (unsigned idx_src = MAX_NR_PAIRS - 1; idx_src >= idx_to_insert; idx_src--, idx_dest--) {
                new_node->body.lf.keys[idx_dest] = node->body.lf.keys[idx_src];
                new_node->body.lf.values[idx_dest] = node->body.lf.values[idx_src];
            }
            new_node->body.lf.keys[idx_dest] = key;
            new_node->body.lf.values[idx_dest--] = value;
            for (unsigned idx_src = idx_to_insert - 1; idx_src >= node_numKeys; idx_src--, idx_dest--) {
                new_node->body.lf.keys[idx_dest] = node->body.lf.keys[idx_src];
                new_node->body.lf.values[idx_dest] = node->body.lf.values[idx_src];
            }
        } else {
            for (unsigned idx_src = node_numKeys - 1, idx_dest = 0; idx_dest < new_node_numKeys; idx_src++, idx_dest++) {
                new_node->body.lf.keys[idx_dest] = node->body.lf.keys[idx_src];
                new_node->body.lf.values[idx_dest] = node->body.lf.values[idx_src];
            }
            for (unsigned idx_dest = node_numKeys - 1; idx_dest > idx_to_insert; idx_dest--) {
                node->body.lf.keys[idx_dest] = node->body.lf.keys[idx_dest - 1];
                node->body.lf.values[idx_dest] = node->body.lf.values[idx_dest - 1];
            }
            node->body.lf.keys[idx_to_insert] = key;
            node->body.lf.values[idx_to_insert] = value;
        }
        key = new_node->body.lf.keys[0];

        for (;;) {
            if (node == root) {
                const MBPTptr new_root = Allocate_node();
                new_root->isRoot = true;
                new_root->isLeaf = false;
                new_root->numKeys = 1;
                new_root->body.inl.keys[0] = key;
                new_root->body.inl.children[0] = node;
                new_root->body.inl.children[1] = new_node;

                root = new_root;
                node->isRoot = false;
                node->parent = new_node->parent = new_root;

                return;
            }

            const MBPTptr parent = node->parent;
            const unsigned idx_to_insert = findUpperBound(parent->body.inl.keys, parent->numKeys, key);

            if (parent->numKeys != MAX_NR_CHILDREN - 1) {
                for (unsigned idx_dest = parent->numKeys; idx_dest > idx_to_insert; idx_dest--) {
                    parent->body.inl.keys[idx_dest] = parent->body.inl.keys[idx_dest - 1];
                    parent->body.inl.children[idx_dest + 1] = parent->body.inl.children[idx_dest];
                }
                parent->body.inl.keys[idx_to_insert] = key;
                parent->body.inl.children[idx_to_insert + 1] = new_node;
                parent->numKeys += 1;

                new_node->parent = parent;
                return;

            } else {
                const unsigned parent_numKeys = MAX_NR_CHILDREN / 2, new_parent_numKeys = (MAX_NR_CHILDREN - 1) / 2;

                const MBPTptr new_parent = Allocate_node();
                new_parent->isRoot = false;
                new_parent->isLeaf = false;
                new_parent->numKeys = new_parent_numKeys;
                // new_parent->parent = ...  // it depends on whether the grandparent is to be splitted

                parent->numKeys = parent_numKeys;

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
                        new_parent->body.inl.children[idx_dest]->parent = new_parent;
                    }
                    new_parent->body.inl.children[idx_dest--] = new_node;
                    new_node->parent = new_parent;
                    for (unsigned idx_src = idx_to_insert; idx_src > parent_numKeys; idx_src--, idx_dest--) {
                        new_parent->body.inl.children[idx_dest] = parent->body.inl.children[idx_src];
                        new_parent->body.inl.children[idx_dest]->parent = new_parent;
                    }
                } else {
                    for (unsigned idx_src = parent_numKeys, idx_dest = 0; idx_dest <= new_parent_numKeys; idx_src++, idx_dest++) {
                        new_parent->body.inl.children[idx_dest] = parent->body.inl.children[idx_src];
                        new_parent->body.inl.children[idx_dest]->parent = new_parent;
                    }
                    for (unsigned idx_src = parent_numKeys - 1; idx_src > idx_to_insert; idx_src--) {
                        parent->body.inl.children[idx_src + 1] = parent->body.inl.children[idx_src];
                    }
                    parent->body.inl.children[idx_to_insert + 1] = new_node;
                    new_node->parent = parent;
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
                new_node = new_parent;
            }
        }
    }
}

/**
 * @brief 
 * get a value related to the key.
 * @param key key
 * @return value related to the key
 */
value_ptr_t BPTreeGet(key_int64_t key)
{
    MBPTptr node = root;
    while (!node->isLeaf) {
        node = node->body.inl.children[findUpperBound(node->body.inl.keys, node->numKeys, key)];
    }

    const unsigned idx_pair_plus_1 = findUpperBound(node->body.lf.keys, node->numKeys, key);
    if (idx_pair_plus_1 != 0 && node->body.lf.keys[idx_pair_plus_1 - 1] == key) {
#ifdef DEBUG_ON
        // printf("[key = %ld: found]", Leaf->key[i]);
#endif
        return node->body.lf.values[idx_pair_plus_1 - 1];
    }

#ifdef DEBUG_ON
// printf("[key = %ld: not found]", key);
#endif
    return 0;
}

bool findSucc(key_int64_t key, MBPTptr subtree, KVPair* succ)
{
    if (subtree->isLeaf) {
        const unsigned first_greater_idx = findUpperBound(subtree->body.lf.keys, subtree->numKeys, key);
        if (first_greater_idx < subtree->numKeys) {
            succ->key = subtree->body.lf.keys[first_greater_idx];
            succ->value = subtree->body.lf.values[first_greater_idx];
            return true;
        } else {
            return false;
        }
    } else {
        const unsigned first_greater_idx = findUpperBound(subtree->body.inl.keys, subtree->numKeys, key);
        if (first_greater_idx < subtree->numKeys) {
            if (!findSucc(key, subtree->body.inl.children[first_greater_idx], succ)) {
                subtree = subtree->body.inl.children[first_greater_idx + 1];
                while (!subtree->isLeaf) {
                    subtree = subtree->body.inl.children[0];
                }
                succ->key = subtree->body.lf.keys[0];
                succ->value = subtree->body.lf.values[0];
            }
            return true;
        } else {
            return findSucc(key, subtree->body.inl.children[subtree->numKeys], succ);
        }
    }
}
/**
 * @brief
 * get the pair with the smallest key greater than the given key.
 * @param key key
 * @return value related to the key
 */
KVPair BPTreeSucc(key_int64_t key)
{
    KVPair res;
    findSucc(key, root, &res);
    return res;
}


#ifdef DEBUG_ON
void showNode(MBPTptr cur, int nodeNo)
{  // show single node
    printf("[Node No. %d]\n", nodeNo);
    if (cur->isLeaf == true) {
        cur->isRoot ? printf("this is a Root LeafNode (addr %p)\n", cur)
                    : printf("this is a LeafNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->parent);
        printf("1. number of keys: %d\n", cur->numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%lx ", cur->body.lf.keys[i]);
        }
        printf("]\n");
        printf("3. value pointers:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%lx ", cur->body.lf.values[i]);
        }
        printf("]\n");
        printf("4. leaf connections, left: %p right: %p\n", cur->body.lf.left,
            cur->body.lf.right);
    } else {
        cur->isRoot ? printf("this is a Root InternalNode (addr %p)\n", cur)
                    : printf("this is an InternalNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->parent);
        printf("1. number of keys: %d\n", cur->numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%lx ", cur->body.inl.keys[i]);
        }
        printf("]\n");
        printf("3. children:[ ");
        for (int i = 0; i <= cur->numKeys; i++) {
            printf("%p ", cur->body.inl.children[i]);
        }
        printf("]\n");
    }
    printf("\n");
}

void BPTreePrintLeaves()
{
    MBPTptr leaf = root;
    while (!leaf->isLeaf) {
        leaf = leaf->body.inl.children[0];
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
    while (!leaf->isLeaf) {
        leaf = leaf->body.inl.children[0];
    }

    int cnt = 0;
    while (leaf != NODE_NULLPTR) {
        for (unsigned i = 0; i < leaf->numKeys; i++) {
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
        if (cur->isLeaf) {
            const MBPTptr left = cur->body.lf.left, right = cur->body.lf.right;
            if (left != NODE_NULLPTR) {
                if (!left->isLeaf) {
                    success = false;
                    printf("Node[%d]: %p is a leaf but %p->left (%p) is not.\n", nodeNo, cur, cur, left);
                }
                if (left->body.lf.right != cur) {
                    success = false;
                    printf("Node[%d]: %p->left->right == %p != %p\n", nodeNo, cur, left->body.lf.right, cur);
                }
            }
            if (right != NODE_NULLPTR) {
                if (!right->isLeaf) {
                    success = false;
                    printf("Node[%d]: %p is a leaf but %p->right (%p) is not.\n", nodeNo, cur, cur, right);
                }
                if (right->body.lf.left != cur) {
                    success = false;
                    printf("Node[%d]: %p->right->left == %p != %p\n", nodeNo, cur, right->body.lf.left, cur);
                }
            }
        } else {
            for (unsigned i = 0; i <= cur->numKeys; i++) {
                const MBPTptr child = cur->body.inl.children[i];
                if (child == NODE_NULLPTR) {
                    success = false;
                    printf("Node[%d]: %p->children[%u] == null\n", nodeNo, cur, i);
                } else {
                    if (child->parent != cur) {
                        success = false;
                        printf("Node[%d]: %p->children[%u]->parent == %p != %p\n", nodeNo, cur, i, child->parent, cur);
                    }
                    if (i != 0 && cur->body.inl.keys[i - 1] > (child->isLeaf ? child->body.lf.keys : child->body.inl.keys)[0]) {
                        success = false;
                        printf("Node[%d]: %p->children[%u]->keys < %p->key[%u]\n", nodeNo, cur, i, cur, i - 1);
                    }
                    if (i != cur->numKeys && (child->isLeaf ? child->body.lf.keys : child->body.inl.keys)[child->numKeys - 1] >= cur->body.inl.keys[i]) {
                        success = false;
                        printf("Node[%d]: %p->children[%u]->keys >= %p->key[%u]\n", nodeNo, cur, i, cur, i);
                    }
                }
            }
            for (int i = 0; i <= cur->numKeys; i++) {
                enqueue(cur->body.inl.children[i]);
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
        if (!cur->isLeaf) {
            for (int i = 0; i <= cur->numKeys; i++) {
                enqueue(cur->body.inl.children[i]);
            }
        }
    }
}

#endif


void BPTreeSerialize(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest)
{
    MBPTptr leaf = root;
    while (!leaf->isLeaf) {
        leaf = leaf->body.inl.children[0];
    }

    do {
        for (unsigned i = 0; i < leaf->numKeys; i++) {
            *(keys_dest++) = leaf->body.lf.keys[i];
            *(values_dest++) = leaf->body.lf.values[i];
        }
        leaf = leaf->body.lf.right;
    } while (leaf != NODE_NULLPTR);

    root = Allocator_reset();
    init_BPTree();
}

uint32_t BPTreeExtractFirstPairs(key_int64_t __mram_ptr* keys_dest, value_ptr_t __mram_ptr* values_dest, key_int64_t delimiter)
{
    MBPTptr root_node = root, leaf = root_node;
    while (!leaf->isLeaf) {
        leaf = leaf->body.inl.children[0];
    }

    uint32_t nr_serialized = 0;

    //--- copy&deletion: While copying keys and values in order from the left end of the tree, delete empty nodes ---//
    for (;;) {
        if (leaf->body.lf.keys[leaf->numKeys - 1] < delimiter) {  // copy all the pairs
            nr_serialized += leaf->numKeys;
            for (unsigned i = 0; i < leaf->numKeys; i++) {
                *(keys_dest++) = leaf->body.lf.keys[i];
                *(values_dest++) = leaf->body.lf.values[i];
            }
            MBPTptr deleted = leaf;
            leaf = leaf->body.lf.right;
            for (;;) {
                if (deleted == root_node) {  // the whole tree get empty
                    deleted->isLeaf = true;
                    deleted->numKeys = 0;
                    deleted->body.lf.right = deleted->body.lf.left = NODE_NULLPTR;
                    num_kvpairs -= nr_serialized;
                    return nr_serialized;
                } else {
                    const MBPTptr parent = deleted->parent;
                    const bool is_last_child = (parent->body.inl.children[parent->numKeys] == deleted);
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
                const unsigned orig_numKeys = leaf->numKeys;
                unsigned i = 0;
                for (; n_move < orig_numKeys; i++, n_move++) {
                    leaf->body.lf.keys[i] = leaf->body.lf.keys[n_move];
                    leaf->body.lf.values[i] = leaf->body.lf.values[n_move];
                }
                leaf->numKeys = i;
            }
            leaf->body.lf.left = NODE_NULLPTR;
            break;
        }
    }

    //--- hole-filling: Ensure no internal node points to the deleted node as a child ---//
    for (MBPTptr node = leaf; node != root_node;) {
        const MBPTptr parent = node->parent;
        if (parent->body.inl.children[0] != node) {
            unsigned live_child_idx = findUpperBound(parent->body.inl.keys, parent->numKeys, delimiter);
            const unsigned orig_numKeys = parent->numKeys;
            unsigned i = 0;
            for (; live_child_idx < orig_numKeys; i++, live_child_idx++) {
                parent->body.inl.keys[i] = parent->body.inl.keys[live_child_idx];
                parent->body.inl.children[i] = parent->body.inl.children[live_child_idx];
            }
            parent->body.inl.children[i] = parent->body.inl.children[live_child_idx];
            parent->numKeys = i;
        }
        node = parent;
    }

    //--- regularization: Ensure no node has too few contents ---//
    while (root_node->numKeys == 0) {  // while the root has too few contents
        const MBPTptr child = root_node->body.inl.children[0];
        Free_node(root_node);
        root_node = child;
    }
    if (root_node != leaf) {
        for (MBPTptr parent = root_node;;) {
            const MBPTptr node = parent->body.inl.children[0];
            if (node == leaf) {
                const unsigned MIN_NR_PAIRS = (MAX_NR_PAIRS + 1) / 2;
                if (node->numKeys < MIN_NR_PAIRS) {
                    const MBPTptr sibling = parent->body.inl.children[1];
                    const unsigned sum_num_pairs = node->numKeys + sibling->numKeys;
                    if (sum_num_pairs >= MIN_NR_PAIRS * 2) {  // move kv-pairs from the sibling
                        const unsigned n_move = MIN_NR_PAIRS - node->numKeys;
                        for (unsigned src = 0, dest = node->numKeys; src < n_move; src++, dest++) {
                            node->body.lf.keys[dest] = sibling->body.lf.keys[src];
                            node->body.lf.values[dest] = sibling->body.lf.values[src];
                        }
                        node->numKeys = MIN_NR_PAIRS;
                        parent->body.inl.keys[0] = sibling->body.lf.keys[n_move];
                        for (unsigned src = n_move, dest = 0; src < sibling->numKeys; src++, dest++) {
                            sibling->body.lf.keys[dest] = sibling->body.lf.keys[src];
                            sibling->body.lf.values[dest] = sibling->body.lf.values[src];
                        }
                        sibling->numKeys = sum_num_pairs - MIN_NR_PAIRS;

                    } else {  // merge node and the sibling
                        for (unsigned src = sibling->numKeys - 1, dest = sum_num_pairs - 1; dest >= node->numKeys; src--, dest--) {
                            sibling->body.lf.keys[dest] = sibling->body.lf.keys[src];
                            sibling->body.lf.values[dest] = sibling->body.lf.values[src];
                        }
                        for (unsigned i = 0; i < node->numKeys; i++) {
                            sibling->body.lf.keys[i] = node->body.lf.keys[i];
                            sibling->body.lf.values[i] = node->body.lf.values[i];
                        }
                        sibling->numKeys = sum_num_pairs;
                        sibling->body.lf.left = NODE_NULLPTR;
                        Free_node(node);

                        if (parent->numKeys <= 1) {
                            Free_node(parent);
                            root_node = sibling;
                        } else {
                            parent->body.inl.children[0] = sibling;
                            unsigned i = 1;
                            for (; i < parent->numKeys; i++) {
                                parent->body.inl.keys[i - 1] = parent->body.inl.keys[i];
                                parent->body.inl.children[i] = parent->body.inl.children[i + 1];
                            }
                            parent->numKeys = i - 1;
                        }
                    }
                }
                break;
            } else {
                const unsigned MIN_NR_KEYS = (MAX_NR_CHILDREN - 1) / 2;
                if (node->numKeys < MIN_NR_KEYS + 1) {
                    const MBPTptr sibling = parent->body.inl.children[1];
                    const unsigned sum_num_keys = node->numKeys + sibling->numKeys;
                    if (sum_num_keys >= MIN_NR_KEYS * 2 + 1) {  // move kv-pairs from the sibling
                        const unsigned n_move = MIN_NR_KEYS + 1 - node->numKeys;
                        for (unsigned i = 0; i < n_move; i++) {
                            sibling->body.inl.children[i]->parent = node;
                        }
                        node->body.inl.keys[node->numKeys] = parent->body.inl.keys[0];
                        for (unsigned src = 0, dest = node->numKeys + 1; src < n_move - 1; src++, dest++) {
                            node->body.inl.keys[dest] = sibling->body.inl.keys[src];
                        }
                        parent->body.inl.keys[0] = sibling->body.inl.keys[n_move - 1];
                        for (unsigned src = n_move, dest = 0; src < sibling->numKeys; src++, dest++) {
                            sibling->body.inl.keys[dest] = sibling->body.inl.keys[src];
                        }

                        for (unsigned src = 0, dest = node->numKeys + 1; src < n_move; src++, dest++) {
                            node->body.inl.children[dest] = sibling->body.inl.children[src];
                        }
                        for (unsigned src = n_move, dest = 0; src < sibling->numKeys + 1; src++, dest++) {
                            sibling->body.inl.children[dest] = sibling->body.inl.children[src];
                        }

                        node->numKeys = MIN_NR_KEYS + 1;
                        sibling->numKeys = sum_num_keys - (MIN_NR_KEYS + 1);

                    } else {  // merge node and the sibling
                        _Static_assert(MAX_NR_CHILDREN % 2 == 0, "Eager merging requires even number for MAX_NR_CHILDREN");
                        for (unsigned i = 0; i < node->numKeys + 1; i++) {
                            node->body.inl.children[i]->parent = sibling;
                        }
                        for (unsigned src = sibling->numKeys - 1, dest = sum_num_keys; dest >= node->numKeys + 1; src--, dest--) {
                            sibling->body.inl.keys[dest] = sibling->body.inl.keys[src];
                        }
                        sibling->body.inl.keys[node->numKeys] = parent->body.inl.keys[0];
                        for (unsigned i = 0; i < node->numKeys; i++) {
                            sibling->body.inl.keys[i] = node->body.inl.keys[i];
                        }

                        for (unsigned src = sibling->numKeys, dest = sum_num_keys + 1; dest >= node->numKeys + 1; src--, dest--) {
                            sibling->body.inl.children[dest] = sibling->body.inl.children[src];
                        }
                        for (unsigned i = 0; i < node->numKeys + 1; i++) {
                            sibling->body.inl.children[i] = node->body.inl.children[i];
                        }
                        sibling->numKeys = sum_num_keys + 1;
                        Free_node(node);

                        if (parent->numKeys <= 1) {
                            Free_node(parent);
                            root_node = sibling;
                        } else {
                            parent->body.inl.children[0] = sibling;
                            unsigned i = 1;
                            for (; i < parent->numKeys; i++) {
                                parent->body.inl.keys[i - 1] = parent->body.inl.keys[i];
                                parent->body.inl.children[i] = parent->body.inl.children[i + 1];
                            }
                            parent->numKeys = i - 1;
                        }
                        parent = sibling;
                        continue;
                    }
                }
                parent = node;
            }
        }
    }
    root = root_node;
    num_kvpairs -= nr_serialized;
    return nr_serialized;
}

key_int64_t BPTreeNthKeyFromLeft(uint32_t nth)
{
    MBPTptr leaf = root;
    while (!leaf->isLeaf) {
        leaf = leaf->body.inl.children[0];
    }

    for (unsigned i = 0; i < 100; i++, leaf = leaf->body.lf.right) {
        if (nth < leaf->numKeys) {
            return leaf->body.lf.keys[nth];
        } else {
            nth -= leaf->numKeys;
        }
    }
    return 0;
}
key_int64_t BPTreeNthKeyFromRight(uint32_t nth)
{
    MBPTptr leaf = root;
    while (!leaf->isLeaf) {
        leaf = leaf->body.inl.children[leaf->numKeys];
    }

    for (;; leaf = leaf->body.lf.left) {
        if (nth < leaf->numKeys) {
            return leaf->body.lf.keys[leaf->numKeys - 1 - nth];
        } else {
            nth -= leaf->numKeys;
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
