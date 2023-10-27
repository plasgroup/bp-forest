#include "bplustree.h"
#include "common.h"
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>

// #define USE_LINEAR_SEARCH

// nodes(keys and pointers)
__mram BPTreeNode nodes[MAX_NUM_BPTREE_IN_DPU][MAX_NODE_NUM];

#ifndef ALLOC_WITH_BITMAP
#ifndef ALLOC_WITH_FREE_LIST
int free_node_index_stack_head[MAX_NUM_BPTREE_IN_DPU] = {-1};
__mram int free_node_index_stack[MAX_NUM_BPTREE_IN_DPU][MAX_NODE_NUM];
int max_node_index[MAX_NUM_BPTREE_IN_DPU] = {-1};
#endif
#endif
#ifdef ALLOC_WITH_BITMAP
#define BITMAP_NUM_ELEMS ((MAX_NODE_NUM_PER_TREE + 31) / 32)
uint32_t node_bitmap[MAX_NUM_BPTREE_IN_DPU][BITMAP_NUM_ELEMS];
// 初期化
void init_node_bitmap(uint32_t tasklet_id)
{
    for (int i = 0; i < (MAX_NODE_NUM_PER_TREE + 31) / 32; i++) {
        node_bitmap[tasklet_id][i] = 0;
    }
}
// ノードの割り当て
MBPTptr newBPTreeNode(uint32_t tasklet_id)
{
    int i = 0;
    MBPTptr p;
    while (node_bitmap[tasklet_id][i] == -1) {  // 空きが無い場合は次の32bitへ
        i++;
    }
    assert(i != (MAX_NODE_NUM_PER_TREE + 31));
    for (int j = 0; j < 32; j++) {
        if (!(node_bitmap[tasklet_id][i] & (1 << j))) {
            p = &nodes[tasklet_id][32 * i + j];
            return p;
        }
    }
    printf("error: nodes buffer is full\n");
    return NULL;
}
// ノードの解放
void freeBPTreeNode(MBPTptr p, int tasklet_id)
{
    int index = p - (MBPTptr)&nodes[tasklet_id];
    int i = index / 32;
    int j = index % 32;
    node_bitmap[tasklet_id][i] &= ~(1 << j);
    return;
}

void freeBPTree(MBPTptr p, int tasklet_id)
{
    p = NULL;
}
#endif

#ifdef ALLOC_WITH_FREE_LIST
MBPTptr free_list_head[MAX_NUM_BPTREE_IN_DPU];
// 初期化
void init_free_list(uint32_t tasklet_id)
{
    free_list_head[tasklet_id] = &nodes[tasklet_id];
    for (int i = 0; i < MAX_NODE_NUM_FOR_A_TASKLET) {
        nodes[MAX_NUM_BPTREE_IN_DPU][i].offset_next = 0;
    }
}
// ノードの割り当て
MBPTptr newBPTreeNode(uint32_t tasklet_id)
{
    MBPTptr p = free_list_head[tasklet_id];
    free_list_head[tasklet_id] = p + p.offset_next;
    return p;
}
// ノードの解放
void freeBPTreeNode(MBPTptr p, uint32_t tasklet_id)
{
    *p.offset_next = (free_list_head[tasklet_id] - p) - 1;
    free_list_head[tasklet_id] = p;
    return;
}

void freeBPTree(MBPTptr p, int tasklet_id)
{
    p = NULL;
}
#endif
int height[NR_TASKLETS] = {1};
MBPTptr root[NR_TASKLETS];

int NumOfNodes[NR_TASKLETS] = {0};

int tree_bitmap = 0;

#ifdef DEBUG_ON
typedef struct Queue {  // queue for showing all nodes by BFS
    int tail;
    int head;
    MBPTptr ptrs[MAX_NODE_NUM];
} Queue_t;

__mram_ptr Queue_t* queue[NR_TASKLETS];

void initQueue(__mram_ptr Queue_t** queue, uint32_t tasklet_id)
{
    queue[tasklet_id]->node.head = 0;
    queue[tasklet_id]->node.tail = -1;
    // printf("queue is initialized\n");
}

void enqueue(__mram_ptr Queue_t** queue, MBPTptr input, uint32_t tasklet_id)
{
    if ((queue[tasklet_id]->node.tail + 2) % MAX_NODE_NUM == queue[tasklet_id]->node.head) {
        printf("queue is full\n");
        return;
    }
    queue[tasklet_id]->node.ptrs[(queue[tasklet_id]->node.tail + 1) % MAX_NODE_NUM] = input;
    queue[tasklet_id]->node.tail = (queue[tasklet_id]->node.tail + 1) % MAX_NODE_NUM;
    // printf("%p is enqueued\n",input);
}

MBPTptr dequeue(__mram_ptr Queue_t** queue, uint32_t tasklet_id)
{
    MBPTptr ret;
    if ((queue[tasklet_id]->node.tail + 1) % MAX_NODE_NUM == queue[tasklet_id]->node.head) {
        printf("queue is empty\n");
        return NULL;
    }
    ret = queue[tasklet_id]->node.ptrs[queue[tasklet_id]->node.head];
    queue[tasklet_id]->node.head = (queue[tasklet_id]->node.head + 1) % MAX_NODE_NUM;
    // printf("%p is dequeued\n",ret);
    return ret;
}

void showNode(MBPTptr, int);
#endif
#ifndef ALLOC_WITH_BITMAP
#ifndef ALLOC_WITH_FREE_LIST
MBPTptr newBPTreeNode(uint32_t tasklet_id)
{
    MBPTptr p;
    if (free_node_index_stack_head[tasklet_id] >= 0) {  // if there is gap in nodes array
        p = &nodes[tasklet_id]
                  [free_node_index_stack[tasklet_id]
                                        [free_node_index_stack_head[tasklet_id]--]];
    } else
        p = &nodes[tasklet_id][++max_node_index[tasklet_id]];
    p->node.parent = NULL;
    p->node.isRoot = false;
    p->node.isLeaf = false;
    p->node.numKeys = 0;
    NumOfNodes[tasklet_id]++;
    return p;
}

void freeBPTree(MBPTptr p, int tasklet_id)
{

    free_node_index_stack[tasklet_id][++free_node_index_stack_head[tasklet_id]] = p - (MBPTptr)&nodes[tasklet_id];
    NumOfNodes[tasklet_id]--;
    for (int i = 0; i < p->node.numKeys; i++) {
        freeBPTree(p->node.ptrs.inl.children[i], tasklet_id);
    }
    p = NULL;
}
#endif
#endif
MBPTptr malloc_tree()
{
    int tree_id;
    MBPTptr p = NULL;
    for (int i = 0; i < MAX_NUM_TREES_IN_DPU; i++) {
        if (tree_bitmap & !((1 << i))) {  // 木が空いているかどうか
            tree_id = i;
            tree_bitmap |= (1 << i);
        }
        break;
    }
    p = root[tree_id];
    return p;
}

void delete_tree(int tid)
{
    tree_bitmap &= ~(1 << tid);
    freeBPTree(root[tid], tid);
    return;
}

// binary search
#ifndef USE_LINEAR_SEARCH
int findKeyPos(MBPTptr n, key_int64_t key)
{
    int l = 0, r = n->node.numKeys;
    if (key < n->node.key[l])
        return l;
    if (n->node.key[r - 1] <= key)
        return r;
    while (l < r - 1) {
        int mid = (l + r) >> 1;
        if (n->node.key[mid - 1] > key)
            r = mid;
        else
            l = mid;
    }
    return l;
}
#endif

#ifdef USE_LINEAR_SEARCH
// linear search
int findKeyPos(MBPTptr n, key_int64_t key)
{
    int ret = 0;
    for (int ret = 0; ret < n->node.numKeys; ret++) {
        if (n->node.key[ret] <= key)
            return ret;
    }
    return ret;
}
#endif
MBPTptr findLeaf(key_int64_t key, uint32_t tasklet_id)
{
    MBPTptr n = root[tasklet_id];
    while (true) {
        if (n->node.isLeaf == true)
            break;
        if (key < n->node.key[0]) {
            n = n->node.ptrs.inl.children[0];
        } else {
            int i = findKeyPos(n, key);
#ifdef DEBUG_ON
            // printf("findLeaf:Node = %p, key = %d, i = %d\n",n, key,i);
#endif
            n = n->node.ptrs.inl.children[i];
        }
    }
    return n;
}
void insert(MBPTptr cur, key_int64_t, value_ptr_t, MBPTptr, uint32_t);
void split(MBPTptr cur, uint32_t tasklet_id)
{
    // cur splits into cur and n
    // copy cur[Mid+1 .. MAX_CHILD] to n[0 .. n->node.key_num-1]
    MBPTptr n = newBPTreeNode(tasklet_id);
    int Mid = (MAX_CHILD + 1) >> 1;
    n->node.isLeaf = cur->node.isLeaf;
    n->node.numKeys = MAX_CHILD - Mid;
    if (!n->node.isLeaf) {  // n is InternalNode
        for (int i = Mid; i < MAX_CHILD; i++) {
            n->node.ptrs.inl.children[i - Mid] = cur->node.ptrs.inl.children[i];
            n->node.key[i - Mid] = cur->node.key[i];
            n->node.ptrs.inl.children[i - Mid]->node.parent = n;
            cur->node.numKeys = Mid - 1;
        }
        n->node.ptrs.inl.children[MAX_CHILD - Mid] = cur->node.ptrs.inl.children[MAX_CHILD];
        n->node.ptrs.inl.children[MAX_CHILD - Mid]->node.parent = n;
    } else {  // n is LeafNode
        n->node.ptrs.lf.right = NULL;
        n->node.ptrs.lf.left = NULL;
        for (int i = Mid; i < MAX_CHILD; i++) {
            n->node.ptrs.lf.value[i - Mid] = cur->node.ptrs.lf.value[i];
            n->node.key[i - Mid] = cur->node.key[i];
            cur->node.numKeys = Mid;
        }
    }
    if (cur->node.isRoot) {  // root Node splits
        // Create a new root
        root[tasklet_id] = newBPTreeNode(tasklet_id);
        root[tasklet_id]->node.isRoot = true;
        root[tasklet_id]->node.isLeaf = false;
        root[tasklet_id]->node.numKeys = 1;
        root[tasklet_id]->node.ptrs.inl.children[0] = cur;
        root[tasklet_id]->node.ptrs.inl.children[1] = n;
        cur->node.parent = n->node.parent = root[tasklet_id];
        cur->node.isRoot = false;
        if (cur->node.isLeaf) {
            cur->node.ptrs.lf.right = n;
            n->node.ptrs.lf.left = cur;
            root[tasklet_id]->node.key[0] = n->node.key[0];
        } else {
            root[tasklet_id]->node.key[0] = cur->node.key[Mid - 1];
        }
        height[tasklet_id]++;
    } else {  // insert n to cur->node.parent
        n->node.parent = cur->node.parent;
        if (cur->node.isLeaf) {
            insert(n->node.parent, n->node.key[0], 0, n, tasklet_id);
        } else {
            insert(cur->node.parent, cur->node.key[Mid - 1], 0, n, tasklet_id);
        }
    }
}

void insert(MBPTptr cur, key_int64_t key, value_ptr_t value, MBPTptr n,
    uint32_t tasklet_id)
{
    int i, ins;
    ins = findKeyPos(cur, key);
    if (cur->node.isLeaf == true) {                       // inserted into a Leaf node
        if (ins != 0 && cur->node.key[ins - 1] == key) {  // key already exist, update the value
            cur->node.ptrs.lf.value[ins - 1] = value;
        } else {  // key doesn't already exist
            for (i = cur->node.numKeys; i > ins; i--) {
                cur->node.key[i] = cur->node.key[i - 1];
                cur->node.ptrs.lf.value[i] = cur->node.ptrs.lf.value[i - 1];
            }
            cur->node.key[ins] = key;
            cur->node.ptrs.lf.value[ins] = value;
            cur->node.numKeys++;
        }

    } else {  // inserted into an internal node by split
        cur->node.ptrs.inl.children[cur->node.numKeys + 1] = cur->node.ptrs.inl.children[cur->node.numKeys];
        for (i = cur->node.numKeys; i > ins; i--) {
            cur->node.ptrs.inl.children[i] = cur->node.ptrs.inl.children[i - 1];
            cur->node.key[i] = cur->node.key[i - 1];
        }
        cur->node.key[ins] = key;
        cur->node.ptrs.inl.children[ins + 1] = n;
        cur->node.numKeys++;
        MBPTptr firstChild = cur->node.ptrs.inl.children[0];
        if (firstChild->node.isLeaf == true) {  // the child is Leaf
            if (ins > 0) {
                MBPTptr prevChild;
                MBPTptr nextChild;
                prevChild = cur->node.ptrs.inl.children[ins];
                nextChild = prevChild->node.ptrs.lf.right;
                prevChild->node.ptrs.lf.right = n;
                n->node.ptrs.lf.right = nextChild;
                n->node.ptrs.lf.left = prevChild;
                if (nextChild != NULL)
                    nextChild->node.ptrs.lf.left = n;
            } else {  // do not have a prevChild
                MBPTptr nextChild = cur->node.ptrs.inl.children[2];
                n->node.ptrs.lf.right = cur->node.ptrs.inl.children[2];
                n->node.ptrs.lf.left = cur->node.ptrs.inl.children[0];
                firstChild->node.ptrs.lf.right = n;
                if (nextChild != NULL)
                    nextChild->node.ptrs.lf.left = n;
            }
        }
    }
    if (cur->node.numKeys == MAX_CHILD)
        split(cur, tasklet_id);  // key is full
}

void init_BPTree(uint32_t tasklet_id)
{
    NumOfNodes[tasklet_id] = 0;
    height[tasklet_id] = 1;
    root[tasklet_id] = newBPTreeNode(tasklet_id);
    root[tasklet_id]->node.numKeys = 0;
    root[tasklet_id]->node.isRoot = true;
    root[tasklet_id]->node.isLeaf = true;
    root[tasklet_id]->node.ptrs.lf.right = NULL;
    root[tasklet_id]->node.ptrs.lf.left = NULL;
    root[tasklet_id]->node.ptrs.lf.value[0] = 0;
}

int BPTreeInsert(key_int64_t key, value_ptr_t value, uint32_t tasklet_id)
{
    if (root[tasklet_id]->node.numKeys == 0) {  // if the tree is empty
        root[tasklet_id]->node.key[0] = key;
        root[tasklet_id]->node.numKeys++;
        root[tasklet_id]->node.ptrs.lf.value[0] = value;
        return true;
    }
    MBPTptr Leaf = findLeaf(key, tasklet_id);
    // int i = findKeyPos(Leaf, key);
    // printf("key:%ld,pos:%d\n",key,i);
    insert(Leaf, key, value, NULL, tasklet_id);
    // printf("inserted {key %d, value '%s'}.\n",key,(char*)value);
    return true;
}

value_ptr_t BPTreeGet(key_int64_t key, uint32_t tasklet_id)
{
    MBPTptr Leaf = findLeaf(key, tasklet_id);
    int i;
    for (i = 0; i < Leaf->node.numKeys; i++) {
        if (Leaf->node.key[i] == key) {
#ifdef DEBUG_ON
            // printf("[key = %ld: found]", Leaf->node.key[i]);
#endif
            return Leaf->node.ptrs.lf.value[i];
        }
    }
#ifdef DEBUG_ON
// printf("[key = %ld: not found]", key);
#endif
    return 0;
}
#ifdef DEBUG_ON
void showNode(MBPTptr cur, int nodeNo)
{  // show single node
    printf("[Node No. %d]\n", nodeNo);
    if (cur->node.isLeaf == true) {
        cur->node.isRoot ? printf("this is a Root LeafNode (addr %p)\n", cur)
                         : printf("this is a LeafNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->node.parent);
        printf("1. number of keys: %d\n", cur->node.numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->node.numKeys; i++) {
            printf("%lu ", cur->node.key[i]);
        }
        printf("]\n");
        printf("3. value pointers:[ ");
        for (int i = 0; i < cur->node.numKeys; i++) {
            printf("%ld ", cur->node.ptrs.lf.value[i]);
        }
        printf("]\n");
        printf("4. leaf connections, left: %p right: %p\n", cur->node.ptrs.lf.left,
            cur->node.ptrs.lf.right);
    } else {
        cur->node.isRoot ? printf("this is a Root InternalNode (addr %p)\n", cur)
                         : printf("this is an InternalNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->node.parent);
        printf("1. number of keys: %d\n", cur->node.numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->node.numKeys; i++) {
            printf("%lu ", cur->node.key[i]);
        }
        printf("]\n");
        printf("3. children:[ ");
        for (int i = 0; i <= cur->node.numKeys; i++) {
            printf("%p ", cur->node.ptrs.inl.children[i]);
        }
        printf("]\n");
    }
    printf("\n");
}

void BPTreePrintLeaves(uint32_t tasklet_id)
{
    MBPTptr Leaf = findLeaf(0, tasklet_id);
    int cnt = 0;
    while (Leaf != NULL) {
        showNode(Leaf, cnt);
        Leaf = Leaf->node.ptrs.lf.right;
        cnt++;
    }
    printf("\n");
}

void BPTreePrintRoot(uint32_t tasklet_id)
{
    printf("rootNode\n");
    showNode(root[tasklet_id], 0);
}

void BPTreePrintAll(uint32_t tasklet_id)
{  // show all node (BFS)
    int nodeNo = 0;
    initQueue(queue, tasklet_id);
    enqueue(queue, root[tasklet_id], tasklet_id);
    while ((queue[tasklet_id]->node.tail + 1) % MAX_NODE_NUM != queue[tasklet_id]->node.head) {
        MBPTptr cur = dequeue(queue, tasklet_id);
        showNode(cur, nodeNo);
        nodeNo++;
        if (!cur->node.isLeaf) {
            for (int i = 0; i <= cur->node.numKeys; i++) {
                enqueue(queue, cur->node.ptrs.inl.children[i], tasklet_id);
            }
        }
    }
}

#endif
int BPTree_GetNumOfNodes(uint32_t tasklet_id)
{
    return NumOfNodes[tasklet_id];
}

int BPTree_GetHeight(uint32_t tasklet_id) { return height[tasklet_id]; }
