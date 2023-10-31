#include "bplustree.h"
#include "common.h"
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>

// #define USE_LINEAR_SEARCH

/* nodes(keys and pointers) */
__mram BPTreeNode nodes[NUM_SEAT_IN_A_DPU][MAX_NODE_NUM];

#ifndef ALLOC_WITH_BITMAP
#ifndef ALLOC_WITH_FREE_LIST
int free_node_index_stack_head[NUM_SEAT_IN_A_DPU] = {-1};
__mram int free_node_index_stack[NUM_SEAT_IN_A_DPU][MAX_NODE_NUM];
int max_node_index[NUM_SEAT_IN_A_DPU] = {-1};
#endif
#endif
#ifdef ALLOC_WITH_BITMAP
#define BITMAP_NUM_ELEMS ((MAX_NODE_NUM_PER_TREE + 31) / 32)
uint32_t node_bitmap[NUM_SEAT_IN_A_DPU][BITMAP_NUM_ELEMS];
// 初期化
void init_node_bitmap(uint32_t seat_id)
{
    for (int i = 0; i < (MAX_NODE_NUM_PER_TREE + 31) / 32; i++) {
        node_bitmap[seat_id][i] = 0;
    }
}
// ノードの割り当て
MBPTptr newBPTreeNode(uint32_t seat_id)
{
    int i = 0;
    MBPTptr p;
    while (node_bitmap[seat_id][i] == -1) {  // 空きが無い場合は次の32bitへ
        i++;
    }
    assert(i != (MAX_NODE_NUM_PER_TREE + 31));
    for (int j = 0; j < 32; j++) {
        if (!(node_bitmap[seat_id][i] & (1 << j))) {
            p = &nodes[seat_id][32 * i + j];
            return p;
        }
    }
    printf("error: nodes buffer is full\n");
    return NULL;
}
// ノードの解放
void freeBPTreeNode(MBPTptr p, int seat_id)
{
    int index = p - (MBPTptr)&nodes[seat_id];
    int i = index / 32;
    int j = index % 32;
    node_bitmap[seat_id][i] &= ~(1 << j);
    return;
}

void freeBPTree(MBPTptr p, int seat_id)
{
    p = NULL;
}
#endif

#ifdef ALLOC_WITH_FREE_LIST
MBPTptr free_list_head[NUM_SEAT_IN_A_DPU];
// 初期化
void init_free_list(uint32_t seat_id)
{
    free_list_head[seat_id] = &nodes[seat_id];
    for (int i = 0; i < MAX_NODE_NUM_FOR_A_TASKLET) {
        nodes[NUM_SEAT_IN_A_DPU][i].offset_next = 0;
    }
}
// ノードの割り当て
MBPTptr newBPTreeNode(uint32_t seat_id)
{
    MBPTptr p = free_list_head[seat_id];
    free_list_head[seat_id] = p + p.offset_next;
    return p;
}
// ノードの解放
void freeBPTreeNode(MBPTptr p, uint32_t seat_id)
{
    *p.offset_next = (free_list_head[seat_id] - p) - 1;
    free_list_head[seat_id] = p;
    return;
}

void freeBPTree(MBPTptr p, int seat_id)
{
    p = NULL;
}
#endif
int height[NUM_SEAT_IN_A_DPU] = {1};
MBPTptr root[NUM_SEAT_IN_A_DPU];

int NumOfNodes[NUM_SEAT_IN_A_DPU] = {0};

int tree_bitmap = 0;
#ifndef MRAM_NODE_ARRAY_SIZE
#define MRAM_NODE_ARRAY_SIZE (48 * 1024 * 1024)
#endif
#ifndef MAX_NODE_NUM_PER_TREE
#define MAX_NODE_NUM_PER_TREE (MRAM_NODE_ARRAY_SIZE / NUM_SEAT_IN_A_DPU / sizeof(BPTreeNode));
#endif
#define BITMAP_NUM_ELEMS (MAX_NODE_NUM_PER_TREE / 32)
uint32_t node_bitmap[NR_TASKLETS][BITMAP_NUM_ELEMS];

#ifdef DEBUG_ON
typedef struct Queue {  // queue for showing all nodes by BFS
    int tail;
    int head;
    MBPTptr ptrs[MAX_NODE_NUM];
} Queue_t;

__mram_ptr Queue_t* queue[NR_TASKLETS];

void initQueue(__mram_ptr Queue_t** queue, uint32_t seat_id)
{
    queue[seat_id]->head = 0;
    queue[seat_id]->tail = -1;
    // printf("queue is initialized\n");
}

void enqueue(__mram_ptr Queue_t** queue, MBPTptr input, uint32_t seat_id)
{
    if ((queue[seat_id]->tail + 2) % MAX_NODE_NUM == queue[seat_id]->head) {
        printf("queue is full\n");
        return;
    }
    queue[seat_id]->ptrs[(queue[seat_id]->tail + 1) % MAX_NODE_NUM] = input;
    queue[seat_id]->tail = (queue[seat_id]->tail + 1) % MAX_NODE_NUM;
    // printf("%p is enqueued\n",input);
}

MBPTptr dequeue(__mram_ptr Queue_t** queue, uint32_t seat_id)
{
    MBPTptr ret;
    if ((queue[seat_id]->tail + 1) % MAX_NODE_NUM == queue[seat_id]->head) {
        printf("queue is empty\n");
        return NULL;
    }
    ret = queue[seat_id]->ptrs[queue[seat_id]->head];
    queue[seat_id]->head = (queue[seat_id]->head + 1) % MAX_NODE_NUM;
    // printf("%p is dequeued\n",ret);
    return ret;
}

void showNode(MBPTptr, int);
#endif
#ifndef ALLOC_WITH_BITMAP
#ifndef ALLOC_WITH_FREE_LIST
MBPTptr newBPTreeNode(uint32_t seat_id)
{
    MBPTptr p;
    if (free_node_index_stack_head[seat_id] >= 0) {  // if there is gap in nodes array
        p = &nodes[seat_id]
                  [free_node_index_stack[seat_id]
                                        [free_node_index_stack_head[seat_id]--]];
    } else
        p = &nodes[seat_id][++max_node_index[seat_id]];
    p->parent = NULL;
    p->isRoot = false;
    p->isLeaf = false;
    p->numKeys = 0;
    NumOfNodes[seat_id]++;
    return p;
}

void freeBPTree(MBPTptr p, int seat_id)
{

    free_node_index_stack[seat_id][++free_node_index_stack_head[seat_id]] = p - (MBPTptr)&nodes[seat_id];
    NumOfNodes[seat_id]--;
    for (int i = 0; i < p->numKeys; i++) {
        freeBPTree(p->ptrs.inl.children[i], seat_id);
    }
    p = NULL;
}
#endif
#endif
MBPTptr malloc_tree()
{
    int tree_id;
    MBPTptr p = NULL;
    for (int i = 0; i < NUM_SEAT_IN_A_DPU; i++) {
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
    int l = 0, r = n->numKeys;
    if (key < n->key[l])
        return l;
    if (n->key[r - 1] <= key)
        return r;
    while (l < r - 1) {
        int mid = (l + r) >> 1;
        if (n->key[mid - 1] > key)
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
    for (int ret = 0; ret < n->numKeys; ret++) {
        if (n->key[ret] <= key)
            return ret;
    }
    return ret;
}
#endif
MBPTptr findLeafOfSubtree(key_int64_t key, MBPTptr subtree)
{
    while (true) {
        if (subtree->isLeaf == true)
            break;
        if (key < subtree->key[0]) {
            subtree = subtree->ptrs.inl.children[0];
        } else {
            int i = findKeyPos(subtree, key);
#ifdef DEBUG_ON
            // printf("findLeaf:Node = %p, key = %d, i = %d\n",n, key,i);
#endif
            subtree = subtree->ptrs.inl.children[i];
        }
    }
    return subtree;
}
MBPTptr findLeaf(key_int64_t key, uint32_t seat_id)
{
    return findLeafOfSubtree(key, root[seat_id]);
}

void insert(MBPTptr cur, key_int64_t, value_ptr_t, MBPTptr, uint32_t);
void split(MBPTptr cur, uint32_t seat_id)
{
    // cur splits into cur and n
    // copy cur[Mid+1 .. MAX_CHILD] to n[0 .. n->key_num-1]
    MBPTptr n = newBPTreeNode(seat_id);
    int Mid = (MAX_CHILD + 1) >> 1;
    n->isLeaf = cur->isLeaf;
    n->numKeys = MAX_CHILD - Mid;
    if (!n->isLeaf) {  // n is InternalNode
        for (int i = Mid; i < MAX_CHILD; i++) {
            n->ptrs.inl.children[i - Mid] = cur->ptrs.inl.children[i];
            n->key[i - Mid] = cur->key[i];
            n->ptrs.inl.children[i - Mid]->parent = n;
            cur->numKeys = Mid - 1;
        }
        n->ptrs.inl.children[MAX_CHILD - Mid] = cur->ptrs.inl.children[MAX_CHILD];
        n->ptrs.inl.children[MAX_CHILD - Mid]->parent = n;
    } else {  // n is LeafNode
        n->ptrs.lf.right = NULL;
        n->ptrs.lf.left = NULL;
        for (int i = Mid; i < MAX_CHILD; i++) {
            n->ptrs.lf.value[i - Mid] = cur->ptrs.lf.value[i];
            n->key[i - Mid] = cur->key[i];
            cur->numKeys = Mid;
        }
    }
    if (cur->isRoot) {  // root Node splits
        // Create a new root
        root[seat_id] = newBPTreeNode(seat_id);
        root[seat_id]->isRoot = true;
        root[seat_id]->isLeaf = false;
        root[seat_id]->numKeys = 1;
        root[seat_id]->ptrs.inl.children[0] = cur;
        root[seat_id]->ptrs.inl.children[1] = n;
        cur->parent = n->parent = root[seat_id];
        cur->isRoot = false;
        if (cur->isLeaf) {
            cur->ptrs.lf.right = n;
            n->ptrs.lf.left = cur;
            root[seat_id]->key[0] = n->key[0];
        } else {
            root[seat_id]->key[0] = cur->key[Mid - 1];
        }
        height[seat_id]++;
    } else {  // insert n to cur->parent
        n->parent = cur->parent;
        if (cur->isLeaf) {
            insert(n->parent, n->key[0], 0, n, seat_id);
        } else {
            insert(cur->parent, cur->key[Mid - 1], 0, n, seat_id);
        }
    }
}

void insert(MBPTptr cur, key_int64_t key, value_ptr_t value, MBPTptr n,
    uint32_t seat_id)
{
    int i, ins;
    ins = findKeyPos(cur, key);
    if (cur->isLeaf == true) {                       // inserted into a Leaf node
        if (ins != 0 && cur->key[ins - 1] == key) {  // key already exist, update the value
            cur->ptrs.lf.value[ins - 1] = value;
        } else {  // key doesn't already exist
            for (i = cur->numKeys; i > ins; i--) {
                cur->key[i] = cur->key[i - 1];
                cur->ptrs.lf.value[i] = cur->ptrs.lf.value[i - 1];
            }
            cur->key[ins] = key;
            cur->ptrs.lf.value[ins] = value;
            cur->numKeys++;
        }

    } else {  // inserted into an internal node by split
        cur->ptrs.inl.children[cur->numKeys + 1] = cur->ptrs.inl.children[cur->numKeys];
        for (i = cur->numKeys; i > ins; i--) {
            cur->ptrs.inl.children[i] = cur->ptrs.inl.children[i - 1];
            cur->key[i] = cur->key[i - 1];
        }
        cur->key[ins] = key;
        cur->ptrs.inl.children[ins + 1] = n;
        cur->numKeys++;
        MBPTptr firstChild = cur->ptrs.inl.children[0];
        if (firstChild->isLeaf == true) {  // the child is Leaf
            if (ins > 0) {
                MBPTptr prevChild;
                MBPTptr nextChild;
                prevChild = cur->ptrs.inl.children[ins];
                nextChild = prevChild->ptrs.lf.right;
                prevChild->ptrs.lf.right = n;
                n->ptrs.lf.right = nextChild;
                n->ptrs.lf.left = prevChild;
                if (nextChild != NULL)
                    nextChild->ptrs.lf.left = n;
            } else {  // do not have a prevChild
                MBPTptr nextChild = cur->ptrs.inl.children[2];
                n->ptrs.lf.right = cur->ptrs.inl.children[2];
                n->ptrs.lf.left = cur->ptrs.inl.children[0];
                firstChild->ptrs.lf.right = n;
                if (nextChild != NULL)
                    nextChild->ptrs.lf.left = n;
            }
        }
    }
    if (cur->numKeys == MAX_CHILD)
        split(cur, seat_id);  // key is full
}

void init_BPTree(uint32_t seat_id)
{
    NumOfNodes[seat_id] = 0;
    height[seat_id] = 1;
    root[seat_id] = newBPTreeNode(seat_id);
    root[seat_id]->numKeys = 0;
    root[seat_id]->isRoot = true;
    root[seat_id]->isLeaf = true;
    root[seat_id]->ptrs.lf.right = NULL;
    root[seat_id]->ptrs.lf.left = NULL;
    root[seat_id]->ptrs.lf.value[0] = 0;
}

int BPTreeInsert(key_int64_t key, value_ptr_t value, uint32_t seat_id)
{
    if (root[seat_id]->numKeys == 0) {  // if the tree is empty
        root[seat_id]->key[0] = key;
        root[seat_id]->numKeys++;
        root[seat_id]->ptrs.lf.value[0] = value;
        return true;
    }
    MBPTptr Leaf = findLeaf(key, seat_id);
    // int i = findKeyPos(Leaf, key);
    // printf("key:%ld,pos:%d\n",key,i);
    insert(Leaf, key, value, NULL, seat_id);
    // printf("inserted {key %d, value '%s'}.\n",key,(char*)value);
    return true;
}

value_ptr_t BPTreeGet(key_int64_t key, uint32_t seat_id)
{
    MBPTptr Leaf = findLeaf(key, seat_id);
    int i;
    for (i = 0; i < Leaf->numKeys; i++) {
        if (Leaf->key[i] == key) {
#ifdef DEBUG_ON
            // printf("[key = %ld: found]", Leaf->key[i]);
#endif
            return Leaf->ptrs.lf.value[i];
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
    if (cur->isLeaf == true) {
        cur->isRoot ? printf("this is a Root LeafNode (addr %p)\n", cur)
                         : printf("this is a LeafNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->parent);
        printf("1. number of keys: %d\n", cur->numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%lu ", cur->key[i]);
        }
        printf("]\n");
        printf("3. value pointers:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%ld ", cur->ptrs.lf.value[i]);
        }
        printf("]\n");
        printf("4. leaf connections, left: %p right: %p\n", cur->ptrs.lf.left,
            cur->ptrs.lf.right);
    } else {
        cur->isRoot ? printf("this is a Root InternalNode (addr %p)\n", cur)
                         : printf("this is an InternalNode (addr %p)\n", cur);
        printf("0. parent: %p\n", cur->parent);
        printf("1. number of keys: %d\n", cur->numKeys);
        printf("2. keys:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%lu ", cur->key[i]);
        }
        printf("]\n");
        printf("3. children:[ ");
        for (int i = 0; i <= cur->numKeys; i++) {
            printf("%p ", cur->ptrs.inl.children[i]);
        }
        printf("]\n");
    }
    printf("\n");
}

void BPTreePrintLeaves(uint32_t seat_id)
{
    MBPTptr Leaf = findLeaf(0, seat_id);
    int cnt = 0;
    while (Leaf != NULL) {
        showNode(Leaf, cnt);
        Leaf = Leaf->ptrs.lf.right;
        cnt++;
    }
    printf("\n");
}

void BPTreePrintRoot(uint32_t seat_id)
{
    printf("rootNode\n");
    showNode(root[seat_id], 0);
}

void BPTreePrintAll(uint32_t seat_id)
{  // show all node (BFS)
    int nodeNo = 0;
    initQueue(queue, seat_id);
    enqueue(queue, root[seat_id], seat_id);
    while ((queue[seat_id]->tail + 1) % MAX_NODE_NUM != queue[seat_id]->head) {
        MBPTptr cur = dequeue(queue, seat_id);
        showNode(cur, nodeNo);
        nodeNo++;
        if (!cur->isLeaf) {
            for (int i = 0; i <= cur->numKeys; i++) {
                enqueue(queue, cur->ptrs.inl.children[i], seat_id);
            }
        }
    }
}

#endif
int BPTree_GetNumOfNodes(uint32_t seat_id)
{
    return NumOfNodes[seat_id];
}

int BPTree_GetHeight(uint32_t seat_id) { return height[seat_id]; }

int BPTree_Serialize(uint32_t seat_id, KVPairPtr dest){
    int n = 0;
    MBPTptr leaf = findLeaf(KEY_MIN, seat_id);
    while(leaf != NULL){
        for(int i = 0; i < leaf->numKeys; i++){
            dest[n].key = leaf->key[i];
            dest[n].value = leaf->ptrs.lf.value[i];
            n++;
        }
        leaf = leaf->ptrs.lf.right;
    }
    return n;
}

void BPTree_Deserialize(uint32_t seat_id, KVPairPtr src, int start_index, int n){
    for(int i = start_index; i < start_index + n; i++){
        BPTreeInsert(src[i].key,src[i].value,seat_id);
    }
}
