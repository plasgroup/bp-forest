#include "bplustree.h"
#include "common.h"
#include <stdbool.h>
#include <stdio.h>

// #define USE_LINEAR_SEARCH

// nodes(keys and pointers)
__mram BPTreeNode nodes[NR_TASKLETS][MAX_NODE_NUM];

int free_node_index_stack_head[NR_TASKLETS] = {-1};
__mram int free_node_index_stack[NR_TASKLETS][MAX_NODE_NUM];

int height[NR_TASKLETS] = {1};

int max_node_index[NR_TASKLETS] = {-1};

MBPTptr root[NR_TASKLETS];

int NumOfNodes[NR_TASKLETS] = {0};

#ifdef DEBUG_ON
typedef struct Queue {  // queue for showing all nodes by BFS
    int tail;
    int head;
    MBPTptr ptrs[MAX_NODE_NUM];
} Queue_t;

__mram_ptr Queue_t* queue[NR_TASKLETS];

void initQueue(__mram_ptr Queue_t** queue, uint32_t tasklet_id)
{
    queue[tasklet_id]->head = 0;
    queue[tasklet_id]->tail = -1;
    // printf("queue is initialized\n");
}

void enqueue(__mram_ptr Queue_t** queue, MBPTptr input, uint32_t tasklet_id)
{
    if ((queue[tasklet_id]->tail + 2) % MAX_NODE_NUM == queue[tasklet_id]->head) {
        printf("queue is full\n");
        return;
    }
    queue[tasklet_id]->ptrs[(queue[tasklet_id]->tail + 1) % MAX_NODE_NUM] = input;
    queue[tasklet_id]->tail = (queue[tasklet_id]->tail + 1) % MAX_NODE_NUM;
    // printf("%p is enqueued\n",input);
}

MBPTptr dequeue(__mram_ptr Queue_t** queue, uint32_t tasklet_id)
{
    MBPTptr ret;
    if ((queue[tasklet_id]->tail + 1) % MAX_NODE_NUM == queue[tasklet_id]->head) {
        printf("queue is empty\n");
        return NULL;
    }
    ret = queue[tasklet_id]->ptrs[queue[tasklet_id]->head];
    queue[tasklet_id]->head = (queue[tasklet_id]->head + 1) % MAX_NODE_NUM;
    // printf("%p is dequeued\n",ret);
    return ret;
}

void showNode(MBPTptr, int);
#endif

MBPTptr newBPTreeNode(uint32_t tasklet_id)
{
    MBPTptr p;
    if (free_node_index_stack_head[tasklet_id] >= 0) {  // if there is gap in nodes array
        p = &nodes[tasklet_id]
                  [free_node_index_stack[tasklet_id]
                                        [free_node_index_stack_head[tasklet_id]--]];
    } else
        p = &nodes[tasklet_id][++max_node_index[tasklet_id]];
    p->parent = NULL;
    p->isRoot = false;
    p->isLeaf = false;
    p->numKeys = 0;
    NumOfNodes[tasklet_id]++;
    return p;
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
MBPTptr findLeaf(key_int64_t key, uint32_t tasklet_id)
{
    MBPTptr n = root[tasklet_id];
    while (true) {
        if (n->isLeaf == true)
            break;
        if (key < n->key[0]) {
            n = n->ptrs.inl.children[0];
        } else {
            int i = findKeyPos(n, key);
#ifdef DEBUG_ON
            // printf("findLeaf:Node = %p, key = %d, i = %d\n",n, key,i);
#endif
            n = n->ptrs.inl.children[i];
        }
    }
    return n;
}
void insert(MBPTptr cur, key_int64_t, value_ptr_t, MBPTptr, uint32_t);
void split(MBPTptr cur, uint32_t tasklet_id)
{
    // cur splits into cur and n
    // copy cur[Mid+1 .. MAX_CHILD] to n[0 .. n->key_num-1]
    MBPTptr n = newBPTreeNode(tasklet_id);
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
        root[tasklet_id] = newBPTreeNode(tasklet_id);
        root[tasklet_id]->isRoot = true;
        root[tasklet_id]->isLeaf = false;
        root[tasklet_id]->numKeys = 1;
        root[tasklet_id]->ptrs.inl.children[0] = cur;
        root[tasklet_id]->ptrs.inl.children[1] = n;
        cur->parent = n->parent = root[tasklet_id];
        cur->isRoot = false;
        if (cur->isLeaf) {
            cur->ptrs.lf.right = n;
            n->ptrs.lf.left = cur;
            root[tasklet_id]->key[0] = n->key[0];
        } else {
            root[tasklet_id]->key[0] = cur->key[Mid - 1];
        }
        height[tasklet_id]++;
    } else {  // insert n to cur->parent
        n->parent = cur->parent;
        if (cur->isLeaf) {
            insert(n->parent, n->key[0], 0, n, tasklet_id);
        } else {
            insert(cur->parent, cur->key[Mid - 1], 0, n, tasklet_id);
        }
    }
}

void insert(MBPTptr cur, key_int64_t key, value_ptr_t value, MBPTptr n,
    uint32_t tasklet_id)
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
        split(cur, tasklet_id);  // key is full
}

void init_BPTree(uint32_t tasklet_id)
{
    NumOfNodes[tasklet_id] = 0;
    height[tasklet_id] = 1;
    root[tasklet_id] = newBPTreeNode(tasklet_id);
    root[tasklet_id]->numKeys = 0;
    root[tasklet_id]->isRoot = true;
    root[tasklet_id]->isLeaf = true;
    root[tasklet_id]->ptrs.lf.right = NULL;
    root[tasklet_id]->ptrs.lf.left = NULL;
    root[tasklet_id]->ptrs.lf.value[0] = 0;
}

int BPTreeInsert(key_int64_t key, value_ptr_t value, uint32_t tasklet_id)
{
    if (root[tasklet_id]->numKeys == 0) {  // if the tree is empty
        root[tasklet_id]->key[0] = key;
        root[tasklet_id]->numKeys++;
        root[tasklet_id]->ptrs.lf.value[0] = value;
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

void BPTreePrintLeaves(uint32_t tasklet_id)
{
    MBPTptr Leaf = findLeaf(0, tasklet_id);
    int cnt = 0;
    while (Leaf != NULL) {
        showNode(Leaf, cnt);
        Leaf = Leaf->ptrs.lf.right;
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
    while ((queue[tasklet_id]->tail + 1) % MAX_NODE_NUM != queue[tasklet_id]->head) {
        MBPTptr cur = dequeue(queue, tasklet_id);
        showNode(cur, nodeNo);
        nodeNo++;
        if (!cur->isLeaf) {
            for (int i = 0; i <= cur->numKeys; i++) {
                enqueue(queue, cur->ptrs.inl.children[i], tasklet_id);
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