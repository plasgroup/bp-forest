#include "bplustree.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

// #define USE_LINEAR_SEARCH

#ifdef DEBUG_ON
typedef struct Queue {  // queue for showing all nodes by BFS
    int tail;
    int head;
    BPTptr ptrs[MAX_NODE_NUM];
} Queue_t;

Queue_t* queue;
Queue_t* initQueue()
{
    Queue_t* q = (Queue_t*)malloc(sizeof(Queue_t));
    q->head = 0;
    q->tail = -1;
    return q;
    // printf("queue is initialized\n");
}

void enqueue(Queue_t* queue, BPTptr input)
{
    if ((queue->tail + 2) % MAX_NODE_NUM == queue->head) {
        printf("queue is full\n");
        return;
    }
    queue->ptrs[(queue->tail + 1) % MAX_NODE_NUM] = input;
    queue->tail = (queue->tail + 1) % MAX_NODE_NUM;
    // printf("%p is enqueued\n",input);
}

BPTptr dequeue(Queue_t* queue)
{
    BPTptr ret;
    if ((queue->tail + 1) % MAX_NODE_NUM == queue->head) {
        printf("queue is empty\n");
        return NULL;
    }
    ret = queue->ptrs[queue->head];
    queue->head = (queue->head + 1) % MAX_NODE_NUM;
    // printf("%p is dequeued\n",ret);
    return ret;
}

void showNode(BPTptr, int);
#endif

BPTptr newBPTreeNode(BPlusTree* bpt)
{
    BPTptr p = (BPTptr)malloc(sizeof(BPTreeNode));
    p->parent = NULL;
    p->isRoot = false;
    p->isLeaf = false;
    p->numKeys = 0;
    bpt->NumOfNodes++;
    return p;
}

// binary search
#ifndef USE_LINEAR_SEARCH
int findKeyPos(BPTptr n, key_t_ key)
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
int findKeyPos(BPTptr n, key_t_ key)
{
    int ret = 0;
    for (int ret = 0; ret < n->numKeys; ret++) {
        if (n->key[ret] <= key)
            return ret;
    }
    return ret;
}
#endif
BPTptr findLeaf(BPlusTree* bpt, key_t_ key)
{
    BPTptr n = bpt->root;
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
void insert(BPlusTree* bpt, BPTptr cur, key_t_, value_ptr_t_, BPTptr);
void split(BPlusTree* bpt, BPTptr cur)
{
    // cur splits into cur and n
    // copy cur[Mid+1 .. MAX_CHILD] to n[0 .. n->key_num-1]
    BPTptr n = newBPTreeNode(bpt);
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
    if (cur->isRoot) {  // bpt->root Node splits
        // Create a new bpt->root
        bpt->root = newBPTreeNode(bpt);
        bpt->root->isRoot = true;
        bpt->root->isLeaf = false;
        bpt->root->numKeys = 1;
        bpt->root->ptrs.inl.children[0] = cur;
        bpt->root->ptrs.inl.children[1] = n;
        cur->parent = n->parent = bpt->root;
        cur->isRoot = false;
        if (cur->isLeaf) {
            cur->ptrs.lf.right = n;
            n->ptrs.lf.left = cur;
            bpt->root->key[0] = n->key[0];
        } else {
            bpt->root->key[0] = cur->key[Mid - 1];
        }
        bpt->height++;
    } else {  // insert n to cur->parent
        n->parent = cur->parent;
        if (cur->isLeaf) {
            insert(bpt, n->parent, n->key[0], (value_ptr_t_)NULL, n);
        } else {
            insert(bpt, cur->parent, cur->key[Mid - 1], (value_ptr_t_)NULL, n);
        }
    }
}

void insert(BPlusTree* bpt, BPTptr cur, key_t_ key, value_ptr_t_ value,
    BPTptr n)
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
        BPTptr firstChild = cur->ptrs.inl.children[0];
        if (firstChild->isLeaf == true) {  // the child is Leaf
            if (ins > 0) {
                BPTptr prevChild;
                BPTptr nextChild;
                prevChild = cur->ptrs.inl.children[ins];
                nextChild = prevChild->ptrs.lf.right;
                prevChild->ptrs.lf.right = n;
                n->ptrs.lf.right = nextChild;
                n->ptrs.lf.left = prevChild;
                if (nextChild != NULL)
                    nextChild->ptrs.lf.left = n;
            } else {  // do not have a prevChild
                BPTptr nextChild = cur->ptrs.inl.children[2];
                n->ptrs.lf.right = cur->ptrs.inl.children[2];
                n->ptrs.lf.left = cur->ptrs.inl.children[0];
                firstChild->ptrs.lf.right = n;
                if (nextChild != NULL)
                    nextChild->ptrs.lf.left = n;
            }
        }
    }
    if (cur->numKeys == MAX_CHILD)
        split(bpt, cur);  // key is full
}

BPlusTree* init_BPTree()
{
    //printf("taskletinit_BPTree\n");
    BPlusTree* bpt = (BPlusTree*)malloc(sizeof(BPlusTree));
    bpt->root = (BPTptr)malloc(sizeof(BPTreeNode));
    bpt->NumOfNodes = 1;
    bpt->height = 1;
    bpt->root = newBPTreeNode(bpt);
    bpt->root->isRoot = true;
    bpt->root->isLeaf = true;
    bpt->root->ptrs.lf.right = NULL;
    bpt->root->ptrs.lf.left = NULL;
    bpt->root->ptrs.lf.value[0] = (value_ptr_t_)NULL;
    return bpt;
}

int BPTreeInsert(BPlusTree* bpt, key_t_ key, value_ptr_t_ value)
{
    if (bpt->root->numKeys == 0) {  // if the tree is empty
        bpt->root->key[0] = key;
        bpt->root->numKeys++;
        bpt->root->ptrs.lf.value[0] = value;
        return true;
    }
    BPTptr Leaf = findLeaf(bpt, key);
    // int i = findKeyPos(Leaf, key);
    // printf("key:%ld,pos:%d\n",key,i);
    insert(bpt, Leaf, key, value, NULL);
    // printf("inserted {key %d, value '%s'}.\n",key,(char*)value);
    return true;
}

value_ptr_t_ BPTreeGet(BPlusTree* bpt, key_t_ key)
{
    BPTptr Leaf = findLeaf(bpt, key);
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
    return (value_ptr_t_)NULL;
}
#ifdef DEBUG_ON
void showNode(BPTptr cur, int nodeNo)
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
        printf("3. values:[ ");
        for (int i = 0; i < cur->numKeys; i++) {
            printf("%s ", (char*)cur->ptrs.lf.value[i]);
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

void BPTreePrintLeaves(BPlusTree* bpt)
{
    BPTptr Leaf = findLeaf(bpt, 0);
    int cnt = 0;
    while (Leaf != NULL) {
        showNode(Leaf, cnt);
        Leaf = Leaf->ptrs.lf.right;
        cnt++;
    }
    printf("\n");
}

void BPTreePrintRoot(BPlusTree* bpt)
{
    printf("rootNode\n");
    showNode(bpt->root, 0);
}

void BPTreePrintAll(BPlusTree* bpt)
{  // show all node (BFS)
    int nodeNo = 0;
    queue = initQueue();
    enqueue(queue, bpt->root);
    while ((queue->tail + 1) % MAX_NODE_NUM != queue->head) {
        BPTptr cur = dequeue(queue);
        showNode(cur, nodeNo);
        nodeNo++;
        if (!cur->isLeaf) {
            for (int i = 0; i <= cur->numKeys; i++) {
                enqueue(queue, cur->ptrs.inl.children[i]);
            }
        }
    }
}

#endif
int BPTree_GetNumOfNodes(BPlusTree* bpt)
{
    return bpt->NumOfNodes;
}

int BPTree_GetHeight(BPlusTree* bpt) { return bpt->height; }
