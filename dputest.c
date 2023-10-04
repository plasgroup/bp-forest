#include <stdio.h>
#include "mram.h"
#include "./dpu/inc/bplustree.h"

#define true 1
#define false 0
// 48 MB for Node data
#define MAX_NODE_NUM (50331648 / sizeof(BPTreeNode))

__mram BPTreeNode nodes[MAX_NODE_NUM];
int free_node_index_stack_head = -1;
__mram int free_node_index_stack[MAX_NODE_NUM];

int max_node_index = -1;

MBPTptr create_node() {
    MBPTptr p;
    // if there is gap in nodes array
    if (free_node_index_stack_head >= 0) {
        p = &nodes[free_node_index_stack[free_node_index_stack_head--]];
    } else p = &nodes[++max_node_index];

    p->isRoot = false;
    p->isLeaf = false;
    p->numKeys = 0;
    p->children[0] = NULL;
    p->parent = NULL;
    p->right = NULL;
    p->left = NULL;
    max_node_index++;
    return p;
}

void delete_node(MBPTptr node) {
    int index = ((char *)node - (char *)nodes) / sizeof(BPTreeNode);
    free_node_index_stack[++free_node_index_stack_head] = index;
}

int main() {
    

    MBPTptr a1 = create_node();
    MBPTptr a2 = create_node();

    a1->isRoot = true;
    a1->numKeys = 11;
    a2->isRoot = false;
    a2->numKeys = 12;
    printf("sizeof mram = 0x%x\n", 64*1024*1024);
    printf("sizeof a node = %d bytes\n", sizeof(BPTreeNode));
    printf("num of max nodes = %d\n", MAX_NODE_NUM);
    printf("sizeof nodes = 0x%x\n", sizeof(nodes));
    printf("a1 MRAM addr %p, with isRoot = %d, numKeys = %d\n", a1, a1->isRoot, a1->numKeys);
    printf("a2 MRAM addr %p, with isRoot = %d, numKeys = %d\n", a2, a2->isRoot, a2->numKeys);
    return 0;
}