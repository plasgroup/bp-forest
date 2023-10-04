#ifndef __bplustree_H__
#define __bplustree_H__

#define MAX_CHILD 12
#include <stdint.h>
typedef struct BPTreeNode {
	int isRoot;
    int isLeaf;
	int numKeys;
	uint32_t key[MAX_CHILD];
	void* children[MAX_CHILD];
	struct BPTreeNode* parent;
	struct BPTreeNode* right;
	struct BPTreeNode* left;
} BPTreeNode;

extern void init_BPTree();

/**
*    @param key key to insert
*    @param pos pos
*    @param value value to insert
**/
extern int BPTreeInsert(uint32_t, void*);
/**
*    @param key key to search
**/
extern void* BPTreeGet(uint32_t);
extern void BPTreeGetRange(int, int);
extern void BPTreeDelete(int);
extern int BPTree_GetNumOfNodes();
extern void BPTreePrintLeaves();
extern void BPTreePrintRoot();

#endif