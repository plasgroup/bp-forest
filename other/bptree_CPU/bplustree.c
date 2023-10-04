#include "bplustree.h"
#include "../../common/common.h"
#include <stdio.h>
#include <stdlib.h>

#define true 1
#define false 0

struct BPTreeNode* root;
char* nullptr = "null";

int NumOfNodes;

BPTreeNode* newBPTreeNode() {
	struct BPTreeNode* p = (struct BPTreeNode*)malloc(sizeof(struct BPTreeNode));
	p->isRoot = false;
	p->isLeaf = false;
	p->numKeys = 0;
	p->children[0] = NULL;
	p->parent = NULL;
	p->right = NULL;
	p->left = NULL;
	NumOfNodes++;
	return p;
}

int findKeyPos(BPTreeNode* n, uint32_t key) {
	int l = 0, r = n->numKeys;
	if (key < n->key[l]) return l;
	if (n->key[r - 1] <= key) return r - 1;
	while (l < r - 1) {
		int mid = (l + r) >> 1;
		if (n->key[mid] > key)
			r = mid;
		else
			l = mid;
	}
	return l;
}

BPTreeNode* findLeaf(uint32_t key) {
	BPTreeNode* n = root;
	while (true) {
		if (n->isLeaf == true)
			break;
		if (key < n->key[0]) {
			n = n->children[0];
		} else {
			int i = findKeyPos(n, key);
			n = n->children[i];
		}
	}
	return n;
}
void insert(BPTreeNode* cur, uint32_t, void*);
void split(BPTreeNode* cur) {
	// copy cur[Mid .. MaxChildNumber] to n[0 .. n->key_num]
	BPTreeNode* n = newBPTreeNode();
	BPTreeNode* ch;
	int Mid = MAX_CHILD >> 1;
	n->isLeaf = cur->isLeaf;
	n->numKeys = MAX_CHILD - Mid;
	int i;
	for (i = Mid; i < MAX_CHILD; i++) {
		n->children[i - Mid] = cur->children[i];
		n->key[i - Mid] = cur->key[i];
		if (!n->isLeaf) {
			ch = (BPTreeNode*)n->children[i - Mid];
			ch->parent = n;
		}
	}
	// Change cur
	cur->numKeys = Mid;
	// Insert n
	if (cur->isRoot) {
		// Create a new root,
		root = newBPTreeNode();
		root->numKeys = 2;
		root->isRoot = true;
		root->key[0] = cur->key[0];
		root->children[0] = cur;
		root->key[1] = n->key[0];
		root->children[1] = n;
		cur->parent = n->parent = root;
		cur->isRoot = false;
		if (cur->isLeaf) {
			cur->right = n;
			n->left = cur;
		}
	} else {
		// insert n to cur->parent
		n->parent = cur->parent;
		insert(cur->parent, cur->key[Mid], (void*)n);
	}
}

void insert(BPTreeNode* cur, uint32_t key, void* value) {
	int i, ins;
	if (key < cur->key[0]) {
        ins = 0;
    } else {
        ins = findKeyPos(cur, key) + 1;
    }
	for (i = cur->numKeys; i > ins; i--) {
		cur->key[i] = cur->key[i - 1];
		cur->children[i] = cur->children[i - 1];
	}
	cur->numKeys++;
	cur->key[ins] = key;
	cur->children[ins] = value;
	if (cur->isLeaf == false) { // make links on leaves
		BPTreeNode* firstChild = (BPTreeNode*)(cur->children[0]);
		if (firstChild->isLeaf == true) {
			BPTreeNode* n = (BPTreeNode*)(value); // value is a leaf node
			if (ins > 0) {
				BPTreeNode* prevChild;
				BPTreeNode* nextChild;
				prevChild = (BPTreeNode*)cur->children[ins - 1];
				nextChild = prevChild->right;
				prevChild->right = n;
				n->right = nextChild;
				n->left = prevChild;
				if (nextChild != NULL) nextChild->left = n;
			} else {
				// do not have a prevChild
				n->right = cur->children[1];
			}
		}
	}
	if (cur->numKeys == MAX_CHILD) // children is full
		split(cur);
}


void init_BPTree() {
    NumOfNodes = 0;
	root = newBPTreeNode();
	root->isRoot = true;
	root->isLeaf = true;
}

int BPTreeInsert(uint32_t key, void* value) {
	BPTreeNode* Leaf = findLeaf(key);
	int i = findKeyPos(Leaf, key);
	if (Leaf->key[i] == key) return false;
	insert(Leaf, key, value);
    //printf("inserted {key %d, value '%s'}.\n",key,(char*)value);
	return true;
}

void* BPTreeGet(uint32_t key) {
	BPTreeNode* Leaf = findLeaf(key);
	int i;
	for (i = 0; i < Leaf->numKeys; i++) {
		if (Leaf->key[i] == key) {
			//printf("found [key = %d, value = %s]\n", Leaf->key[i], (char*)Leaf->children[i]);
            return Leaf->children[i];
		}
	}
	return nullptr;
    //printf("[key = %d: value not found]\n", key);
}

void BPTreePrintLeaves() {
	struct BPTreeNode* Leaf = findLeaf(0);
	int cnt = 0;
	while (Leaf != NULL) {
		int i;
        printf("Leafnode #%d[",cnt);
		for (i = 0; i < Leaf->numKeys; i++) {
			printf("%4d ", Leaf->key[i]);
		}
		Leaf = Leaf->right;
        cnt++;
        printf("]\n");
	}
    printf("\n");
}

void BPTreePrintRoot() {
    printf("RootNode[");
    for(int i = 1; i < root->numKeys; i++) {
        printf("%4d ", root->key[i]);
    }
    printf("]\n");
}

int BPTree_GetNumOfNodes() {
    return NumOfNodes;
}