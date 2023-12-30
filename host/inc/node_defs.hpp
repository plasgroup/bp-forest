#ifndef __NODE_DEFS_HPP__
#define __NODE_DEFS_HPP__

#include <stdint.h>
typedef uint64_t key_int64_t;
typedef uint64_t value_ptr_t;
typedef struct MBPTptr {
    uint64_t x : 48;
} __attribute__((packed)) MBPTptr;

#define MAX_CHILD 12
extern MBPTptr root;

typedef struct InternalNodePtrs {
    MBPTptr children[MAX_CHILD + 1];

} InternalNodePtrs;
typedef struct LeafNodePtrs {
    value_ptr_t value[MAX_CHILD];
    MBPTptr right;
    MBPTptr left;
} LeafNodePtrs;

typedef struct BPTreeNode {
    int isRoot : 8;
    int isLeaf : 8;
    int numKeys : 16;
    key_int64_t key[MAX_CHILD];
    MBPTptr parent;
    union {
        InternalNodePtrs inl;
        LeafNodePtrs lf;
    } ptrs;
} BPTreeNode;

#endif /* __NODE_DEFS_HPP__ */