#pragma once

#include "common.h"
#include "node_ptr.h"
#include "workload_types.h"

#include <attributes.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>


typedef struct {
    unsigned numKeys : 8;
    bool isLeaf : 1;
    NodePtr parent;
} NodeHeader;

typedef struct {
#ifdef CACHE_CHILD_HEADER_IN_LINK
    unsigned numKeys : 8;
    bool isLeaf : 1;
#endif
    NodePtr ptr;
} ChildInfo;

typedef struct {
    key_int64_t keys[MAX_NR_CHILDREN - 1];
    ChildInfo children[MAX_NR_CHILDREN];
} InternalNodeBody;
typedef struct {
    key_int64_t keys[MAX_NR_PAIRS];
    value_ptr_t values[MAX_NR_PAIRS];
    NodePtr right;
    NodePtr left;
} LeafNodeBody;

typedef union {
    struct {
        NodeHeader header;
        union {
            InternalNodeBody inl;  // also update MAX_NR_CHILDREN in common/inc/common.h
            LeafNodeBody lf;       // also update MAX_NR_PAIRS in common/inc/common.h
        } body;
    };
    char size_adjuster[SIZEOF_NODE];
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");

typedef struct {
    NodeHeader header;
    union {
        struct {
            key_int64_t keys[MAX_NR_CHILDREN - 1];
        } inl;
        struct {
            key_int64_t keys[MAX_NR_PAIRS];
        } lf;
    } body;
} NodeHeaderAndKeys;
