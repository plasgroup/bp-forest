#pragma once

#include "workload_types.h"
#include "node_ptr.h"

#include <attributes.h>


enum RBColor {
    Black,
    Red
};
typedef struct __attribute__((packed)) {
    NodePtr left : NODE_PTR_WIDTH;
    NodePtr right : NODE_PTR_WIDTH;
    NodePtr parent : NODE_PTR_WIDTH;
    enum RBColor color : 1;
} NodeHeader;

typedef struct {
    NodeHeader header;
    key_uint64_t key;
    value_uint64_t value;
} Node;
_Static_assert(sizeof(Node) == SIZEOF_NODE, "sizeof(Node) == SIZEOF_NODE");

typedef struct {
    NodeHeader header;
    key_uint64_t key;
} NodeHeaderAndKey;
