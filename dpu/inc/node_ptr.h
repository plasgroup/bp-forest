#pragma once

#include "bit_ops_macro.h"
#include "dpu_params.h"

#include <limits.h>
#include <stdint.h>


#define MAX_NR_NODES (MRAM_FOR_TREE / SIZEOF_NODE)
_Static_assert(MAX_NR_NODES <= UINT32_MAX, "MAX_NR_NODES <= UINT32_MAX");

#define NODE_PTR_WIDTH (BITWIDTH_UINT32(MAX_NR_NODES))

typedef enum : uint32_t { NODE_NULLPTR = (UINT32_MAX >> (32 - NODE_PTR_WIDTH)) } NodePtr;
#define Deref(NODE_PTR) (nodes_storage[NODE_PTR])
// nodes_storage is in allocator.h
