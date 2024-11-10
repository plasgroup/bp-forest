#pragma once

#include "bit_ops_macro.h"
#include "common.h"
#include "dpu_params.h"

#include <stdint.h>


#define MAX_NUM_NODES_IN_DPU (MRAM_FOR_TREE / SIZEOF_NODE)
_Static_assert(MAX_NUM_NODES_IN_DPU <= UINT32_MAX, "MAX_NUM_NODES_IN_DPU <= UINT32_MAX");

#define NODE_PTR_WIDTH (BITWIDTH_UINT32(MAX_NUM_NODES_IN_DPU))

typedef enum : uint32_t { NODE_NULLPTR = (UINT32_MAX >> (32 - NODE_PTR_WIDTH)) } NodePtr;
#define Deref(NODE_PTR) (nodes_storage[NODE_PTR])
