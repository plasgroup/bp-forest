#pragma once

#include "common.h"

#include <stdint.h>


_Static_assert(MAX_NUM_NODES_IN_DPU <= UINT32_MAX, "MAX_NUM_NODES_IN_DPU <= UINT32_MAX");
#define NODE_PTR_WIDTH (BITWIDTH_UINT32(MAX_NUM_NODES_IN_DPU))

typedef enum : uint32_t { NODE_NULLPTR = UINT32_MAX } NodePtr;
#define Deref(NODE_PTR) (nodes_storage[NODE_PTR])
