#pragma once

#include "common.h"

#include <stdint.h>


#define NODE_PTR_WIDTH (BITWIDTH_UINT32(MAX_NUM_NODES_IN_DPU))

typedef enum :
#if NODE_PTR_WIDTH <= 8
uint8_t
#elif NODE_PTR_WIDTH <= 16
uint16_t
#elif NODE_PTR_WIDTH <= 32
uint32_t
#else
uint64_t
#endif
{ NODE_NULLPTR = ((1ul << NODE_PTR_WIDTH) - 1u) } NodePtr;
#define Deref(NODE_PTR) (nodes_storage[NODE_PTR])
