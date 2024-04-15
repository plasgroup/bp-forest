#pragma once

#include "bplustree.h"

#include <attributes.h>


extern __mram_ptr Node nodes_storage[MAX_NUM_NODES_IN_SEAT];
#define NODE_NULLPTR (&nodes_storage[MAX_NUM_NODES_IN_SEAT])

/**
 * @return ptr to root node
 */
MBPTptr Allocator_reset();

MBPTptr Allocate_node();
void Free_node(MBPTptr node);
