#pragma once

#include "common.h"
#include "node_ptr.h"
#include "tree_impl.h"

#include <attributes.h>


extern __mram_ptr Node nodes_storage[MAX_NUM_NODES_IN_DPU];

void Allocator_reset();

NodePtr Allocate_node();
void Free_node(NodePtr node);
