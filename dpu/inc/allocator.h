#pragma once

#include "bitmap.h"
#include "dpu_params.h"
#include "node_ptr.h"
#include "tree_impl.h"

#include <attributes.h>


extern __mram_ptr Node nodes_storage[MAX_NR_NODES];

//! @brief Make the first n nodes allocated and the rest available
//! @pre workspace is available
static void Allocator_init(const unsigned n);

static NodePtr Allocate_node();
static void Free_node(NodePtr node);


#define TASK_INIT_ALLOC_NR_TASKLETS TASK_INIT_BITMAP_NR_TASKLETS

#include "allocator.i"  // implementation
