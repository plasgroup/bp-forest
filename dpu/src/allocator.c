#include "allocator.h"

#include "bitmap.h"
#include "common.h"
#include "tree_impl.h"

#include <attributes.h>

#include <assert.h>
#include <stdint.h>


static DEFINE_BITMAP(allocated_bitmap, MAX_NUM_NODES_IN_DPU);
static NodePtr next_alloc;

/* MRAM */
__mram_noinit Node nodes_storage[MAX_NUM_NODES_IN_DPU];


void Allocator_reset()
{
    bitmap_clear_all(allocated_bitmap, MAX_NUM_NODES_IN_DPU);
    next_alloc = 0;
}


NodePtr Allocate_node()
{
    int id = bitmap_find_and_set_first_zero(allocated_bitmap, next_alloc, MAX_NUM_NODES_IN_DPU);
    assert(id >= 0);

    next_alloc = (NodePtr)id + 1;
    return (NodePtr)id;
}

void Free_node(NodePtr node)
{
    bitmap_clear(allocated_bitmap, node);
    next_alloc = node;
}
