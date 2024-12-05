#pragma once

#include "allocator.h"

#include "bitmap.h"
#include "node_ptr.h"
#include "sync.h"
#include "tree_impl.h"

#include <attributes.h>

#include <assert.h>
#include <stdint.h>


static DEFINE_BITMAP(allocated_bitmap, MAX_NR_NODES);
static NodePtr next_alloc;

__mram_noinit Node nodes_storage[MAX_NR_NODES];


static void Allocator_init(const unsigned n)
{
    bitmap_init(allocated_bitmap, n);
    next_alloc = n;
}

inline NodePtr Allocate_node()
{
    int id;

    acquire_lock();
    {
        id = bitmap_find_and_set_first_zero(allocated_bitmap, next_alloc, MAX_NR_NODES);
        next_alloc = (NodePtr)id + 1;
    }
    release_lock();

    assert(id >= 0);
    return (NodePtr)id;
}

inline void Free_node(NodePtr node)
{
    bitmap_clear(allocated_bitmap, node);
    next_alloc = node;
}
