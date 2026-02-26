#pragma once

#include "allocator.h"

#include "bitmap.h"
#include "node_ptr.h"
#include "sync.h"
#include "tree_impl.h"

#include <attributes.h>

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>


static DEFINE_BITMAP(allocated_bitmap, MAX_NR_NODES);
static unsigned next_alloc;

__mram_noinit Node nodes_storage[MAX_NR_NODES];


static void Allocator_init(const unsigned n)
{
    bitmap_init(allocated_bitmap, MAX_NR_NODES, n);
    if (me() == 0) {
        next_alloc = n;
    }
}

inline NodePtr Allocate_node()
{
    int id;

    acquire_lock();
    {
        id = bitmap_find_and_set_first_zero(allocated_bitmap, next_alloc, MAX_NR_NODES);
        next_alloc = (unsigned)(id + 1);
    }
    release_lock();

    if (!(id >= 0)) {
        printf("mem exhausted\n");
        abort();
    }
    return (NodePtr)id;
}

inline void Free_node(NodePtr node)
{
    acquire_lock();
    {
        bitmap_clear(allocated_bitmap, (unsigned)node);
        next_alloc = (unsigned)node;
    }
    release_lock();
}

__attribute__((unused)) static uint32_t Allocator_nr_allocd()
{
    return bitmap_count_ones(allocated_bitmap, MAX_NR_NODES);
}
