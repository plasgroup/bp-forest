#include "allocator.h"

#include "bitmap.h"
#include "bplustree.h"
#include "common.h"

#include <attributes.h>

#include <assert.h>
#include <stdint.h>


/* WRAM */
static bitmap_word_t allocated_bitmap[(MAX_NUM_NODES_IN_SEAT + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD];
static int next_alloc;

/* MRAM */
__mram_noinit Node nodes_storage[MAX_NUM_NODES_IN_SEAT];


MBPTptr Allocator_reset()
{
    memset(allocated_bitmap, 0, sizeof(allocated_bitmap));
    bitmap_set(allocated_bitmap, 0);
    next_alloc = 1;
    return &nodes_storage[0];
}


MBPTptr Allocate_node()
{
    int id = bitmap_find_and_set_first_zero(allocated_bitmap, next_alloc, MAX_NUM_NODES_IN_SEAT);
    assert(id >= 0);

    next_alloc = id + 1;
    return &nodes_storage[id];
}

void Free_node(MBPTptr node)
{
    int32_t id = node - &nodes_storage[0];
    bitmap_clear(allocated_bitmap, id);
    next_alloc = id;
}
