#include "cabin.h"

#include "bitmap.h"
#include "bplustree.h"
#include "common.h"

#include <attributes.h>

#include <assert.h>
#include <stdint.h>


#define INIT_ROOT_NODE_INDEX 0

struct Seat {
    int in_use;
    __mram_ptr Node* storage;
    int root_index;
    bitmap_word_t bitmap[(MAX_NUM_NODES_IN_SEAT + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD];
    int next_alloc;
    int height;
    int n_nodes;
};


/* WRAM */
static struct Seat cabin[NR_SEATS_IN_DPU];

/* MRAM */
static __mram Node cabin_storage[MAX_NUM_NODES_IN_SEAT * NR_SEATS_IN_DPU];

static void Seat_init(seat_id_t seat_id);


/*** Cabin ***/

void Cabin_init()
{
    seat_id_t i;
    for (i = 0; i < NR_SEATS_IN_DPU; i++) {
        cabin[i].in_use = 0;
        cabin[i].storage = &cabin_storage[MAX_NUM_NODES_IN_SEAT * i]; /* TODO: align */
    }
}

seat_id_t Cabin_allocate_seat(seat_id_t seat_id)
{
    if (seat_id == INVALID_SEAT_ID) {
        seat_id_t i;
        for (i = 0; i < NR_SEATS_IN_DPU; i++)
            if (cabin[i].in_use == 0) {
                Seat_init(i);
                return i;
            }
    } else {
        if (cabin[seat_id].in_use == 0) {
            Seat_init(seat_id);
            return seat_id;
        }
    }
    return INVALID_SEAT_ID;
}

void Cabin_release_seat(seat_id_t seat_id)
{
    extern int num_kvpairs_in_seat[NR_SEATS_IN_DPU];
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    cabin[seat_id].in_use = 0;
    num_kvpairs_in_seat[seat_id] = 0;
}

int Cabin_get_nr_available_seats()
{
    int n = 0;
    for (int i = 0; i < NR_SEATS_IN_DPU; i++)
        if (!cabin[i].in_use)
            n++;
    return n;
}

/*** Seat ***/

static void Seat_init(seat_id_t seat_id)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(!cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    seat->in_use = 1;
    seat->root_index = INIT_ROOT_NODE_INDEX;
    memset(seat->bitmap, 0, sizeof(seat->bitmap));
    bitmap_set(seat->bitmap, INIT_ROOT_NODE_INDEX);
    seat->next_alloc = 1;
    seat->height = 1;
    seat->n_nodes = 1;
}

__mram_ptr Node* Seat_get_root(seat_id_t seat_id)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    return &seat->storage[seat->root_index];
}

void Seat_set_root(seat_id_t seat_id, __mram_ptr Node* new_root)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    seat->root_index = new_root - seat->storage;
}

void Seat_inc_height(seat_id_t seat_id)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    seat->height++;
}

int Seat_get_height(seat_id_t seat_id)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    return seat->height;
}

int Seat_get_n_nodes(seat_id_t seat_id)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    return seat->n_nodes;
}

__mram_ptr Node* Seat_allocate_node(seat_id_t seat_id)
{
    int id;
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    id = bitmap_find_and_set_first_zero(seat->bitmap, seat->next_alloc,
        MAX_NUM_NODES_IN_SEAT);
    if (id < 0)
        assert(0);

    seat->next_alloc = id + 1;
    seat->n_nodes++;
    return &seat->storage[id];
}

void Seat_free_node(seat_id_t seat_id, __mram_ptr Node* node)
{
    int id;
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    id = node - seat->storage;
    bitmap_clear(seat->bitmap, id);
    seat->next_alloc = id;
    seat->n_nodes--;
}

__mram_ptr Node* Seat_get_node_by_id(seat_id_t seat_id, int id)
{
    struct Seat* seat;
    assert(0 <= seat_id && seat_id < NR_SEATS_IN_DPU);
    assert(cabin[seat_id].in_use);
    seat = &cabin[seat_id];

    return &seat->storage[id];
}

bool Seat_is_used(seat_id_t seat_id)
{
    return cabin[seat_id].in_use;
}
