#include "cabin.h"
#include <assert.h>

/* WRAM */
static struct Seat cabin[NR_SEATS_IN_DPU];

/* MRAM */
static __mram Node cabin_storage[MAX_NODES_IN_SEAT * NR_SEATS_IN_DPU];

/*** Bitmap Operation ***/

static void bitmap_set(char* bitmap, int n)
{
  int index = n >> LOG_BITS_IN_BMPWD;
  int offset = n & (BITS_IN_BMPWD - 1);
  bitmap[index] |= 1 << offset;
}

static int bitmap_find_and_set_first_zero_range(char* bitmap,
						int start, int end)
{
  int end_index = end >> LOG_BITS_IN_BMPWD;
  for (id = start; id < end;) {
    int index = id >> LOG_BITS_IN_BMPWD;
    int byte = bitmap[index];
    int pat;
    if (index == end_index)
      byte |= ((1 << BITS_IN_BMPWD) - 1) << (end & (BITS_IN_BMPWD - 1));
    for (pat = 1 << (id & (BITS_IN_BMPWD - 1)); pat < (1 < BITS_IN_BMPWD);
	 id++, pat <<= 1)
      if ((byte & pat) == 0) {
	bitmap[index] |= pat;
	return id;
      }
  }
  return -1;
}

static int bitmap_find_and_set_first_zero(char* bitmap, int next, int size)
{
  int id;

  id = bitmap_find_and_set_first_zero(bitmap, next, size);
  if (id > 0)
    return id;
  id = bitmap_find_and_set_first_zero(bitmap, 0, next);
  if (id > 0)
    return id;
  return -1;
}

/*** Cabin ***/

void Cabin_init()
{
  seat_id_t i;
  for (i = 0; i < NR_SEATS_IN_DPU; i++) {
    cabin[i].in_use = 0;
    cabin[i].storage = &cabin_storage[MAX_NODES_IN_SEAT * i]; /* TODO: align */
  }
}

seat_id_t Cabin_allocate_seat()
{
  seat_id_t i;
  for (i = 0; i < NR_SEATS_IN_DPU; i++)
    if (cabin[i].in_use == 0) {
      Seat_init(i);
      return i;
    }
  return INVALID_SEAT_ID;
}

void Cabin_release_seat(seat_id_t seat_id)
{
  assert(0 <= seat_id && seat_id <= NR_SEATS_IN_DPU);
  assert(cabin[seat_id].in_use);
  cabin[seat_id].in_use = 0;
}

/*** Seat ***/

void Seat_init(seat_id_t seat_id)
{
  struct Seat* seat;
  assert(0 <= seat_id && seat_id <= NR_SEATS_IN_DPU);
  assert(cabin[seat_id].in_use);
  seat = &cabin[seat_id];
  
  memset(seat->bitmap, 0, sizeof(seat->bitmap));
  bitmap_set(seat->bitmap, 0);
  seat->next_alloc = 1;
}

Node* Seat_get_root(seat_id_t seat_id)
{
  assert(0 <= seat_id && seat_id <= NR_SEATS_IN_DPU);
  assert(cabin[seat_id].in_use);
  return &cabin[seat_id].storage[ROOT_NODE_INDEX];
}

Node* Seat_allocate_node(seat_id_t seat_id)
{
  int id;
  struct Seat* seat;
  assert(0 <= seat_id && seat_id <= NR_SEATS_IN_DPU);
  assert(cabin[seat_id].in_use);
  seat = &cabin[seat_id];

  id = bitmap_find_and_set_first_zero(seat->bitmap, seat->next_alloc,
				      MAX_NODES_IN_SEAT);
  assert(id != ROOT_NODE_INDEX);

  if (id < 0)
    abort();

  seat->next_alloc = id + 1;
  return &seat->storage[id];
}

void Seat_free_node(seat_id_t seat_id, Node* node)
{
  int id;
  struct Seat* seat;
  assert(0 <= seat_id && seat_id <= NR_SEATS_IN_DPU);
  assert(cabin[seat_id].in_use);
  seat = &cabin[seat_id];

  id = node - seat->storage;
  bitmap_clear(seat->bitma, id);
  seat->next_alloc = id;
}
