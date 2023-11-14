#pragma once

#include "mram.h"
#include "common.h"

struct BPTreeNode;
typedef struct BPTreeNode Node;

extern void Cabin_init();
extern seat_id_t Cabin_allocate_seat(seat_id_t seat_id);
extern void Cabin_release_seat(seat_id_t seat_id);
extern __mram_ptr Node* Seat_get_root(seat_id_t seat_id);
extern void Seat_set_root(seat_id_t seat_id, __mram_ptr Node* new_root);
extern __mram_ptr Node* Seat_allocate_node(seat_id_t seat_id);
extern void Seat_free_node(seat_id_t seat_id, __mram_ptr Node* node);
extern void Seat_inc_height(seat_id_t seat_id);
extern int Seat_get_height(seat_id_t seat_id);
extern int Seat_get_n_nodes(seat_id_t seat_id);
extern int Seat_is_used(seat_id_t seat_id);
extern __mram_ptr Node* Seat_get_node_by_id(seat_id_t seat_id, int id);
