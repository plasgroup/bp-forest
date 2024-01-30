#pragma once

#include "common.h"

//--- UPMEM ---//
#include <attributes.h>

#include <stdbool.h>

struct BPTreeNode;
typedef struct BPTreeNode Node;


void Cabin_init();

seat_id_t Cabin_allocate_seat(seat_id_t seat_id);
void Cabin_release_seat(seat_id_t seat_id);
int Cabin_get_nr_available_seats();

bool Seat_is_used(seat_id_t seat_id);

__mram_ptr Node* Seat_get_root(seat_id_t seat_id);
void Seat_set_root(seat_id_t seat_id, __mram_ptr Node* new_root);

__mram_ptr Node* Seat_allocate_node(seat_id_t seat_id);
void Seat_free_node(seat_id_t seat_id, __mram_ptr Node* node);

void Seat_inc_height(seat_id_t seat_id);
int Seat_get_height(seat_id_t seat_id);
int Seat_get_n_nodes(seat_id_t seat_id);
__mram_ptr Node* Seat_get_node_by_id(seat_id_t seat_id, int id);
