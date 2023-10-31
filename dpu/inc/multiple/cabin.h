#ifndef __CABIN_H__
#define __CABIN_H__

#include "mram.h"
/* move this section to config.h */

#define NR_SEATS_IN_DPU 10
#define MAX_NODES_IN_SEAT (10)

/* move this section to config.h end */
struct BPTreeNode;
typedef struct BPTreeNode Node;
typedef int seat_id_t;

#define INVALID_SEAT_ID (-1)

extern seat_id_t Cabin_allocate_seat();
extern void Cabin_release_seat(seat_id_t seat_id);
extern __mram_ptr Node* Seat_get_root(seat_id_t seat_id);
extern void Seat_set_root(seat_id_t seat_id, __mram_ptr Node* new_root);
extern __mram_ptr Node* Seat_allocate_node(seat_id_t seat_id);
extern void Seat_free_node(seat_id_t seat_id, __mram_ptr Node* node);
extern void Seat_inc_height(seat_id_t seat_id);
extern int Seat_get_height(seat_id_t seat_id);
extern int Seat_get_n_nodes(seat_id_t seat_id);

#endif /* __CABIN_H__ */