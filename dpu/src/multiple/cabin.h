#include "mram.h"
/* move this section to config.h */

#define NR_SEATS_IN_DPU 20
#define MAX_NODES_IN_SEAT (10 * 1000)

/* move this section to config.h end */
struct BPTreeNode;
typedef BPTreeNode Node;
typedef int seat_id_t;

#define INVALID_SEAT_ID (-1)

extern __mram_ptr Node* Seat_get_root(seat_id_t seat_id);
extern __mram_ptr Node* Seat_allocate_node(seat_id_t seat_id);
extern void Seat_free_node(seat_id_t seat_id, __mram_ptr Node* node);
