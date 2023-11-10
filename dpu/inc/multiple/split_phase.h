#include "bplustree.h"
#include "common.h"

#define SPLIT_THRESHOLD (MAX_NUM_NODES_IN_SEAT * MAX_CHILD / 4)

extern __mram split_info_t split_result[NR_SEATS_IN_DPU];

extern int do_split_phase(seat_id_t seat_id, KVPairPtr tree_transfer_buffer);
