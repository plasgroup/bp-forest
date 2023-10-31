#include "bplustree.h"
#include "common.h"

#define SPLIT_THRESHOLD (MAX_NODE_NUM * MAX_CHILD / 4)

extern __mram split_info_t split_result[NUM_SEAT_IN_A_DPU];

extern int do_split_phase(MBPTptr);
