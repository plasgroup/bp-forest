#include "bplustree.h"
#include "common.h"

//#define SPLIT_THRESHOLD (MAX_NUM_NODES_IN_SEAT * MAX_CHILD / 4)
#define SPLIT_THRESHOLD (4000)
#define NR_ELEMS_AFTER_SPLIT (SPLIT_THRESHOLD)

extern void split_phase(void);
