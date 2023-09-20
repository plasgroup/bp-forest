#pragma once
/* definitions used by both CPU and DPUs */
#define NR_ELEMS_PER_DPU (RAND_MAX / NR_DPUS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)

#ifndef NR_DPUS
#define NR_DPUS (4)
#endif
#ifndef NR_TASKLETS
#define NR_TASKLETS (1)
#endif
#define MAX_NUM_BPTREE_IN_DPU (100)
#define MAX_NUM_BPTREE_IN_CPU (50)
#define MAX_NUM_SPLIT (10)
// #define NODE_DATA_SIZE (40)  // maximum node data size, MB
// #define MAX_NODE_NUM \
//     ((NODE_DATA_SIZE << 20) / sizeof(BPTreeNode) / MAX_NUM_BPTREE_IN_DPU)  // the maximum number of nodes in a tree
// #define DEBUG_ON
// the size for a request(default:17B)
#define REQUEST_SIZE (17)
// buffer size for request in a DPU(default:20MB/64MB)
#ifndef MRAM_REQUEST_BUFFER_SIZE
#define MRAM_REQUEST_BUFFER_SIZE (10 * 1024 * 1024)
#endif
// default:1,233,618 requests / DPU / batch
#ifndef MAX_REQ_NUM_IN_A_DPU
#define MAX_REQ_NUM_IN_A_DPU (MRAM_REQUEST_BUFFER_SIZE / REQUEST_SIZE)
#endif
#define NODE_DATA_SIZE (40)                                         // maximum node data size, MB
#define MAX_NODE_NUM_IN_DPU ((NODE_DATA_SIZE << 20) / sizeof(BPTreeNode))  // NODE_DATA_SIZE MB for Node data
#define MAX_NODE_NUM_TRANSFER (MAX_NODE_NUM_IN_DPU / MAX_NUM_BPTREE_IN_DPU)
#define SPLIT_THRESHOLD (MAX_CHILD * MAX_CHILD)
#define NUM_REQUESTS_PER_BATCH (1000000)
#define READ (1)
#define WRITE (0)
#define W50R50 (1)
#define W05R95 (2)
#ifndef WORKLOAD
#define WORKLOAD (W50R50)
#endif
// #define PRINT_DEBUG
// #define VARY_REQUESTNUM
// #define DEBUG_ON
// #define STATS_ON
// #define PRINT_ON
// #define WRITE_CSV
