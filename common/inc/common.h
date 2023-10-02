#ifndef __COMMON_H__
#define __COMMON_H__

#define NR_ELEMS_PER_DPU (RAND_MAX / NR_DPUS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)

#ifndef NR_DPUS
#define NR_DPUS (4)
#endif
#ifndef NR_TASKLETS
#define NR_TASKLETS (1)
#endif
#ifndef MAX_NUM_BPTREE_IN_DPU
#define MAX_NUM_BPTREE_IN_DPU (NR_TASKLETS)
#endif
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
#define MAX_NUM_TREES_IN_DPU (NR_TASKLETS)
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

#ifdef VARY_REQUESTNUM  // for experiment: xaxis is requestnum
#define NUM_VARS (8)    // number of point of xs
#endif

/* Structure used by both the host and the dpu to communicate information */
#include <stdint.h>

typedef uint64_t key_int64_t;
typedef uint64_t value_ptr_t;
// one request
typedef struct {
    key_int64_t key;            // key of each request
    value_ptr_t write_val_ptr;  // write pointer to the value if request is write
    uint8_t operation;          // 1→read or 0→write
} each_request_t;

// requests for a DPU in a batch
typedef struct {
    each_request_t requests[MAX_REQ_NUM_IN_A_DPU];
} dpu_requests_t;

#ifdef VARY_REQUESTNUM
typedef struct {  // for returning the result of the experiment
    int x;
    int cycle_insert;
    int cycle_get;
} dpu_stats_t;

typedef struct {  // for specifing the points of x in the experiment
    int vars[NUM_VARS];
    int gap;
    int DPUnum;
} dpu_experiment_var_t;
#endif

typedef union {
    char val_ptr;
    uint32_t ifsuccsess;
} dpu_result_t;

/* Tasks */
#define TASK_INIT (0)
#define TASK_GET (10)
#define TASK_INSERT (11)
#define TASK_FROM (100)
#define TASK_TO (101)
#endif /* __COMMON_H__ */
