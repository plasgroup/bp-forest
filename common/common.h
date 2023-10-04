#ifndef __COMMON_H__
#define __COMMON_H__

/* DPU variable that will be read of write by the host */
#define NR_ELEMS_PER_DPU (RAND_MAX / NR_DPUS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)
/* Size of the table */
#define NUM_BATCH (5)
#ifndef NR_DPUS
#define NR_DPUS (2500)
#endif
#ifndef NR_TASKLETS
#define NR_TASKLETS (1)
#endif
#ifndef NUM_REQUESTS
#define NUM_REQUESTS (NR_DPUS * NR_TASKLETS * 10000) // 10000reqs for 1 tasklet per batch
#endif
#define NUM_REQUESTS_PER_DPU (NUM_REQUESTS / NR_DPUS)
#define NUM_REQUESTS_PER_TASKLET (NUM_REQUESTS / NR_DPUS / NR_TASKLETS)
#define MAX_REQ_NUM_PER_TASKLET_IN_BATCH (NUM_REQUESTS_PER_TASKLET * 2)
#define READ (1)
#define WRITE (0)

// #define VARY_REQUESTNUM
// #define DEBUG_ON
// #define STATS_ON
// #define WRITE_CSV

#ifdef VARY_REQUESTNUM // for experiment: xaxis is requestnum
#define NUM_VARS (8)   // number of point of xs
#endif

/* Structure used by both the host and the dpu to communicate information */
#include <stdint.h>

typedef uint64_t key_t;
typedef uint64_t value_ptr_t;
typedef struct
{
    int num_req;                                                 // number of requests
    key_t key[MAX_REQ_NUM_PER_TASKLET_IN_BATCH];                 // key of each request
    uint8_t read_or_write[MAX_REQ_NUM_PER_TASKLET_IN_BATCH];     // 1→read or 0→write
    value_ptr_t write_val_ptr[MAX_REQ_NUM_PER_TASKLET_IN_BATCH]; // write value if request is write
} dpu_request_t;

#ifdef VARY_REQUESTNUM
typedef struct
{ // for returning the result of the experiment
    int x;
    int cycle_insert;
    int cycle_get;
} dpu_stats_t;

typedef struct
{ // for specifing the points of x in the experiment
    int vars[NUM_VARS];
    int gap;
    int DPUnum;
} dpu_experiment_var_t;
#endif

typedef union
{
    char val_ptr;
    uint32_t ifsuccsess;
} dpu_result_t;

/* Structure used by both the host and the dpu to communicate information */

#endif /* __COMMON_H__ */