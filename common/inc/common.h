#ifndef __COMMON_H__
#define __COMMON_H__

#define NR_ELEMS_PER_DPU (RAND_MAX / NR_DPUS)
#define NR_ELEMS_PER_TASKLET (RAND_MAX / NR_DPUS / NR_TASKLETS)

#ifndef NR_DPUS
#define NR_DPUS (50)
#endif
#ifndef NR_TASKLETS
#define NR_TASKLETS (1)
#endif
#ifndef NUM_BPTREE_IN_DPU
#define NUM_BPTREE_IN_DPU (100)
#endif
#ifndef NUM_BPTREE_IN_CPU
#define NUM_BPTREE_IN_CPU (2)
#endif
#ifndef NUM_REQUESTS
#define NUM_REQUESTS (100000)
#endif
#define NUM_REQUESTS_PER_DPU (NUM_REQUESTS / NR_DPUS)
#define NUM_REQUESTS_PER_TASKLET (NUM_REQUESTS / NR_DPUS / NR_TASKLETS)
#define NUM_REQUESTS_PER_BATCH (100000)
#define MAX_REQ_NUM_IN_A_TREE (NUM_REQUESTS_PER_BATCH)
#define NUM_BATCH ((NUM_REQUESTS - 1) / NUM_REQUESTS_PER_BATCH + 1)
#define READ (1)
#define WRITE (0)

//#define VARY_REQUESTNUM
//#define DEBUG_ON
//#define STATS_ON
//#define WRITE_CSV

#ifdef VARY_REQUESTNUM  // for experiment: xaxis is requestnum
#define NUM_VARS (8)    // number of point of xs
#endif

/* Structure used by both the host and the dpu to communicate information */
#include <stdint.h>

typedef uint64_t intkey_t;
typedef uint64_t value_ptr_t;
typedef struct {
    int num_req;                                   // number of requests
    intkey_t key[MAX_REQ_NUM_IN_A_TREE];           // key of each request
    uint8_t read_or_write[MAX_REQ_NUM_IN_A_TREE];  // 1→read or 0→write
    value_ptr_t
        write_val_ptr[MAX_REQ_NUM_IN_A_TREE];  // write value if request is write
} dpu_request_t;

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

/* Structure used by both the host and the dpu to communicate information */

#endif /* __COMMON_H__ */