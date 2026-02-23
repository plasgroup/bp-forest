#pragma once
/* Listing parameters and setting default values */

/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() TOGETHER !!!!!!!!!!!!!! */

#include "common_params.h"

#include "common.h"
#include "workload_types.h"

#include <cstddef>
#include <cstdint>
#include <limits>


constexpr size_t MAX_NR_DPUS = MAX_NR_DPUS_IN_RANK * NR_RANKS;

#ifndef NUM_REQUESTS_PER_BATCH
#define NUM_REQUESTS_PER_BATCH 1000000
#endif

#ifndef DEFAULT_NR_BATCHES
#define DEFAULT_NR_BATCHES 20
#endif

#ifndef NUM_INIT_REQS
#define NUM_INIT_REQS (20000 * MAX_NR_DPUS)
#endif

inline key_uint64_t init_key_interval(size_t nr_keys)
{
    return (KEY_MAX - KEY_MIN) / (nr_keys - 1);
}

using dpu_id_t = uint32_t;
constexpr dpu_id_t INVALID_DPU_ID = std::numeric_limits<dpu_id_t>::max();

using block_id_t = uint32_t;

#ifndef KVPAIRS_CHUNK_SIZE
#define KVPAIRS_CHUNK_SIZE 256
#endif
constexpr uint32_t KVPairsChunkSize = KVPAIRS_CHUNK_SIZE;


// #define TOUCH_QUERIES_IN_ADVANCE
// #define DEBUG_ON
// #define PRINT_DEBUG

#ifdef HOST_ONLY
// #define MEASURE_XFER_BYTES
#else
// #define UPMEM_TRACE
#endif

// #define SYNCHRONOUS_DPU_EXEC

// #define EXTRACT_BY_INITIALIZATION
