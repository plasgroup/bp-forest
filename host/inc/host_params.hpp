#pragma once

#include "common.h"

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>


#ifndef NR_DPUS
constexpr size_t NR_DPUS = 4;
#endif

#ifndef NR_DPUS_IN_RANK
constexpr size_t NR_DPUS_IN_RANK = 64;
#endif

#ifndef NR_INITIAL_TREES_IN_DPU
constexpr size_t NR_INITIAL_TREES_IN_DPU = 12;
#endif
static_assert(NR_SEATS_IN_DPU == NR_INITIAL_TREES_IN_DPU, "parameter error: NR_SEAT_IN_DPU != NR_INITIAL_TREES_IN_DPU");

#ifndef NUM_REQUESTS_PER_BATCH
constexpr size_t NUM_REQUESTS_PER_BATCH = 1000000;
#endif

#ifndef DEFAULT_NR_BATCHES
constexpr size_t DEFAULT_NR_BATCHES = 20;
#endif

#ifndef NUM_INIT_REQS
constexpr size_t NUM_INIT_REQS = 2000 * (NR_DPUS * NR_INITIAL_TREES_IN_DPU);
#endif

#ifndef SOFT_LIMIT_NR_TREES_IN_DPU
constexpr size_t SOFT_LIMIT_NR_TREES_IN_DPU = NR_SEATS_IN_DPU - MAX_NUM_SPLIT - 1;
#endif

constexpr size_t MERGE_THRESHOLD = 1500;
constexpr size_t NUM_ELEMS_AFTER_MERGE = 2000;


using dpu_id_t = uint32_t;
constexpr dpu_id_t INVALID_DPU_ID = std::numeric_limits<dpu_id_t>::max();

using block_id_t = uint32_t;

template <class T, size_t BlockSize>
struct SizedBuffer {
    uint32_t size_in_elems;
    std::array<T, BlockSize> buf;
};

/* requests for a DPU in a batch */
#if defined(HOST_MULTI_THREAD) && defined(QUERY_GATHER_XFER)
constexpr size_t QUERY_BUFFER_BLOCK_SIZE = MAX_REQ_NUM_IN_A_DPU / HOST_MULTI_THREAD + MAX_REQ_NUM_IN_A_DPU / 100;
using dpu_requests_t = SizedBuffer<each_request_t, QUERY_BUFFER_BLOCK_SIZE>[HOST_MULTI_THREAD];
#else
using dpu_requests_t = each_request_t[MAX_REQ_NUM_IN_A_DPU];
#endif
