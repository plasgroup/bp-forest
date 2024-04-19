#pragma once

#include "common.h"

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>


#ifndef NR_RANKS
#error NR_RANKS should always be defined
#endif

#ifdef UPMEM_SIMULATOR
constexpr size_t MAX_NR_DPUS_IN_RANK = 1;
#else
constexpr size_t MAX_NR_DPUS_IN_RANK = 64;
#endif

constexpr size_t MAX_NR_DPUS = MAX_NR_DPUS_IN_RANK * NR_RANKS;

#ifndef NUM_REQUESTS_PER_BATCH
constexpr size_t NUM_REQUESTS_PER_BATCH = 1000000;
#endif

#ifndef DEFAULT_NR_BATCHES
constexpr size_t DEFAULT_NR_BATCHES = 20;
#endif

#ifndef NUM_INIT_REQS
constexpr size_t NUM_INIT_REQS = 1500 * MAX_NR_DPUS;
#endif
constexpr key_int64_t INIT_KEY_INTERVAL = (KEY_MAX - KEY_MIN) / (NUM_INIT_REQS - 1);


using dpu_id_t = uint32_t;
constexpr dpu_id_t INVALID_DPU_ID = std::numeric_limits<dpu_id_t>::max();

using block_id_t = uint32_t;

template <class T, size_t BlockSize>
struct SizedBuffer {
    uint32_t size_in_elems;
    std::array<T, BlockSize> buf;
};

/* requests for a DPU in a batch */
#if defined(HOST_MULTI_THREAD)
constexpr size_t QUERY_BUFFER_BLOCK_SIZE = MAX_REQ_NUM_IN_A_DPU / HOST_MULTI_THREAD + MAX_REQ_NUM_IN_A_DPU / 100;
using dpu_requests_t = SizedBuffer<each_request_t, QUERY_BUFFER_BLOCK_SIZE>[HOST_MULTI_THREAD];
#else
using dpu_requests_t = each_request_t[MAX_REQ_NUM_IN_A_DPU];
#endif
