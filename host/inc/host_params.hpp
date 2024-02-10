#pragma once

#include "common.h"

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
static_assert(NR_SEATS_IN_DPU >= NR_INITIAL_TREES_IN_DPU, "parameter error: NR_SEAT_IN_DPU < NR_INITIAL_TREES_IN_DPU");

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
