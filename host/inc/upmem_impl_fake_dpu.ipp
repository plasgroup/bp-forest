#pragma once

//! @file
//! UPMEM module implementation for the fake_dpu build: every DPU is a
//! FakeDPU, a fake reimplementation of the DPU program (see
//! fake_dpu.hpp).  Everything except the backend definition is shared
//! with the dpu_on_cpu build via upmem_impl_emulated_common.ipp.

#include "dpu_set.hpp"
#include "host_params.hpp"
#include "statistics.hpp"
#include "fake_dpu_fleet.hpp"

#include <optional>


constexpr dpu_id_t NrDPUs = MAX_NR_DPUS;
constexpr dpu_id_t NrDPUsInRank = MAX_NR_DPUS_IN_RANK;
inline std::optional<FakeDPUFleet<NrDPUs>> emulated_dpus;


inline void upmem_init_impl()
{
    all_dpu.reset();
    all_dpu.flip();
    emulated_dpus.emplace();
}
inline void upmem_release_impl()
{
    all_dpu.reset();
    emulated_dpus.reset();
}


#include "upmem_impl_emulated_common.ipp"
