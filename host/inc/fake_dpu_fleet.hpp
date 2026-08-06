#pragma once

#include "fake_dpu.hpp"
#include "emulation_worker_pool.hpp"
#include "host_params.hpp"

#include <array>
#include <cstddef>


//! @brief The set of all DPUs of the fake_dpu build, each a FakeDPU.
//!
//! launch_dpu()/wait_all() run launches on the shared worker pool; a
//! FakeDPU runs single-threaded, so one running DPU occupies one thread.
template <dpu_id_t NrDPUs>
class FakeDPUFleet
{
    EmulationWorkerPool<FakeDPU> pool{decide_nr_emulation_workers(1, NrDPUs)};

    std::array<FakeDPU, NrDPUs> dpus;

public:
    std::byte* get_comm_buffer(dpu_id_t idx_dpu) { return dpus[idx_dpu].get_comm_buffer(); }
    void launch_dpu(dpu_id_t idx_dpu) { pool.add_work(&dpus[idx_dpu]); }
    void wait_all() { pool.wait_all(); }

    //! @brief Size of each DPU's emulated MRAM (= comm buffer).
    static constexpr size_t mram_heap_bytes() { return FakeDPU::MRAMSize; }
};
