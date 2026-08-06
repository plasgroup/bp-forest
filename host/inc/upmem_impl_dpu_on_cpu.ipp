#pragma once

//! @file
//! UPMEM module implementation for the dpu_on_cpu build: every DPU is one
//! loaded copy of the CPU-compiled DPU program (dpu_on_cpu_program.hpp).
//! Defines the backend plus read_log()/get_param_dump() (absent in
//! fake_dpu); the rest is shared via upmem_impl_emulated_common.ipp.

#include "dpu_on_cpu_program.hpp"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "log_buffer.hpp"

#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>


constexpr dpu_id_t NrDPUs = MAX_NR_DPUS;
constexpr dpu_id_t NrDPUsInRank = MAX_NR_DPUS_IN_RANK;
inline std::optional<DPUProgramFleet<NrDPUs>> emulated_dpus;


//! @brief Write the DPUs' pending printf output to stderr before dying —
//! the counterpart of the upmem mode's on-fault dpu_log_read; without it,
//! an abort()/assert inside the DPU program would discard the log that
//! explains it.  Draining is not async-signal-safe, but abort is raised
//! synchronously and the process is dying anyway: a best-effort aid.
//! Tasklets may abort concurrently; only the first entrant dumps.
extern "C" inline void dpu_on_cpu_dump_logs_on_abort(int signum)
{
    static std::atomic_flag dump_claimed = ATOMIC_FLAG_INIT;
    if (dump_claimed.test_and_set()) {
        // park this thread until the first entrant's re-raise kills the
        // process; pause() is async-signal-safe and blocks in the kernel
        for (;;) {
            pause();
        }
    }

    if (emulated_dpus.has_value()) {
        std::fputs("dpu_on_cpu: pending DPU logs at abort:\n", stderr);
        for (dpu_id_t i = 0; i < NrDPUs; i++) {
            emulated_dpus->drain_log_into(stderr, i, true);
        }
    }
    std::signal(signum, SIG_DFL);
    std::raise(signum);
}

inline void upmem_init_impl()
{
    all_dpu.reset();
    all_dpu.flip();

    const char* const so_path = std::getenv("DPU_ON_CPU_PROGRAM_PATH");
    emulated_dpus.emplace(so_path != nullptr ? so_path : DPU_CPU_PROGRAM_PATH);

    std::signal(SIGABRT, dpu_on_cpu_dump_logs_on_abort);
}
inline void upmem_release_impl()
{
    all_dpu.reset();
    emulated_dpus.reset();
}


//! @brief Collect the pending printf output of the DPUs in the set, in
//! ascending DPU-ID order with per-DPU headers — different DPUs never mix.
inline std::unique_ptr<LogBuffer> read_log(const DPUSet& set)
{
    LogStream stream;
    for (dpu_id_t i = 0; i < NrDPUs; i++) {
        if (set[i]) {
            emulated_dpus->drain_log_into(stream.get(), i);
        }
    }
    return std::move(stream).close();
}

inline std::unique_ptr<char[]> get_param_dump()
{
    return emulated_dpus->param_dump();
}


#include "upmem_impl_emulated_common.ipp"
