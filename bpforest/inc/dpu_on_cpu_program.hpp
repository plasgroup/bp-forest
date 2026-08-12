#pragma once

//! @file
//! Host-side management of CPU-compiled DPU program instances for the
//! dpu_on_cpu build (see dpu_on_cpu/shim/dpu_cpu.h for the runtime the
//! instances export).

#include "emulation_worker_pool.hpp"
#include "host_params.hpp"

#include <dlfcn.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>


//! @brief One emulated DPU: a private copy of the CPU-compiled DPU program.
//!
//! Backed by a dlopen handle.  The program state stays private per DPU only
//! because each instance is loaded from its own copy of the shared-object
//! file (dlopen returns the already-loaded image for a known file) with
//! RTLD_LOCAL (RTLD_GLOBAL would bind every copy's references to the first
//! copy's globals).
class DPUProgramInstance
{
    void* handle = nullptr;
    std::byte* comm_buffer = nullptr;
    void (*run)() = nullptr;
    char* (*read_log)() = nullptr;

public:
    DPUProgramInstance() = default;
    ~DPUProgramInstance() { unload(); }
    DPUProgramInstance(const DPUProgramInstance&) = delete;
    DPUProgramInstance& operator=(const DPUProgramInstance&) = delete;

    void load(const std::string& so_path, dpu_id_t idx_dpu)
    {
        handle = dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            std::cerr << "dlopen: " << dlerror() << std::endl;
            std::exit(EXIT_FAILURE);
        }
        comm_buffer = static_cast<std::byte*>(symbol("dpu_cpu_mram_heap"));
        run = reinterpret_cast<void (*)()>(symbol("dpu_cpu_run"));
        read_log = reinterpret_cast<char* (*)()>(symbol("dpu_cpu_read_log"));
        *static_cast<unsigned*>(symbol("dpu_cpu_dpu_id")) = static_cast<unsigned>(idx_dpu);
    }
    void unload()
    {
        if (handle != nullptr) {
            dlclose(handle);
            handle = nullptr;
        }
    }

    void* symbol(const char* name) const
    {
        void* const addr = dlsym(handle, name);
        if (addr == nullptr) {
            std::cerr << "dlsym(" << name << "): " << dlerror() << std::endl;
            std::exit(EXIT_FAILURE);
        }
        return addr;
    }

    std::byte* get_comm_buffer() const { return comm_buffer; }

    //! @brief Perform one DPU launch; returns when all tasklets finish.
    void execute() { run(); }

    //! @brief Write the log of this DPU's current launch to stream, prefixed
    //! with the UPMEM SDK's dpu_log_read() header.  Non-consuming, like
    //! dpu_log_read.  With skip_if_empty, an empty log emits nothing.
    void drain_log_into(std::FILE* stream, dpu_id_t idx_dpu, bool skip_if_empty = false)
    {
        char* const log = read_log();
        if (log == nullptr && skip_if_empty) {
            return;
        }
        std::fprintf(stream, "=== DPU#0x%x ===\n", static_cast<unsigned>(idx_dpu));
        if (log != nullptr) {
            std::fputs(log, stream);
            std::free(log);
        }
    }
};


//! @brief The set of all emulated DPUs, indexed by dpu_id_t.
//!
//! Construction makes one file copy of the DPU program per DPU in a
//! process-unique temporary directory (concurrent processes of the same
//! build must not clobber each other's mapped copies) and loads each copy;
//! destruction removes it all.
template <dpu_id_t NrDPUs>
class DPUProgramFleet
{
    std::array<DPUProgramInstance, NrDPUs> dpus;
    std::filesystem::path work_dir;
    std::optional<EmulationWorkerPool<DPUProgramInstance>> pool;

public:
    explicit DPUProgramFleet(const char* so_path)
    {
        std::error_code ec;

        work_dir = std::filesystem::temp_directory_path()
                   / ("bp-forest-dpu-on-cpu-" + std::to_string(getpid()));
        std::filesystem::create_directories(work_dir, ec);
        if (ec) {
            std::cerr << "cannot create " << work_dir << ": " << ec.message() << std::endl;
            std::exit(EXIT_FAILURE);
        }

        const std::string so_name = std::filesystem::path{so_path}.filename().string();
        for (dpu_id_t i = 0; i < NrDPUs; i++) {
            const std::filesystem::path copy = work_dir / (so_name + "." + std::to_string(i));
            std::filesystem::copy_file(so_path, copy, std::filesystem::copy_options::overwrite_existing, ec);
            if (ec) {
                std::cerr << "cannot copy " << so_path << " to " << copy << ": " << ec.message() << std::endl;
                std::exit(EXIT_FAILURE);
            }
            dpus[i].load(copy.string(), i);
        }

        const unsigned nr_tasklets = *static_cast<const unsigned*>(dpus[0].symbol("dpu_cpu_nr_tasklets"));
        pool.emplace(decide_nr_emulation_workers(nr_tasklets, NrDPUs));
    }
    ~DPUProgramFleet()
    {
        pool.reset();
        for (DPUProgramInstance& dpu : dpus) {
            dpu.unload();
        }
        std::error_code ec;
        std::filesystem::remove_all(work_dir, ec);
    }

    DPUProgramFleet(const DPUProgramFleet&) = delete;
    DPUProgramFleet& operator=(const DPUProgramFleet&) = delete;

    std::byte* get_comm_buffer(dpu_id_t idx_dpu) { return dpus[idx_dpu].get_comm_buffer(); }
    void launch_dpu(dpu_id_t idx_dpu) { pool->add_work(&dpus[idx_dpu]); }
    void wait_all() { pool->wait_all(); }

    //! @brief Size of each instance's emulated MRAM heap (= comm buffer).
    size_t mram_heap_bytes() const
    {
        return *static_cast<const uint32_t*>(dpus[0].symbol("dpu_cpu_mram_heap_bytes"));
    }

    void drain_log_into(std::FILE* stream, dpu_id_t idx_dpu, bool skip_if_empty = false)
    {
        dpus[idx_dpu].drain_log_into(stream, idx_dpu, skip_if_empty);
    }

    //! @brief The DPU program's parameter dump (its ParamDump string).
    //! @return a NUL-terminated copy
    std::unique_ptr<char[]> param_dump() const
    {
        const uint64_t size = *static_cast<const uint64_t*>(dpus[0].symbol("ParamDumpSize"));
        const char* const dump = static_cast<const char*>(dpus[0].symbol("ParamDump"));

        std::unique_ptr<char[]> result{new char[size + 1]};
        std::memcpy(&result[0], dump, size);
        result[size] = '\0';

        return result;
    }
};
