#pragma once

#include "dpu_emulator.hpp"
#include "host_params.hpp"

#include <array>
#include <cstddef>
#include <iosfwd>


#define EMU_MULTI_THREAD 16 /* nr worker threads */
#ifdef EMU_MULTI_THREAD

#include <condition_variable>
#include <mutex>
#include <queue>

class EmulationWorkerManager
{
    std::mutex mtx;
    std::condition_variable to_worker;
    std::condition_variable to_manager;
    std::queue<DPUEmulator*> queue;
    unsigned nr_unfinished = 0;
    bool stop = false;

    class Worker
    {
        EmulationWorkerManager* const manager;

    public:
        Worker(EmulationWorkerManager* manager) : manager{manager} {}
        void operator()();
    };

public:
    EmulationWorkerManager();
    ~EmulationWorkerManager();
    void add_work(DPUEmulator* emu);
    void wait_all();
};

#endif /* EMU_MULTI_THREAD */


template <dpu_id_t NrDPUs>
class UPMEMEmulator
{
#ifdef EMU_MULTI_THREAD
    EmulationWorkerManager worker_manager;
#endif /* EMU_MULTI_THREAD */

    std::array<DPUEmulator, NrDPUs> dpus;

public:
    std::byte* get_comm_buffer(dpu_id_t idx_dpu) { return dpus[idx_dpu].get_comm_buffer(); }
    void launch_dpu(dpu_id_t);
    void wait_all();

    void print_nr_queries_in_last_batch(std::ostream&, dpu_id_t nr_dpus_to_print) const;
    void print_nr_RMQ_delims_in_last_batch(std::ostream&, dpu_id_t nr_dpus_to_print) const;
    void print_nr_pairs(std::ostream&, dpu_id_t nr_dpus_to_print) const;
};


#include "upmem_emulator.ipp"
