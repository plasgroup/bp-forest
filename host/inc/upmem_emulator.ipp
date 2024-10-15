#pragma once

#include "upmem_emulator.hpp"

#include "host_params.hpp"

#include <mutex>
#include <ostream>
#include <thread>


#ifdef EMU_MULTI_THREAD
inline EmulationWorkerManager::EmulationWorkerManager()
{
    for (int i = 0; i < EMU_MULTI_THREAD; i++)
        std::thread(Worker{this}).detach();
}
inline EmulationWorkerManager::~EmulationWorkerManager()
{
    {
        std::lock_guard<std::mutex> lk(mtx);
        stop = true;
    }
    to_worker.notify_all();
}
inline void EmulationWorkerManager::add_work(DPUEmulator* emu)
{
    {
        std::lock_guard<std::mutex> lk(mtx);
        nr_unfinished++;
        queue.push(emu);
    }
    to_worker.notify_one();
}
inline void EmulationWorkerManager::wait_all()
{
    std::unique_lock<std::mutex> lk(mtx);
    to_manager.wait(lk, [this] {
        return nr_unfinished == 0;
    });
}

inline void EmulationWorkerManager::Worker::operator()()
{
    for (;;) {
        DPUEmulator* emu;
        {
            std::unique_lock<std::mutex> lk(manager->mtx);
            manager->to_worker.wait(lk, [this] {
                return manager->stop || !manager->queue.empty();
            });
            if (manager->stop)
                return;
            emu = manager->queue.front();
            manager->queue.pop();
        }

        emu->execute();

        bool am_i_last;
        {
            std::lock_guard<std::mutex> lk(manager->mtx);
            manager->nr_unfinished--;
            am_i_last = (manager->nr_unfinished == 0);
        }
        if (am_i_last) {
            manager->to_manager.notify_all();
        }
    }
}
#endif /* EMU_MULTI_THREAD */


template <dpu_id_t NrDPUs>
inline void UPMEMEmulator<NrDPUs>::launch_dpu(dpu_id_t idx_dpu)
{
#ifdef EMU_MULTI_THREAD
    worker_manager.add_work(&dpus[idx_dpu]);
#else  /* EMU_MULTI_THREAD */
    dpus[idx_dpu].execute();
#endif /* EMU_MULTI_THREAD */
}

template <dpu_id_t NrDPUs>
inline void UPMEMEmulator<NrDPUs>::wait_all()
{
#ifdef EMU_MULTI_THREAD
    worker_manager.wait_all();
#endif /* EMU_MULTI_THREAD */
}

template <dpu_id_t NrDPUs>
inline void UPMEMEmulator<NrDPUs>::print_nr_queries_in_last_batch(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
{
    if (nr_dpus_to_print > 0) {
        for (dpu_id_t idx_dpu = 0; idx_dpu < NrDPUs && idx_dpu < nr_dpus_to_print; idx_dpu++) {
            if (idx_dpu != 0) {
                ostr << ", ";
            }
            ostr << (dpus[idx_dpu].get_nr_queries_to_cold_range_in_last_batch() + dpus[idx_dpu].get_nr_queries_to_hot_range_in_last_batch());
        }
        ostr << std::endl;
    }
}
template <dpu_id_t NrDPUs>
inline void UPMEMEmulator<NrDPUs>::print_nr_pairs(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
{
    if (nr_dpus_to_print > 0) {
        for (dpu_id_t idx_dpu = 0; idx_dpu < NrDPUs && idx_dpu < nr_dpus_to_print; idx_dpu++) {
            if (idx_dpu != 0) {
                ostr << ", ";
            }
            ostr << (dpus[idx_dpu].get_nr_pairs_in_cold_range() + dpus[idx_dpu].get_nr_pairs_in_hot_range());
        }
        ostr << std::endl;
    }
}
