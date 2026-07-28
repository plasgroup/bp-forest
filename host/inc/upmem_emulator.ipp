#pragma once

#include "upmem_emulator.hpp"

#include "host_params.hpp"

#include <mutex>
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
