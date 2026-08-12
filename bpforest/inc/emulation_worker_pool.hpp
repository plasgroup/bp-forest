#pragma once

//! @file
//! Worker-thread pool shared by the emulated-DPU builds (fake_dpu and
//! dpu_on_cpu) for running DPU launches concurrently.

#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>


//! @brief Number of worker threads for running emulated DPUs: fills the
//! hardware threads, given that one running DPU occupies nr_threads_per_dpu
//! threads.  The environment variable EMU_NR_WORKERS overrides it (e.g. =1
//! to run the DPUs of each launch one by one).
inline unsigned decide_nr_emulation_workers(unsigned nr_threads_per_dpu, unsigned nr_dpus)
{
    if (const char* const env = std::getenv("EMU_NR_WORKERS")) {
        const unsigned nr_workers = static_cast<unsigned>(std::atoi(env));
        if (nr_workers != 0) {
            return std::min(nr_workers, nr_dpus);
        }
    }
    const unsigned nr_cores = std::max(1u, std::thread::hardware_concurrency());
    return std::min(std::max(1u, nr_cores / std::max(1u, nr_threads_per_dpu)), nr_dpus);
}


//! @brief Executes DPU launches on a fixed pool of worker threads.
//! A launch is one call of DPU::execute(); wait_all() blocks until every
//! enqueued launch has finished.
template <class DPU>
class EmulationWorkerPool
{
    std::mutex mtx;
    std::condition_variable to_worker;
    std::condition_variable to_manager;
    std::queue<DPU*> queue;
    unsigned nr_unfinished = 0;
    bool stop = false;
    std::vector<std::thread> workers;

public:
    explicit EmulationWorkerPool(unsigned nr_workers)
    {
        for (unsigned i = 0; i < nr_workers; i++) {
            workers.emplace_back([this] { work(); });
        }
    }
    ~EmulationWorkerPool()
    {
        {
            std::lock_guard<std::mutex> lk{mtx};
            stop = true;
        }
        to_worker.notify_all();
        for (std::thread& worker : workers) {
            worker.join();
        }
    }

    EmulationWorkerPool(const EmulationWorkerPool&) = delete;
    EmulationWorkerPool& operator=(const EmulationWorkerPool&) = delete;

    void add_work(DPU* dpu)
    {
        {
            std::lock_guard<std::mutex> lk{mtx};
            nr_unfinished++;
            queue.push(dpu);
        }
        to_worker.notify_one();
    }
    void wait_all()
    {
        std::unique_lock<std::mutex> lk{mtx};
        to_manager.wait(lk, [this] {
            return nr_unfinished == 0;
        });
    }

private:
    void work()
    {
        for (;;) {
            DPU* dpu;
            {
                std::unique_lock<std::mutex> lk{mtx};
                to_worker.wait(lk, [this] {
                    return stop || !queue.empty();
                });
                if (stop && queue.empty()) {
                    return;
                }
                dpu = queue.front();
                queue.pop();
            }

            dpu->execute();

            {
                std::lock_guard<std::mutex> lk{mtx};
                nr_unfinished--;
                if (nr_unfinished == 0) {
                    to_manager.notify_all();
                }
            }
        }
    }
};
