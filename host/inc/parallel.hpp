#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <vector>


template <class Derived>
class ParallelManager
{
    std::vector<std::thread> threads;

    std::mutex mtx;
    std::condition_variable to_worker, to_manager;
    unsigned nr_launched_workers = 0, nr_finished_workers = 0;
    bool stopping = false;

    void (Derived::*task)(unsigned worker_id) = nullptr;

protected:
    explicit ParallelManager(unsigned nt);
    ParallelManager(const ParallelManager&) = delete;

    ~ParallelManager();

    void parallel_run(void (Derived::*t)(unsigned worker_id));

    unsigned get_parallelism() const { return static_cast<unsigned>(threads.size()); }
};


#include "parallel.ipp"
