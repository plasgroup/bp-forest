#pragma once

#include "parallel.hpp"

#include <cstddef>
#include <mutex>
#include <thread>


template <class Derived>
inline ParallelManager<Derived>::ParallelManager(unsigned nt)
    : threads(nt < 1 ? std::thread::hardware_concurrency() : nt)
{
    for (unsigned id = 0; id < threads.size(); id++) {
        threads[id] = std::thread{[this, id] {
            for (;;) {
                void (Derived::*local_task)(unsigned worker_id);
                {
                    std::unique_lock<std::mutex> lk{mtx};
                    to_worker.wait(lk, [&] {
                        return task != nullptr || stopping;
                    });
                    if (stopping) {
                        return;
                    }
                    local_task = task;

                    nr_launched_workers++;
                    if (nr_launched_workers == threads.size()) {
                        task = nullptr;
                        nr_launched_workers = 0;
                        to_worker.notify_all();
                    } else {
                        to_worker.wait(lk, [&] {
                            return nr_launched_workers == 0;
                        });
                    }
                }

                (static_cast<Derived*>(this)->*local_task)(id);

                {
                    std::unique_lock<std::mutex> lk{mtx};
                    nr_finished_workers++;
                    to_manager.notify_one();
                }
            }
        }};
    }
}
template <class Derived>
inline ParallelManager<Derived>::~ParallelManager()
{
    {
        std::lock_guard<std::mutex> lk{mtx};
        stopping = true;
    }
    to_worker.notify_all();
    for (auto& th : threads) {
        th.join();
    }
}

template <class Derived>
inline void ParallelManager<Derived>::parallel_run(void (Derived::*t)(unsigned worker_id))
{
    std::unique_lock<std::mutex> lk{mtx};
    task = t;

    to_worker.notify_all();
    to_manager.wait(lk, [&] {
        return nr_finished_workers == threads.size();
    });

    nr_finished_workers = 0;
}
