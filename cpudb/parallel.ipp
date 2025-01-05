#pragma once

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

class ParallelManager
{
    class Worker
    {
        ParallelManager* const manager;
        const unsigned id;

    public:
        Worker(ParallelManager* manager, unsigned id)
            : manager{manager}, id{id}
        {
        }
        void operator()()
        {
            for (;;) {
                std::function<void(size_t, size_t)> local_task;
                {
                    std::unique_lock<std::mutex> lk{manager->mtx};
                    manager->to_worker.wait(lk, [&] {
                        return manager->task != nullptr || manager->stopping;
                    });
                    if (manager->stopping) {
                        return;
                    }
                    local_task = manager->task;

                    manager->nr_launched_workers++;
                    if (manager->nr_launched_workers == manager->threads.size()) {
                        manager->task = nullptr;
                        manager->nr_launched_workers = 0;
                    }
                }

                size_t interval = (manager->end - manager->start) / manager->threads.size();
                size_t local_start = manager->start + id * interval,
                       local_end = (id == manager->threads.size() - 1) ? manager->end : manager->start + (id + 1) * interval;
                local_task(local_start, local_end);

                {
                    std::unique_lock<std::mutex> lk{manager->mtx};
                    manager->nr_finished_workers++;
                    manager->to_manager.notify_one();
                }
            }
        }
    };

    std::vector<std::thread> threads;

    std::mutex mtx;
    std::condition_variable to_worker, to_manager;
    unsigned nr_launched_workers = 0, nr_finished_workers = 0;
    bool stopping = false;

    std::function<void(size_t, size_t)> task = nullptr;
    size_t start, end;

public:
    ParallelManager(unsigned nt)
        : threads(nt < 1 ? std::thread::hardware_concurrency() : nt)
    {
        std::cout << "ParallelManager: threads.size()=" << threads.size() << std::endl;
        for (unsigned id = 0; id < threads.size(); id++)
            threads[id] = std::thread{Worker{this, id}};
    }
    ParallelManager(const ParallelManager&) = delete;

    ~ParallelManager()
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

    void run(size_t s, size_t e, std::function<void(size_t, size_t)> t)
    {
        std::unique_lock<std::mutex> lk{mtx};
        task = t;
        start = s;
        end = e;

        to_worker.notify_all();
        to_manager.wait(lk, [&] {
            return nr_finished_workers == threads.size();
        });

        nr_finished_workers = 0;
    }

    size_t get_parallelism() const
    {
        return threads.size();
    }
};
