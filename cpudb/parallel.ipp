#pragma once

#include <cassert>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>

class ParallelManager
{
    class Worker
    {
        ParallelManager* manager;
        size_t id;

    public:
        Worker(ParallelManager* manager, size_t id)
            : manager(manager), id(id)
        {
        }
        void operator()()
        {
            std::function<void(size_t, size_t)> task;
            size_t s = 0, e = 0;
            while (true) {
                task = manager->get_task(id, &s, &e);
                if (task == nullptr)
                    return;
                task(s, e);
                manager->notify_complete();
            }
        }
    };

    class Barrier
    {
        std::mutex mtx;
        std::condition_variable cv;
        size_t thread_count;
        size_t counter;
        bool stopping = false;

    public:
        Barrier(size_t thread_count)
            : thread_count(thread_count), counter(0) {}

        void wait()
        {
            std::unique_lock<std::mutex> lk(mtx);
            counter++;
            if (counter == thread_count) {
                counter = 0;
                cv.notify_all();
            } else {
                cv.wait(lk, [&] {
                    return stopping || counter == 0;
                });
            }
        }

        void stop()
        {
            std::unique_lock<std::mutex> lk(mtx);
            stopping = true;
            cv.notify_all();
        }
    };

    size_t nthreads;
    Barrier start_barrier, end_barrier;
    bool stopping = false;
    std::function<void(size_t, size_t)> task = nullptr;
    size_t start, end;

    // Called by worker
    std::function<void(size_t, size_t)> get_task(size_t id, size_t* s, size_t* e)
    {
        start_barrier.wait();
        if (stopping)
            return nullptr;
        assert(task != nullptr);
        size_t interval = (end - start) / nthreads;
        *s = start + id * interval;
        *e = (id == nthreads - 1) ? end : start + (id + 1) * interval;
        return task;
    }
    void notify_complete()
    {
        end_barrier.wait();
    }

public:
    ParallelManager(size_t nt)
        : nthreads(nt < 1 ? std::thread::hardware_concurrency() : nt),
          start_barrier(nthreads + 1), end_barrier(nthreads + 1)
    {
        std::cout << "ParallelManager: nthreads=" << nthreads << std::endl;
        for (size_t i = 0; i < nthreads; i++)
            std::thread(Worker(this, i)).detach();
    }

    ~ParallelManager()
    {
        stopping = true;
        start_barrier.stop();
        end_barrier.stop();
    }

    void run(size_t s, size_t e, std::function<void(size_t, size_t)> t)
    {
        start = s;
        end = e;
        task = t;
        start_barrier.wait();  // start all workers
        end_barrier.wait();    // wait for all workers to complete
        task = nullptr;
    }

    size_t get_parallelism() const
    {
        return nthreads;
    }

    friend class Worker;
};
