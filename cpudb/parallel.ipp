#pragma once

#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>

class ParallelManager {
    class Worker {
        ParallelManager* manager;
        int id;
    public:
        Worker(ParallelManager* manager, int id)
            : manager(manager), id(id) {
            }
        void operator ()() {
            std::function<void(size_t, size_t)> task;
            while (true) {
                size_t s = 0, e = 0;
                task = manager->get_task(id, &s, &e);
                if (task == nullptr)
                    return;
                task(s, e);
                manager->notify_complete(id);
            }
        }
    };

    class Barrier {
        std::mutex mtx;
        std::condition_variable cv;
        size_t thread_count;
        size_t counter;
        bool stopping = false;
    public:
        Barrier(size_t thread_count)
            : thread_count(thread_count), counter(0) {}
        
        void wait(size_t) {
            std::unique_lock<std::mutex> lk(mtx);
        //    std::cout << "Worker:" << id << " waiting" << std::endl;
            counter++;
            if (counter == thread_count) {
                counter = 0;
                cv.notify_all();
            } else {
                cv.wait(lk, [&] {
                    return stopping || counter == 0;
                });
            }
        //    std::cout << "Worker:" << id << " done" << std::endl;
        }

        void stop(int) {
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
    std::thread* threads;

     // Called by worker
    std::function<void(size_t, size_t)> get_task(size_t id, size_t* s, size_t* e) {
        start_barrier.wait(id);
        if (stopping)
            return nullptr;
        assert(task != nullptr);
        size_t interval = (end - start) / nthreads;
        *s = start + id * interval;
        *e = (id == nthreads - 1) ? end : start + (id + 1) * interval;
        return task;
    }
    void notify_complete(int id) {
        end_barrier.wait(id);
    }

public:
    ParallelManager(int nt)
        : nthreads(nt < 1 ? std::thread::hardware_concurrency() : nt),
          start_barrier(nthreads + 1), end_barrier(nthreads + 1)
    {
        std::cout << "ParallelManager: nthreads=" << nthreads << std::endl;
        threads = new std::thread[nthreads];
        for (size_t i = 0; i < nthreads; i++)
            threads[i] = std::thread(Worker(this, i));
    }

    ~ParallelManager() {
        stopping = true;
        start_barrier.stop(-1);
        end_barrier.stop(-1);
        for (size_t i = 0; i < nthreads; i++)
            threads[i].join();
    }

    void run(size_t s, size_t e, std::function<void(size_t, size_t)> t)
    {
        if (nthreads == 1) {
            t(s, e);
            return;
        } else {
            start = s;
            end = e;
            task = t;
            start_barrier.wait(-1); // start all workers
            end_barrier.wait(-1); // wait for all workers to complete
            task = nullptr;
        }
    }

    int get_parallelism() const {
        return nthreads;
    }

    friend class Worker;
};

