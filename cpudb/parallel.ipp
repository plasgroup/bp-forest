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
                size_t s, e;
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
        
        void wait(int id) {
            std::unique_lock<std::mutex> lk(mtx);
//            std::cout << "Worker:" << id << " waiting" << std::endl;
            counter++;
            if (counter == thread_count) {
                counter = 0;
                cv.notify_all();
            } else {
                cv.wait(lk, [&] {
                    return stopping || counter == 0;
                });
            }
//            std::cout << "Worker:" << id << " done" << std::endl;
        }

        void stop(int id) {
            std::unique_lock<std::mutex> lk(mtx);
            stopping = true;
            cv.notify_all();
        }
    };

    int nthreads;
    Barrier start_barrier, end_barrier;
    bool stopping = false;
    std::function<void(size_t, size_t)> task = nullptr;
    size_t start, end;

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
    ParallelManager(int nthreads)
        : nthreads(nthreads < 1 ? std::thread::hardware_concurrency() : nthreads),
          start_barrier(nthreads + 1), end_barrier(nthreads + 1)
    {
        for (int i = 0; i < nthreads; i++)
            std::thread(Worker(this, i)).detach();
    }

    ~ParallelManager() {
        stopping = true;
        start_barrier.stop(-1);
        end_barrier.stop(-1);
    }

    void run(size_t s, size_t e, std::function<void(size_t, size_t)> t)
    {
        start = s;
        end = e;
        task = t;
        start_barrier.wait(-1); // start all workers
        end_barrier.wait(-1); // wait for all workers to complete
        task = nullptr;
    }

    int get_parallelism() const {
        return nthreads;
    }

    friend class Worker;
};

