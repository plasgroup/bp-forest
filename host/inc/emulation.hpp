#ifndef __EMULATION_HPP__
#define __EMULATION_HPP__

#include "common.h"
#include "host_params.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <map>

#define EMU_MULTI_THREAD 16 /* nr worker threads */

#ifdef EMU_MULTI_THREAD

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

class Emulation;

class EmulatorWorkerManager
{
    bool stop;
    std::condition_variable cond;
    std::mutex mtx;
    std::queue<Emulation*> queue;
    int nr_running;
    std::condition_variable done_cond;

public:
    EmulatorWorkerManager()
    {
        for (int i = 0; i < EMU_MULTI_THREAD; i++)
            std::thread([&] { this->run(); }).detach();
        nr_running = 0;
        stop = false;
    }

    void add_work(Emulation* emu)
    {
        std::unique_lock<std::mutex> lk(mtx);
        queue.push(emu);
        cond.notify_one();
    }

    void wait_all()
    {
        std::unique_lock<std::mutex> lk(mtx);
        while (!stop && (nr_running > 0 || !queue.empty()))
            done_cond.wait(lk);
    }

    void terminate()
    {
        std::unique_lock<std::mutex> lk(mtx);
        stop = true;
        cond.notify_all();
        done_cond.notify_all();
    }

private:
    void execute(Emulation* emu);

    void run()
    {
        while (true) {
            std::unique_lock<std::mutex> lk(mtx);
            while (!stop && queue.empty())
                cond.wait(lk);
            if (stop)
                return;
            Emulation* emu = queue.front();
            queue.pop();
            nr_running++;
            lk.unlock();
            execute(emu);
            lk.lock();
            nr_running--;
            if (nr_running == 0 && queue.empty())
                done_cond.notify_all();
        }
    }
};

static EmulatorWorkerManager emu_threads;

#endif /* EMU_MULTI_THREAD */

class Emulation
{
#ifdef EMU_MULTI_THREAD
    friend class EmulatorWorkerManager;
#endif /* EMU_MULTI_THREAD */

    struct MRAM {
        uint64_t task_no;
        unsigned end_idx;
        each_request_t request_buffer[MAX_REQ_NUM_IN_A_DPU];
        dpu_results_t results;
        uint32_t num_kvpairs;
        dpu_init_param_t dpu_init_param;
        migration_ratio_param_t migration_ratio_param;
        migration_key_param_t migration_key_param;
        migration_pairs_param_t migration_pairs_param;
        key_int64_t migrated_keys[MAX_NUM_NODES_IN_SEAT * MAX_NR_PAIRS];
        value_ptr_t migrated_values[MAX_NUM_NODES_IN_SEAT * MAX_NR_PAIRS];
    };
    std::unique_ptr<MRAM> mram{new MRAM};

    std::map<key_int64_t, value_ptr_t> subtree;
    uint32_t dpu_id;

public:
    void init(uint32_t id)
    {
        dpu_id = id;
        subtree.clear();
    }

    void* get_addr_of_symbol(const char* symbol)
    {

#define MRAM_SYMBOL(S)               \
    do {                             \
        if (strcmp(symbol, #S) == 0) \
            return &mram->S;         \
    } while (0)

        MRAM_SYMBOL(task_no);
        MRAM_SYMBOL(end_idx);
        MRAM_SYMBOL(request_buffer);
        MRAM_SYMBOL(results);
        MRAM_SYMBOL(num_kvpairs);
        MRAM_SYMBOL(dpu_init_param);
        MRAM_SYMBOL(migration_ratio_param);
        MRAM_SYMBOL(migration_key_param);
        MRAM_SYMBOL(migration_pairs_param);
        MRAM_SYMBOL(migrated_keys);
        MRAM_SYMBOL(migrated_values);

#undef MRAM_SYMBOL

        std::cerr << "Emulation Error: \"" << symbol << "\" is not registered as MRAM_SYMBOL" << std::endl;

        abort();
        return NULL;
    }

    void execute()
    {
#ifdef EMU_MULTI_THREAD
        emu_threads.add_work(this);
#else  /* EMU_MULTI_THREAD */
        do_execute();
#endif /* EMU_MULTI_THREAD */
    }

    static void wait_all()
    {
#ifdef EMU_MULTI_THREAD
        emu_threads.wait_all();
#endif /* EMU_MULTI_THREAD */
    }

    static void terminate()
    {
#ifdef EMU_MULTI_THREAD
        emu_threads.terminate();
#endif /* EMU_MULTI_THREAD */
    }

private:
    void do_execute()
    {
        switch (TASK_GET_ID(mram->task_no)) {
        case TASK_INIT:
            task_init();
            break;
        case TASK_INSERT:
            task_insert();
#ifdef DEBUG_ON
            task_get();
#endif /* DEBUG_ON */
            break;
        case TASK_GET:
            task_get();
            break;
        case TASK_SUCC:
            task_succ();
            break;
        case TASK_FROM:
            task_from();
            break;
        case TASK_TO:
            task_to();
            break;
        default:
            abort();
        }
    }

    uint32_t serialize(KVPair buf[])
    {
        uint32_t n = 0;
        for (auto x : subtree) {
            buf[n].key = x.first;
            buf[n].value = x.second;
            n++;
        }
        return n;
    }

    void deserialize(KVPair buf[], unsigned start, uint32_t n)
    {
        assert(subtree.size() == 0);
        for (unsigned i = start; i < start + n; i++) {
            key_int64_t key = buf[i].key;
            value_ptr_t val = buf[i].value;
            assert(subtree.find(key) == subtree.end());
            subtree.insert(std::make_pair(key, val));
            mram->num_kvpairs++;
        }
    }

    void task_init()
    {
        subtree.clear();
        if (mram->dpu_init_param.end_inclusive >= mram->dpu_init_param.start) {
            key_int64_t k = mram->dpu_init_param.start;
            while (true) {
                value_ptr_t v = k;
                subtree.insert(std::make_pair(k, v));
                if (mram->dpu_init_param.end_inclusive - k < mram->dpu_init_param.interval)
                    break;
                k += mram->dpu_init_param.interval;
            }
        }
        mram->num_kvpairs = static_cast<uint32_t>(subtree.size());
    }

    void task_insert()
    {
        using std::begin, std::end;

        for (unsigned idx_req = 0; idx_req < mram->end_idx; idx_req++) {
            subtree.insert_or_assign(mram->request_buffer[idx_req].key, mram->request_buffer[idx_req].write_val_ptr);
        }
        mram->num_kvpairs = static_cast<uint32_t>(subtree.size());
    }

    void task_get()
    {
        using std::cbegin, std::cend;

        for (unsigned idx_req = 0; idx_req < mram->end_idx; idx_req++) {
            key_int64_t key = mram->request_buffer[idx_req].key;
            const auto it = subtree.find(key);
            mram->results.get[idx_req].key = key;
            if (it != subtree.end())
                mram->results.get[idx_req].get_result = it->second;
            else
                mram->results.get[idx_req].get_result = 0;
        }
    }

    void task_succ()
    {
        // TODO
    }

    void task_from()
    {
        migration_pairs_param_t npairs{
            static_cast<uint32_t>(mram->migration_ratio_param.left_npairs_ratio_x2147483648 * subtree.size() / 2147483648u),
            static_cast<uint32_t>(subtree.size() - (2147483648u - mram->migration_ratio_param.right_npairs_ratio_x2147483648) * subtree.size() / 2147483648u),
        };

        if (mram->migration_ratio_param.left_npairs_ratio_x2147483648 != 2147483648u) {
            mram->migration_key_param.left_delim_key = std::next(subtree.cbegin(), npairs.num_left_kvpairs)->first;
        }
        if (mram->migration_ratio_param.right_npairs_ratio_x2147483648 != 0u) {
            mram->migration_key_param.right_delim_key = std::prev(subtree.cend(), npairs.num_right_kvpairs)->first;
        }

        unsigned idx_migrated = 0;

        switch (mram->migration_ratio_param.left_npairs_ratio_x2147483648) {
        case 0u:
            break;
        case 2147483648u:
            npairs.num_left_kvpairs = static_cast<uint32_t>(subtree.size());
            for (const auto& pair : subtree) {
                mram->migrated_keys[idx_migrated] = pair.first;
                mram->migrated_values[idx_migrated] = pair.second;
                idx_migrated++;
            }
            subtree.clear();
            mram->num_kvpairs = 0;
            break;
        default:
            while (!subtree.empty() && subtree.cbegin()->first < mram->migration_key_param.left_delim_key) {
                mram->migrated_keys[idx_migrated] = subtree.cbegin()->first;
                mram->migrated_values[idx_migrated] = subtree.cbegin()->second;
                idx_migrated++;

                subtree.erase(subtree.cbegin());
            }
            npairs.num_left_kvpairs = idx_migrated;
            break;
        }

        switch (mram->migration_ratio_param.right_npairs_ratio_x2147483648) {
        case 0u:
            break;
        case 2147483648u:
            npairs.num_right_kvpairs = static_cast<uint32_t>(subtree.size());
            for (const auto& pair : subtree) {
                mram->migrated_keys[idx_migrated] = pair.first;
                mram->migrated_values[idx_migrated] = pair.second;
                idx_migrated++;
            }
            subtree.clear();
            break;
        default:
            for (auto iter = subtree.lower_bound(mram->migration_key_param.right_delim_key); iter != subtree.end();) {
                mram->migrated_keys[idx_migrated] = iter->first;
                mram->migrated_values[idx_migrated] = iter->second;
                idx_migrated++;
                subtree.erase(iter++);
            }
            npairs.num_right_kvpairs = idx_migrated - npairs.num_left_kvpairs;
            break;
        }

        mram->num_kvpairs = static_cast<uint32_t>(subtree.size());
        mram->migration_pairs_param = npairs;
    }

    void task_to()
    {
        const uint32_t num_migrated_pairs = mram->migration_pairs_param.num_left_kvpairs + mram->migration_pairs_param.num_right_kvpairs;
        for (unsigned i = 0; i < num_migrated_pairs; i++) {
            subtree.emplace(mram->migrated_keys[i], mram->migrated_values[i]);
        }
        mram->num_kvpairs = static_cast<uint32_t>(subtree.size());
    }
};

#ifdef EMU_MULTI_THREAD
inline void EmulatorWorkerManager::execute(Emulation* emu)
{
    emu->do_execute();
}
#endif /* EMU_MULTI_THREAD */

#endif /* __EMULATION_HPP__ */