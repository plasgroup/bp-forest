#ifndef __EMULATION_HPP__
#define __EMULATION_HPP__

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <map>

#include "node_defs.hpp"
extern "C" {
#include "common.h"
}

#define EMU_MAX_DPUS 3000
#define EMU_DPUS_IN_RANK 64
#define EMU_MULTI_THREAD 16 /* nr worker threads */

#ifdef EMU_MULTI_THREAD

#include <thread>
#include <condition_variable>
#include <mutex>
#include <queue>

class Emulation;

class EmulatorWorkerManager {
    std::thread* threads[EMU_MULTI_THREAD];
    bool stop;
    std::condition_variable cond;
    std::mutex mtx;
    std::queue<Emulation*> queue;
    int nr_running;
    std::condition_variable  done_cond;

public:
    EmulatorWorkerManager()
    {
        for (int i = 0; i < EMU_MULTI_THREAD; i++)
            threads[i] = new std::thread([&]{this->run();});
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

class Emulation {
    friend class EmulatorWorkerManager;

    struct MRAM {
        uint64_t task_no;
        int end_idx[NR_SEATS_IN_DPU];
        each_request_t request_buffer[MAX_REQ_NUM_IN_A_DPU];
        merge_info_t merge_info;
        each_get_result_t result[MAX_REQ_NUM_IN_A_DPU];
        split_info_t split_result[NR_SEATS_IN_DPU];
        int num_kvpairs_in_seat[NR_SEATS_IN_DPU];
        uint64_t tree_transfer_num;
        KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
        dpu_init_param_t dpu_init_param[NR_SEATS_IN_DPU];
    } mram;

    std::map<key_int64_t, value_ptr_t> subtree[NR_SEATS_IN_DPU];
    bool in_use[NR_SEATS_IN_DPU];
    uint32_t dpu_id;

public:
    void init(uint32_t id)
    {
        dpu_id = id;
        for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
            in_use[i] = false;
            subtree[i].clear();
        }
    }

    void* get_addr_of_symbol(const char* symbol)
    {

#define MRAM_SYMBOL(S) do {          \
        if (strcmp(symbol, #S) == 0) \
            return &mram.S;          \
} while (0)

        MRAM_SYMBOL(task_no);
        MRAM_SYMBOL(end_idx);
        MRAM_SYMBOL(request_buffer);
        MRAM_SYMBOL(merge_info);
        MRAM_SYMBOL(result);
        MRAM_SYMBOL(split_result);
        MRAM_SYMBOL(num_kvpairs_in_seat);
        MRAM_SYMBOL(tree_transfer_num);
        MRAM_SYMBOL(tree_transfer_buffer);
        MRAM_SYMBOL(dpu_init_param);

#undef MRAM_SYMBOL

        abort();
        return NULL;
    }

    void execute()
    {
#ifdef EMU_MULTI_THREAD
        emu_threads.add_work(this);
#else /* EMU_MULTI_THREAD */
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
        switch (TASK_GET_ID(mram.task_no)) {
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
        case TASK_FROM:
            task_from(TASK_GET_OPERAND(mram.task_no));
            break;
        case TASK_TO:
            task_to(TASK_GET_OPERAND(mram.task_no));
            break;
        default:
            abort();
        }
    }

    /* counterpert of Cabin_allocate_seat */
    seat_id_t allocate_seat(seat_id_t seat_id)
    {
        if (seat_id == INVALID_SEAT_ID) {
            for (seat_id_t i = 0; i < NR_SEATS_IN_DPU; i++)
                if (!in_use[i]) {
                    in_use[i] = true;
                    return i;
                }
            abort();
            return INVALID_SEAT_ID;
        } else {
            assert(!in_use[seat_id]);
            in_use[seat_id] = true;
            return seat_id;
        }
    }

    /* counterpart of Cabin_release_seat */
    void release_seat(seat_id_t seat_id)
    {
        assert(in_use[seat_id]);
        in_use[seat_id] = false;
        mram.num_kvpairs_in_seat[seat_id] = 0;
    }

    int serialize(seat_id_t seat_id, KVPair buf[])
    {
        assert(in_use[seat_id]);
        int n = 0;
        for (auto x: subtree[seat_id]) {
            buf[n].key = x.first;
            buf[n].value = x.second;
            n++;
        }
        return n;
    }

    void deserialize(seat_id_t seat_id, KVPair buf[], int start, int n)
    {
        assert(in_use[seat_id]);
        assert(subtree[seat_id].size() == 0);
        for (int i = start; i < start + n; i++) {
            key_int64_t key = buf[i].key;
            value_ptr_t val = buf[i].value;
            assert(subtree[seat_id].find(key) == subtree[seat_id].end());
            subtree[seat_id].insert(std::make_pair(key, val));
            mram.num_kvpairs_in_seat[seat_id]++;
        }
    }

    void split_tree(KVPair buf[], int n, split_info_t result[])
    {
        int num_trees = (n + NR_ELEMS_AFTER_SPLIT - 1) / NR_ELEMS_AFTER_SPLIT;
        assert(num_trees <= MAX_NUM_SPLIT);
        for (int i = 0; i < num_trees; i++) {
            int start = n * i / num_trees;
            int end = n * (i + 1) / num_trees;
            seat_id_t seat_id = allocate_seat(INVALID_SEAT_ID);
            deserialize(seat_id, buf, start, end - start);
            result->num_elems[i] = end - start;
            result->new_tree_index[i] = seat_id;
        }
        for (int i = 0; i < num_trees; i++) {
            int end = n * (i + 1) / num_trees - 1;
            result->split_key[i] = buf[end].key;
        }
        result->num_split = num_trees;
    }

    void split()
    {
        for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
            assert(subtree[i].size() == mram.num_kvpairs_in_seat[i]);
            assert(in_use[i] || mram.num_kvpairs_in_seat[i] == 0);
            if (in_use[i]) {
                int n = mram.num_kvpairs_in_seat[i];
                if (n > SPLIT_THRESHOLD) {
                    serialize(i, mram.tree_transfer_buffer);
                    subtree[i].clear();
                    release_seat(i);
                    split_tree(mram.tree_transfer_buffer, n, &mram.split_result[i]);
                }
            }
        }
    }

    void task_init()
    {
        for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
            mram.num_kvpairs_in_seat[i] = 0;
            in_use[i] = false;
        }
        for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
            dpu_init_param_t& param = mram.dpu_init_param[i];
            if (param.use != 0) {
                allocate_seat(i);
                if (param.end_inclusive < param.start)
                    continue;
                key_int64_t k = param.start;
                while (true) {
                    value_ptr_t v = k;
                    subtree[i].insert(std::make_pair(k, v));
                    mram.num_kvpairs_in_seat[i]++;
                    if (param.end_inclusive - k < param.interval)
                        break;
                    k += param.interval;
                }
            }
        }
    }

    void task_insert()
    {
        /* sanity check */
        assert(mram.end_idx[0] >= 0);
        assert(mram.end_idx[0] == 0 || in_use[0]);
        for (int i = 1; i < NR_SEATS_IN_DPU; i++) {
            assert(mram.end_idx[i - 1] <= mram.end_idx[i]);
            assert(mram.end_idx[i - 1] == mram.end_idx[i] || in_use[i]);
        }

        /* insert */
        for (int i = 0, j = 0; i < NR_SEATS_IN_DPU; i++) {
            auto& t = subtree[i];
            for (; j < mram.end_idx[i]; j++) {
                key_int64_t key = mram.request_buffer[j].key;
                value_ptr_t val = mram.request_buffer[j].write_val_ptr;
                if (t.find(key) == t.end()) {
                    t.insert(std::make_pair(key, val));
                    mram.num_kvpairs_in_seat[i]++;
                } else
                    t[key] = val;
                assert(t.size() == mram.num_kvpairs_in_seat[i]);
            }
        }
        
        split();
    }

    void task_get()
    {
        /* sanity check */
        assert(mram.end_idx[0] >= 0);
        assert(mram.end_idx[0] == 0 || in_use[0]);
        for (int i = 1; i < NR_SEATS_IN_DPU; i++) {
            assert(mram.end_idx[i - 1] <= mram.end_idx[i]);
            assert(mram.end_idx[i - 1] == mram.end_idx[i] || in_use[i]);
        }

        for (int i = 0, j = 0; i < NR_SEATS_IN_DPU; i++)
            for (; j < mram.end_idx[i]; j++) {
                key_int64_t key = mram.request_buffer[j].key;
                auto it = subtree[i].lower_bound(key);
                if (it != subtree[i].end() && it->first == key)
                    mram.result[j].get_result = subtree[i].at(key);
                else
                    mram.result[j].get_result = 0;
            }
    }

    void task_from(seat_id_t seat_id)
    {
        assert(in_use[seat_id]);
        int n = serialize(seat_id, mram.tree_transfer_buffer);
        mram.tree_transfer_num = n;
        subtree[seat_id].clear();
        release_seat(seat_id);
    }

    void task_to(seat_id_t seat_id)
    {
        allocate_seat(seat_id);
        deserialize(seat_id, mram.tree_transfer_buffer, 0, mram.tree_transfer_num);
        mram.num_kvpairs_in_seat[seat_id] = mram.tree_transfer_num;
    }

};

#ifdef EMU_MULTI_THREAD
void
EmulatorWorkerManager::execute(Emulation* emu)
{
    emu->do_execute();
}
#endif /* EMU_MULTI_THREAD */

#endif /* __EMULATION_HPP__ */