#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <sys/time.h>
#include <vector>
// extern "C" {
// #include "bplustree.h"
// }
#include <map>

#include "cmdline.h"
#include "common.h"
#include "host_data_structures.hpp"
#include "migration.hpp"
#include "node_defs.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "utils.hpp"

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

/* for stats */
uint64_t nb_cycles_insert[NR_DPUS];
uint64_t nb_cycles_get[NR_DPUS];
uint64_t total_cycles_insert;
float preprocess_time1;
float preprocess_time2;
float preprocess_time;
float migration_time;
float migration_plan_time;
float send_time;
float execution_time;
float receive_result_time = 0;
float merge_time;
float batch_time = 0;
float total_preprocess_time = 0;
float total_preprocess_time1 = 0;
float total_preprocess_time2 = 0;
float total_migration_plan_time = 0;
float total_migration_time = 0;
float total_send_time = 0;
float total_execution_time = 0;
float total_receive_result_time = 0;
float total_merge_time = 0;
float total_batch_time = 0;
float init_time = 0;

key_int64_t* batch_keys;

#ifdef DEBUG_ON
std::map<key_int64_t, value_ptr_t> verify_db;
#endif /* DEBUG_ON */

#ifdef MEASURE_XFER_BYTES
XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */

#ifdef PRINT_DISTRIBUTION
    extern int numofnodes[NR_DPUS][NR_SEATS_IN_DPU];
#endif /* PRINT_DISTRIBUTION */

static void print_merge_info();
static void print_subtree_size(HostTree* host_tree);
static void print_nr_queries(BatchCtx* batch_ctx, Migration* mig);

struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, NUM_REQUESTS_PER_BATCH * DEFAULT_NR_BATCHES);
        a.add<std::string>("zipfianconst", 'a', "zipfian consttant", false, "0.99");
        a.add<int>("migration_num", 'm', "migration_num per batch", false, 5);
        a.add<std::string>("directory", 'd', "execution directory, offset from bp-forest directory. ex)bp-forest-exp", false, ".");
        a.add("simulator", 's', "if declared, the binary for simulator is used");
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, succ", false, "get");
        a.add<std::string>("print-load", 'q', "print number of queries sent for each seat", false, "");
        a.add<std::string>("print-subtree-size", 'e', "print number of elements for each seat", false, "");
        a.add<std::string>("variant", 'b', "build variant", false, "");
        a.parse_check(argc, argv);

        std::string alpha = a.get<std::string>("zipfianconst");
        zipfian_const = atof(alpha.c_str());
        std::string wlf = a.get<std::string>("directory") + "/workload/zipf_const_" + alpha + ".bin";
        workload_file = strdup(wlf.c_str());
        nr_total_queries = a.get<int>("keynum");
        nr_migrations_per_batch = a.get<int>("migration_num");
        is_simulator = a.exist("simulator");
        if (a.get<std::string>("ops") == "get")
            op_type = OP_TYPE_GET;
        else if (a.get<std::string>("ops") == "insert")
            op_type = OP_TYPE_INSERT;
        else if (a.get<std::string>("ops") == "succ")
            op_type = OP_TYPE_SUCC;
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
        }
#ifdef HOST_ONLY
        dpu_binary = NULL;
#else  /* HOST_ONLY */
        std::string db = a.get<std::string>("directory");
        db += "/build/" + a.get<std::string>("variant");
        if (is_simulator)
            db += "/dpu/dpu_program_simulator";
        else
            db += "/dpu/dpu_program_UPMEM";
#ifdef PRINT_DEBUG
        printf("binary: %s\n", db.c_str());
#endif /* PRINT_DEBUG */
        dpu_binary = strdup(db.c_str());
#endif /* HOST_ONLY */

        parse_row_column(a.get<std::string>("print-load"), &print_load, &print_load_rc);
        parse_row_column(a.get<std::string>("print-subtree-size"), &print_subtree_size, &print_subtree_size_rc);
    }

    void parse_row_column(std::string arg, bool* enable, std::pair<int, int>* rc)
    {
        char* p;
        int row, col;

        *enable = false;
        if (arg.length() == 0)
            return;
        row = strtoul(arg.c_str(), &p, 10);
        if (row == -1)
            row = NR_DPUS;
        if (*p == '\0')
            col = NR_SEATS_IN_DPU;
        else {
            if (*p++ != ',')
                return;
            col = strtoul(p, &p, 10);
            if (*p != '\0')
                return;
        }
        *enable = true;
        rc->first = row < NR_DPUS ? row : NR_DPUS;
        rc->second = col < NR_SEATS_IN_DPU ? col : NR_SEATS_IN_DPU;
    }

    const char* dpu_binary;
    const char* workload_file;
    bool is_simulator;
    float zipfian_const;
    int nr_total_queries;
    int nr_migrations_per_batch;
    enum OpType {
        OP_TYPE_GET,
        OP_TYPE_INSERT,
        OP_TYPE_SUCC
    } op_type;
    bool print_load;
    std::pair<int, int> print_load_rc;
    bool print_subtree_size;
    std::pair<int, int> print_subtree_size_rc;
} opt;

#ifdef DEBUG_ON
void check_get_results(dpu_results_t* dpu_results, int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1])
{
    for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (seat_id_t seat = 0; seat < NR_SEATS_IN_DPU; seat++) {
            for (int index = seat == 0 ? 0 : key_index[dpu][seat - 1]; index < key_index[dpu][seat]; index++) {
                key_int64_t key = dpu_requests[dpu].requests[index].key;
                auto it = verify_db.lower_bound(key);
                if (it == verify_db.end() || it->first != key)
                    assert(dpu_results[dpu].get.results[index].get_result == 0);
                else
                    assert(dpu_results[dpu].get.results[index].get_result == it->second);
            }
        }
    }
}

void check_succ_results(dpu_results_t* dpu_results, int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1], HostTree* host_tree)
{
    for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (seat_id_t seat = 0; seat < NR_SEATS_IN_DPU; seat++) {
            for (int index = seat == 0 ? 0 : key_index[dpu][seat - 1]; index < key_index[dpu][seat]; index++) {
                key_int64_t key = dpu_requests[dpu].requests[index].key;
                auto it = verify_db.upper_bound(key);
                if (it == verify_db.end()) {
                    assert(dpu_results[dpu].succ.results[index].succ_val_ptr == 0);
                } else {
                    assert(dpu_results[dpu].succ.results[index].succ_key == it->first);
                    assert(dpu_results[dpu].succ.results[index].succ_val_ptr == it->second);
                }
            }
        }
    }
}
#endif

void initialize_dpus(int num_init_reqs, HostTree* tree)
{
    key_int64_t interval = (key_int64_t)std::numeric_limits<uint64_t>::max() / num_init_reqs;

    for (int i = 0; i < NR_DPUS; i++)
        for (int j = 0; j < NR_SEATS_IN_DPU; j++)
            dpu_init_param[i][j].use = 0;

    key_int64_t min_in_range = 0;
    for (auto& it : tree->key_to_tree_map) {
        key_int64_t max_in_range = it.first;
        seat_addr_t& sa = it.second;
        dpu_init_param_t& param = dpu_init_param[sa.dpu][sa.seat];
        param.use = 1;
        param.start = (min_in_range + interval - 1) / interval * interval;
        param.end_inclusive = max_in_range;
        param.interval = interval;
        min_in_range = max_in_range + 1;
#ifdef DEBUG_ON
        if (param.start <= param.end_inclusive) {
            key_int64_t k = param.start;
            while (true) {
                verify_db.insert(std::make_pair(k, k));
                if (param.end_inclusive - k < param.interval)
                    break;
                k += param.interval;
            }
        }
#endif /* DEBUG_ON */
    }

    /* init BPTree in DPUs */
    BatchCtx dummy;
    upmem_send_task(TASK_INIT, dummy, NULL, NULL);

#ifdef PRINT_DEBUG
    printf("DPU initialization:%0.5f\n", init_time);
#endif /* PRINT_DEBUG */
    return;
}

/* update cpu structs according to results of split after insertion from DPUs */
void update_cpu_struct(HostTree* host_tree)
{
    for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (seat_id_t old_tree = 0; old_tree < NR_SEATS_IN_DPU; old_tree++) {
            if (split_result[dpu][old_tree].num_split != 0) {
                seat_addr_t old_sa = seat_addr_t(dpu, old_tree);
                host_tree->key_to_tree_map.erase(host_tree->inverse(old_sa));
                host_tree->inv_map_del(old_sa);  // TODO: insearted this line. correct?
                for (int new_tree = 0; new_tree < split_result[dpu][old_tree].num_split; new_tree++) {
                    seat_id_t new_seat_id = split_result[dpu][old_tree].new_tree_index[new_tree];
                    // printf("split: DPU %d seat %d -> seat %d\n", dpu, old_tree, new_seat_id);
                    key_int64_t ub = split_result[dpu][old_tree].split_key[new_tree];
                    seat_addr_t new_sa = seat_addr_t(dpu, new_seat_id);
                    host_tree->key_to_tree_map[ub] = new_sa;
                    host_tree->inv_map_add(new_sa, ub);
                }
            }
        }
    }
}

/* update cpu structs according to merge info */
void update_cpu_struct_merge(HostTree* host_tree)
{
    /* migration plan should be applied in advance */
    for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++)
        for (seat_id_t i = 0; i < NR_SEATS_IN_DPU; i++)
            if (merge_info[dpu].merge_to[i] != INVALID_SEAT_ID)
                host_tree->remove(dpu, i);  // merge to the previous subtree
}

int prepare_batch_keys(std::ifstream& file_input, key_int64_t* const batch_keys)
{
    file_input.read(reinterpret_cast<char*>(batch_keys), sizeof(key_int64_t) * NUM_REQUESTS_PER_BATCH);
    return file_input.gcount() / sizeof(key_int64_t);
}

#ifdef HOST_MULTI_THREAD
#include <condition_variable>
#include <mutex>
#include <thread>

class PreprocessWorker
{
    key_int64_t* requests;
    int start, end;
    HostTree* host_tree;
    std::thread t;
    int count[NR_DPUS][NR_SEATS_IN_DPU];
    std::condition_variable cond;
    std::mutex mtx;
    bool finished = false;
    void (PreprocessWorker::*job)() = nullptr;

public:
    PreprocessWorker()
    {
        t = std::thread{
            [this] {
                std::unique_lock<std::mutex> lock{mtx};
                for (;;) {
                    cond.wait(lock, [&] { return finished || job; });
                    assert(!(finished && job));
                    if (job) {
                        (this->*job)();
                        job = nullptr;
                    } else {
                        assert(finished);
                        break;
                    }
                }
            }};
    }
    ~PreprocessWorker()
    {
        {
            std::unique_lock<std::mutex> lock{mtx};
            finished = true;
            cond.notify_one();
        }
        t.join();
    }

    void initialize(key_int64_t* r, int s, int e, HostTree* h)
    {
        for (int i = 0; i < NR_DPUS; i++)
            for (int j = 0; j < NR_SEATS_IN_DPU; j++)
                count[i][j] = 0;
        requests = r;
        start = s;
        end = e;
        host_tree = h;
    }

private:
    void count_requests_job()
    {
        for (int i = start; i < end; i++) {
            key_int64_t key = requests[i];
            auto it = host_tree->key_to_tree_map.lower_bound(key);
            assert(it != host_tree->key_to_tree_map.end());
            uint32_t dpu = it->second.dpu;
            seat_id_t seat = it->second.seat;
            count[dpu][seat]++;
        }
    }

public:
    void count_requests()
    {
        std::lock_guard<std::mutex> lock{mtx};
        assert(job == nullptr);
        job = &PreprocessWorker::count_requests_job;
        cond.notify_one();
    }

private:
    void fill_get_requests_job()
    {
        for (int i = start; i < end; i++) {
            key_int64_t key = requests[i];
            auto it = host_tree->key_to_tree_map.lower_bound(key);
            assert(it != host_tree->key_to_tree_map.end());
            uint32_t dpu = it->second.dpu;
            seat_id_t seat = it->second.seat;
            int index = count[dpu][seat]++;
            dpu_requests[dpu].requests[index].key = key;
        }
    }
    void fill_insert_requests_job()
    {
        for (int i = start; i < end; i++) {
            key_int64_t key = requests[i];
            auto it = host_tree->key_to_tree_map.lower_bound(key);
            assert(it != host_tree->key_to_tree_map.end());
            uint32_t dpu = it->second.dpu;
            seat_id_t seat = it->second.seat;
            int index = count[dpu][seat]++;
            dpu_requests[dpu].requests[index].key = key;
            dpu_requests[dpu].requests[index].write_val_ptr = key;
        }
    }
    void fill_succ_requests_job()
    {
        for (int i = start; i < end; i++) {
            key_int64_t key = requests[i];
            auto it = host_tree->key_to_tree_map.upper_bound(key);
            if (it != host_tree->key_to_tree_map.end()) {
                uint32_t dpu = it->second.dpu;
                seat_id_t seat = it->second.seat;
                int index = count[dpu][seat]++;
                dpu_requests[dpu].requests[index].key = key;
            }
        }
    }

public:
    void fill_requests(uint64_t task, int end_index[][NR_SEATS_IN_DPU])
    {
        for (int i = 0; i < NR_DPUS; i++)
            for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
                end_index[i][j] -= count[i][j];
                count[i][j] = end_index[i][j];
            }

        std::lock_guard<std::mutex> lock{mtx};
        assert(job == nullptr);
        switch (task) {
        case TASK_GET:
            job = &PreprocessWorker::fill_get_requests_job;
            break;
        case TASK_INSERT:
            job = &PreprocessWorker::fill_insert_requests_job;
            break;
        case TASK_SUCC:
            job = &PreprocessWorker::fill_succ_requests_job;
            break;
        default:
            abort();
        }
        cond.notify_one();
    }

    void join()
    {
        for (;;) {
            {
                std::lock_guard<std::mutex> lock{mtx};
                if (job == nullptr) {
                    break;
                }
            }
            std::this_thread::yield();
        }
    }

    void add_request_count(int acc_count[][NR_SEATS_IN_DPU])
    {
        for (int i = 0; i < NR_DPUS; i++)
            for (int j = 0; j < NR_SEATS_IN_DPU; j++)
                acc_count[i][j] += count[i][j];
    }
};

PreprocessWorker ppwk[HOST_MULTI_THREAD];
#endif /* HOST_MULTI_THREAD */

int do_one_batch(const uint64_t task, int batch_num, int migrations_per_batch, uint64_t& total_num_keys, const int max_key_num, std::ifstream& file_input, HostTree* host_tree, BatchCtx& batch_ctx)
{
#ifdef PRINT_DEBUG
    printf("======= batch %d =======\n", batch_num);
#endif /* PRINT_DEBUG */
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }
    int num_migration;

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.new_batch();
#endif /* MEASURE_XFER_BYTES */

    if (batch_num == 0) {
        num_migration = 0;  // batch 0: no migration
    } else {
        num_migration = migrations_per_batch;
    }

    /* 0. read workload file */
    const int num_keys_batch = prepare_batch_keys(file_input, batch_keys);
    if (num_keys_batch == 0) {
        return 0;
    }

    /* 1. count number of queries for each DPU, tree */
#ifdef HOST_MULTI_THREAD
        for (int i = 0; i < HOST_MULTI_THREAD; i++) {
            int start = num_keys_batch * i / HOST_MULTI_THREAD;
            int end = num_keys_batch * (i + 1) / HOST_MULTI_THREAD;
            ppwk[i].initialize(batch_keys, start, end, host_tree);
            ppwk[i].count_requests();
        }
        for (int i = 0; i < HOST_MULTI_THREAD; i++) {
            ppwk[i].join();
            ppwk[i].add_request_count(batch_ctx.num_keys_for_tree);
        }
#else  /* HOST_MULTI_THREAD */
        for (int i = 0; i < num_keys_batch; i++) {
            //printf("i: %d, batch_keys[i]:%ld\n", i, batch_keys[i]);
            auto it = host_tree->key_to_tree_map.lower_bound(batch_keys[i]);
            if (it != host_tree->key_to_tree_map.end()) {
                uint32_t dpu = it->second.dpu;
                seat_id_t seat = it->second.seat;
                batch_ctx.num_keys_for_tree[dpu][seat]++;
            } else {
                printf("ERROR: the key is out of range 3: 0x%lx\n", batch_keys[i]);
            }
        }
#endif /* HOST_MULTI_THREAD */

    /* 2. migration planning */
    Migration migration_plan(host_tree);
    migration_plan_time = measure_time([&] {
        migration_plan.migration_plan_memory_balancing();
        migration_plan.migration_plan_query_balancing(batch_ctx, num_migration);
    }).count();

    /* 3. execute migration according to migration_plan */
    migration_time = measure_time([&] {
        migration_plan.execute();
        host_tree->apply_migration(&migration_plan);
    }).count();

    /* 4. prepare requests to send to DPUs */
    preprocess_time2 = measure_time([&] {
#ifdef HOST_MULTI_THREAD
        /* 4.1 key_index
         * - *END* index of the queries to the j-th seat of the i-th DPU
         * - key_index[NR_SEATS_IN_DPU]: number of queries to the DPU
         */
        for (int i = 0; i < NR_DPUS; i++) {
            int acc = migration_plan.get_num_queries_for_source(batch_ctx, i, 0);
            batch_ctx.key_index[i][0] = acc;
            for (int j = 1; j < NR_SEATS_IN_DPU; j++) {
                acc += migration_plan.get_num_queries_for_source(batch_ctx, i, j);
                batch_ctx.key_index[i][j] = acc;
            }
            batch_ctx.key_index[i][NR_SEATS_IN_DPU] = acc;
        }
        /* 4.2. make requests to send to DPUs*/
        static int end_index[NR_DPUS][NR_SEATS_IN_DPU];
        for (int i = 0; i < NR_DPUS; i++)
            for (int j = 0; j < NR_SEATS_IN_DPU; j++)
                end_index[i][j] = batch_ctx.key_index[i][j];
        for (int i = HOST_MULTI_THREAD - 1; i >= 0; i--)
            ppwk[i].fill_requests(task, end_index);
        for (int i = HOST_MULTI_THREAD - 1; i >= 0; i--)
            ppwk[i].join();
#ifdef DEBUG_ON
        if (task == TASK_INSERT)
            for (int i = 0; i < num_keys_batch; i++)
                verify_db.insert(std::make_pair(batch_keys[i], batch_keys[i]));
#endif /* DEBUG_ON */
#else  /* HOST_MULTI_THREAD */
        /* 4.1 key_index (starting index for queries to the j-th seat of the i-th DPU) */
        for (uint32_t i = 0; i < NR_DPUS; i++) {
            batch_ctx.key_index[i][0] = 0;
            for (seat_id_t j = 1; j <= NR_SEATS_IN_DPU; j++) {
                batch_ctx.key_index[i][j] = batch_ctx.key_index[i][j - 1] + migration_plan.get_num_queries_for_source(batch_ctx, i, j - 1);
            }
        }

        /* 4.2. make requests to send to DPUs*/
        switch (task) {
        case TASK_GET:
            for (int i = 0; i < num_keys_batch; i++) {
                auto it = host_tree->key_to_tree_map.lower_bound(batch_keys[i]);
                assert(it != host_tree->key_to_tree_map.end());
                uint32_t dpu = it->second.dpu;
                seat_id_t seat = it->second.seat;
                /* key_index is incremented here, so batch_ctx.key_index[i][j] represents
                * the first index for seat j in DPU i BEFORE this for loop, then
                * the first index for seat j+1 in DPU i AFTER this for loop. */
                int index = batch_ctx.key_index[dpu][seat]++;
                each_request_t& req = dpu_requests[dpu].requests[index];
                req.key = batch_keys[i];
            }
            break;
        case TASK_INSERT:
            for (int i = 0; i < num_keys_batch; i++) {
                auto it = host_tree->key_to_tree_map.lower_bound(batch_keys[i]);
                assert(it != host_tree->key_to_tree_map.end());
                uint32_t dpu = it->second.dpu;
                seat_id_t seat = it->second.seat;
                /* key_index is incremented here, so batch_ctx.key_index[i][j] represents
                * the first index for seat j in DPU i BEFORE this for loop, then
                * the first index for seat j+1 in DPU i AFTER this for loop. */
                int index = batch_ctx.key_index[dpu][seat]++;
                each_request_t& req = dpu_requests[dpu].requests[index];
                req.key = batch_keys[i];
                req.write_val_ptr = batch_keys[i];
#ifdef DEBUG_ON
                verify_db.insert(std::make_pair(batch_keys[i], batch_keys[i]));
#endif /* DEBUG_ON */
            }
            break;
        case TASK_SUCC:
            for (int i = 0; i < num_keys_batch; i++) {
                auto it = host_tree->key_to_tree_map.upper_bound(batch_keys[i]);
                if (it != host_tree->key_to_tree_map.end()) {
                    uint32_t dpu = it->second.dpu;
                    seat_id_t seat = it->second.seat;
                    /* key_index is incremented here, so batch_ctx.key_index[i][j] represents
                    * the first index for seat j in DPU i BEFORE this for loop, then
                    * the first index for seat j+1 in DPU i AFTER this for loop. */
                    int index = batch_ctx.key_index[dpu][seat]++;
                    each_request_t& req = dpu_requests[dpu].requests[index];
                    req.key = batch_keys[i];
                }
            }
            break;
        default:
            abort();
        }
#endif /* HOST_MULTI_THREAD */

#ifdef PRINT_DEBUG
        print_nr_queries(&batch_ctx, &migration_plan);
#endif /* PRINT_DEBUG */

#ifndef RANK_ORIENTED_XFER
        /* count the number of requests for each DPU, determine the send size */
        for (uint32_t dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
            /* send size: maximum number of requests to a DPU */
            if (batch_ctx.send_size < batch_ctx.key_index[dpu_i][NR_SEATS_IN_DPU])
                batch_ctx.send_size = batch_ctx.key_index[dpu_i][NR_SEATS_IN_DPU];
        }
#endif /* RANK_ORIENTED_XFER */
    }).count();

    /* 5. query deliver + 6. DPU query execution */
    upmem_send_task(task, batch_ctx, &send_time, &execution_time);

    /* 7. receive results (and update CPU structs) */
    receive_result_time = measure_time([&] {
        upmem_receive_num_kvpairs(host_tree, NULL);
        if (task == TASK_INSERT) {
            upmem_receive_split_info(NULL);
            update_cpu_struct(host_tree);
        }
        if (task == TASK_GET) {
            upmem_receive_get_results(batch_ctx, NULL);
#ifdef DEBUG_ON
            check_get_results(dpu_results, batch_ctx.key_index);
#endif /* DEBUG_ON */
        }
        if (task == TASK_SUCC) {
            upmem_receive_succ_results(batch_ctx, NULL);
#ifdef DEBUG_ON
            check_succ_results(dpu_results, batch_ctx.key_index, host_tree);
#endif /* DEBUG_ON */
        }
    }).count();

#ifdef PRINT_DISTRIBUTION
    upmem_receive_numofnodes();
    int max_num_nodes_tree = 0;
    int max_num_elems_in_dpu = 0;
    for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++) {
        if (batch_ctx.send_size < batch_ctx.key_index[dpu][NR_SEATS_IN_DPU])
            batch_ctx.send_size = batch_ctx.key_index[dpu][NR_SEATS_IN_DPU];
        int nnodes_in_dpu = 0;
        int num_elems_in_dpu = 0;
        for (seat_id_t seat = 0; seat < NR_SEATS_IN_DPU; seat++) {
            if (numofnodes[dpu][seat] > max_num_nodes_tree)
                max_num_nodes_tree = numofnodes[dpu][seat];
            nnodes_in_dpu += numofnodes[dpu][seat];
            num_elems_in_dpu += host_tree->num_kvpairs[dpu][seat];
        }
        if (max_num_elems_in_dpu < num_elems_in_dpu)
            max_num_elems_in_dpu = num_elems_in_dpu;
        printf("%d, %d, %d, %d, %d\n", batch_num, dpu, batch_ctx.key_index[dpu][NR_SEATS_IN_DPU], num_elems_in_dpu, nnodes_in_dpu);
    }
    printf("%d, -1, %d, %d, %d\n", batch_num, batch_ctx.send_size, max_num_elems_in_dpu, max_num_nodes_tree);
#endif /* PRINT_DISTRIBUTION */

    /* 8. merge small subtrees in DPU*/
    merge_time = measure_time([&] {
#ifdef MERGE
        for (uint32_t i = 0; i < NR_DPUS; i++)
            std::fill(&merge_info[i].merge_to[0], &merge_info[i].merge_to[NR_SEATS_IN_DPU], INVALID_SEAT_ID);
        Migration migration_plan_for_merge(host_tree);
        migration_plan_for_merge.migration_plan_for_merge(host_tree, merge_info);
        // migration_plan_for_merge.print_plan();
        // print_merge_info();
        migration_plan_for_merge.execute();
        host_tree->apply_migration(&migration_plan_for_merge);
        update_cpu_struct_merge(host_tree);
        upmem_send_task(TASK_MERGE, batch_ctx, NULL, NULL);
#endif /* MERGE */
    }).count();

    return num_keys_batch;
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    /* In current implementation, bitmap word is 64 bit. So NR_SEAT_IN_DPU must not be greater than 64. */
    assert(NR_SEATS_IN_DPU <= 64);
    assert(sizeof(dpu_requests_t) == sizeof(dpu_requests[0]));
#ifdef PRINT_DEBUG
    std::cout << "NR_DPUS:" << NR_DPUS << std::endl
              << "NR_TASKLETS:" << NR_TASKLETS << std::endl
              << "NR_SEATS_PER_DPU:" << NR_SEATS_IN_DPU << std::endl
              << "seats per DPU (init):" << NR_INITIAL_TREES_IN_DPU << std::endl
              << "seats per DPU (max):" << NR_SEATS_IN_DPU << std::endl
              << "requests per batch:" << NUM_REQUESTS_PER_BATCH << std::endl
              << "init elements in total:" << NUM_INIT_REQS << std::endl
              << "init elements per DPU:" << (NUM_INIT_REQS / NR_DPUS) << std::endl
              << "size of BPTREENode:" << sizeof(BPTreeNode) << std::endl
              << "MAX_NUM_NODES_IN_SEAT:" << MAX_NUM_NODES_IN_SEAT << std::endl
              << "estm. max elems in seat:" << (MAX_NUM_NODES_IN_SEAT * MAX_CHILD) << std::endl;
              
#endif

    upmem_init(opt.dpu_binary, opt.is_simulator);

    int keys_array_size = NUM_INIT_REQS > NUM_REQUESTS_PER_BATCH ? NUM_INIT_REQS : NUM_REQUESTS_PER_BATCH;
    batch_keys = (key_int64_t*)malloc(keys_array_size * sizeof(key_int64_t));

    /* initialization */
    HostTree* host_tree = new HostTree(NR_INITIAL_TREES_IN_DPU);
    int num_init_reqs = NUM_INIT_REQS;
    initialize_dpus(num_init_reqs, host_tree);
#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    /* load workload file */
    std::ifstream file_input(opt.workload_file, std::ios_base::binary);
    if (!file_input) {
        printf("cannot open file\n");
        return 1;
    }

    /* main routine */
    int num_keys = 0;
    int batch_num = 0;
    uint64_t total_num_keys = 0;
#ifdef PRINT_DISTRIBUTION
    printf("batch, DPU, nqueries, nkvpairs, nnodes\n");
#endif /* PRINT_DISTRIBUTION */
#ifndef PRINT_DISTRIBUTION
    printf("zipfian_const, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time, execution_time, receive_result_time, merge_time, batch_time, throughput\n");
#endif /* PRINT_DISTRIBUTION */
    while (total_num_keys < opt.nr_total_queries) {
        BatchCtx batch_ctx;
        switch (opt.op_type) {
        case Option::OP_TYPE_GET:
            num_keys = do_one_batch(TASK_GET, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, file_input, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_INSERT:
            num_keys = do_one_batch(TASK_INSERT, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, file_input, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_SUCC:
            num_keys = do_one_batch(TASK_SUCC, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, file_input, host_tree, batch_ctx);
            break;
        default:
            abort();
        }
        total_num_keys += num_keys;
        batch_num++;
        batch_time = preprocess_time1 + preprocess_time2 + migration_plan_time + migration_time + send_time + execution_time + receive_result_time + merge_time;
        total_preprocess_time1 += preprocess_time1;
        total_preprocess_time2 += preprocess_time2;
        total_migration_plan_time += migration_plan_time;
        total_migration_time += migration_time;
        total_send_time += send_time;
        total_execution_time += execution_time;
        total_receive_result_time += receive_result_time;
        total_merge_time += merge_time;
        total_batch_time += batch_time;
        double throughput = num_keys / batch_time;
#ifndef PRINT_DISTRIBUTION
        printf("%.2f, %d, %d, %d, %d, %d, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            opt.zipfian_const, NR_DPUS, NR_TASKLETS, batch_num,
            num_keys, batch_ctx.send_size, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time,
            execution_time, receive_result_time, merge_time, batch_time, throughput);
#endif /* PRINT_DISTRIBUTION */
    }


#ifdef PRINT_DEBUG
    printf("zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%%], send_and_execution_time, total_time, throughput\n");
#endif
    //printf("%ld,%ld,%ld\n", total_num_keys_cpu, total_num_keys_dpu, total_num_keys_cpu + total_num_keys_dpu);
    //printf("%s, %d, %d, %d, %d, %ld, %ld, %ld, %ld, %0.5f, %0.5f, %0.5f, %0.3f, %0.5f, %0.0f\n", zipfian_const.c_str(), NR_DPUS, NR_TASKLETS, NUM_BPTREE_IN_CPU, NUM_BPTREE_IN_DPU * NR_DPUS, (long int)2 * total_num_keys, 2 * total_num_keys_cpu, 2 * total_num_keys_dpu, 100 * total_num_keys_cpu / total_num_keys, send_time, cpu_time,
    //    execution_time, 100 * cpu_time / execution_time, send_and_execution_time, total_time, throughput);
    double throughput = total_num_keys / total_batch_time;

#ifndef PRINT_DISTRIBUTION
    printf("%.2f, %d, %d, total, %ld,, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f\n",
        opt.zipfian_const, NR_DPUS, NR_TASKLETS,
        total_num_keys, total_preprocess_time1, total_preprocess_time2, total_migration_plan_time, total_migration_time, total_send_time,
        total_execution_time, total_receive_result_time, total_merge_time, total_batch_time, throughput);
#endif /* PRINT_DISTRIBUTION */

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print(stdout);
#endif /* MEASURE_XFER_BYTES */

    upmem_release();
    delete host_tree;
    return 0;
}

void HostTree::apply_migration(Migration* m)
{
    for (auto it = m->begin(); it != m->end(); ++it) {
        seat_addr_t from = (*it).first;
        seat_addr_t to = (*it).second;
        key_int64_t key = inverse(from);
        inv_map_del(from);
        inv_map_add(to, key);
        key_to_tree_map[key] = to;
    }
}

//
// Printer
//

static void print_subtree_size(HostTree* host_tree)
{
    if (!opt.print_subtree_size)
        return;
    int nr_dpus = opt.print_subtree_size_rc.first;
    int nr_seats_in_dpu = opt.print_subtree_size_rc.second;
    printf("===== subtree size =====\n");
    printf("SEAT ");
    for (seat_id_t j = 0; j < nr_seats_in_dpu; j++)
        printf(" %4d ", j);
    printf("\n");
    for (uint32_t i = 0; i < nr_dpus; i++) {
        printf("[%3d]", i);
        for (seat_id_t j = 0; j < nr_seats_in_dpu; j++)
            printf(" %4d ", host_tree->num_kvpairs[i][j]);
        printf("\n");
    }
}

static void print_merge_info()
{
    printf("===== merge info =====\n");
    for (uint32_t i = 0; i < NR_DPUS; i++) {
        printf("%2d", i);
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            printf(" %2d", merge_info[i].merge_to[j]);
        printf("\n");
    }
}

static void print_nr_queries(BatchCtx* batch_ctx, Migration* mig)
{
    if (!opt.print_load)
        return;
    int nr_dpus = opt.print_load_rc.first;
    int nr_seats_in_dpu = opt.print_load_rc.second;
    printf("===== nr queries =====\n");
    printf("SEAT ");
    for (seat_id_t j = 0; j < nr_seats_in_dpu; j++)
        printf(" %4d ", j);
    printf("\n");
    for (uint32_t i = 0; i < nr_dpus; i++) {
        printf("[%3d]", i);
        for (seat_id_t j = 0; j < nr_seats_in_dpu; j++)
            printf(" %4d ", mig->get_num_queries_for_source(*batch_ctx, i, j));
        printf("\n");
    }
}
