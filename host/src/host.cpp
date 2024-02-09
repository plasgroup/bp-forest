#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"
#include "migration.hpp"
#include "node_defs.hpp"
#include "piecewise_constant_workload.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "utils.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <pthread.h>
#include <sched.h>
#include <sys/time.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <numeric>
#include <vector>

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

#ifdef DEBUG_ON
std::map<key_int64_t, value_ptr_t> verify_db;
#endif /* DEBUG_ON */

#ifdef MEASURE_XFER_BYTES
XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */

static void print_merge_info();
static void print_num_kvpairs(HostTree* host_tree);
static void print_nr_queries(BatchCtx* batch_ctx);

struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, NUM_REQUESTS_PER_BATCH * DEFAULT_NR_BATCHES);
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
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
        printf("binary: %s\n", db.c_str());
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
void check_get_results(dpu_get_results_t dpu_get_results[], std::array<unsigned, NR_DPUS>& num_keys_for_DPU)
{
    for (dpu_id_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (unsigned index = 0; index < num_keys_for_DPU.at(dpu); index++) {
            key_int64_t key = dpu_requests[dpu][index].key;
            auto it = verify_db.lower_bound(key);
            if (it == verify_db.end() || it->first != key)
                assert(dpu_get_results[dpu][index].get_result == 0);
            else
                assert(dpu_get_results[dpu][index].get_result == it->second);
        }
    }
}

void check_succ_results(dpu_succ_results_t dpu_succ_results[], std::array<unsigned, NR_DPUS>& num_keys_for_DPU)
{
    for (dpu_id_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (unsigned index = 0; index < num_keys_for_DPU.at(dpu); index++) {
            key_int64_t key = dpu_requests[dpu][index].key;
            auto it = verify_db.upper_bound(key);
            if (it == verify_db.end()) {
                assert(dpu_succ_results[dpu][index].succ_val_ptr == 0);
            } else {
                assert(dpu_succ_results[dpu][index].succ_key == it->first);
                assert(dpu_succ_results[dpu][index].succ_val_ptr == it->second);
            }
        }
    }
}
#endif

[[nodiscard]] HostTree initialize_bpforest(PiecewiseConstantWorkloadMetadata& workload_dist)
{
    // the initial keys are: KEY_MIN + KeyInterval * {0, 1, 2, ..., NUM_INIT_REQS - 1}
    constexpr key_int64_t KeyInterval = (KEY_MAX - KEY_MIN) / NUM_INIT_REQS;
    constexpr auto NrInitTrees = NR_DPUS * NR_INITIAL_TREES_IN_DPU;

    auto initialize_subtree = [](dpu_id_t idx_dpu, seat_id_t idx_tree, key_int64_t first_key, key_int64_t last_key) {
        dpu_init_param_t& param = dpu_init_param[idx_dpu][idx_tree];
        param.use = 1;
        param.start = first_key;
        param.end_inclusive = last_key;
        param.interval = KeyInterval;
#ifdef DEBUG_ON
        for (key_int64_t k = param.start; k <= param.end_inclusive; k += param.interval) {
            verify_db.emplace(k, k);
        }
#endif /* DEBUG_ON */
    };

    [[maybe_unused]] const auto num_pieces = workload_dist.densities.size();
    const auto sum_weight = std::reduce(workload_dist.densities.cbegin(), workload_dist.densities.cend());
    const auto weight_for_each_tree = sum_weight / NrInitTrees;

    HostTree host_struct;

    key_int64_t next_key = KEY_MIN;
    size_t idx_dist_piece = 0;
    auto weight_of_the_piece = workload_dist.densities.at(idx_dist_piece),
         left_weight_in_the_piece = weight_of_the_piece;
    for (dpu_id_t idx_dpu = 0; idx_dpu < NR_DPUS; idx_dpu++) {
        const auto first_key_in_this_dpu = next_key;

        for (seat_id_t idx_tree = 0; idx_tree < NR_INITIAL_TREES_IN_DPU; idx_tree++) {
            if (idx_dpu == NR_DPUS - 1 && idx_tree == NR_INITIAL_TREES_IN_DPU - 1) {  // last tree
                constexpr key_int64_t LastKey = KeyInterval * (NUM_INIT_REQS - 1);
                assert(next_key <= LastKey);
                initialize_subtree(idx_dpu, idx_tree, next_key, LastKey);
                break;
            }

            auto weight_left = weight_for_each_tree;
            while (weight_left >= left_weight_in_the_piece) {
                weight_left -= left_weight_in_the_piece;
                idx_dist_piece++;
                assert(idx_dist_piece < num_pieces);
                weight_of_the_piece = left_weight_in_the_piece = workload_dist.densities.at(idx_dist_piece);
            }
            left_weight_in_the_piece -= weight_left;
            const auto last_key = (static_cast<key_int64_t>(
                                       static_cast<double>(workload_dist.intervals.at(idx_dist_piece + 1))
                                       - static_cast<double>(workload_dist.intervals.at(idx_dist_piece + 1) - workload_dist.intervals.at(idx_dist_piece))
                                             * left_weight_in_the_piece / weight_of_the_piece)
                                      - KEY_MIN)
                                      / KeyInterval * KeyInterval
                                  + KEY_MIN;
            assert(next_key <= last_key);

            initialize_subtree(idx_dpu, idx_tree, next_key, last_key);

            next_key = last_key + KeyInterval;
        }

        host_struct.lower_bounds.at(idx_dpu) = first_key_in_this_dpu;
        host_struct.num_kvpairs.at(idx_dpu) = (next_key - first_key_in_this_dpu) / KeyInterval;
    }

    /* init BPTree in DPUs */
    BatchCtx dummy;
    upmem_send_task(TASK_INIT, dummy, NULL, NULL);

    return host_struct;
}

// /* update cpu structs according to merge info */
// void update_cpu_struct_merge(HostTree* host_tree)
// {
//     /* migration plan should be applied in advance */
//     for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++)
//         for (seat_id_t i = 0; i < NR_SEATS_IN_DPU; i++)
//             if (merge_info[dpu].merge_to[i] != INVALID_SEAT_ID)
//                 host_tree->remove(dpu, i);  // merge to the previous subtree
// }

#ifdef HOST_MULTI_THREAD
#include <condition_variable>
#include <mutex>
#include <thread>

class PreprocessWorker
{
    key_int64_t* requests;
    unsigned start, end;
    HostTree* host_tree;
    std::thread t;
    std::array<unsigned, NR_DPUS> count;
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

    void initialize(key_int64_t* r, unsigned s, unsigned e, HostTree& h)
    {
        count.fill(0);
        requests = r;
        start = s;
        end = e;
        host_tree = &h;
    }

private:
    void count_get_requests_job()
    {
        for (unsigned i = start; i < end; i++) {
            count[host_tree->dpu_resposible_for_get_query_with(requests[i])]++;
        }
    }
    void count_insert_requests_job()
    {
        for (unsigned i = start; i < end; i++) {
            count[host_tree->dpu_resposible_for_insert_query_with(requests[i])]++;
        }
    }
    void count_pred_requests_job()
    {
        for (unsigned i = start; i < end; i++) {
            count[host_tree->dpu_resposible_for_pred_query_with(requests[i])]++;
        }
    }

public:
    void count_requests(uint64_t task)
    {
        std::lock_guard<std::mutex> lock{mtx};
        assert(job == nullptr);
        switch (task) {
        case TASK_GET:
            job = &PreprocessWorker::count_get_requests_job;
            break;
        case TASK_INSERT:
            job = &PreprocessWorker::count_insert_requests_job;
            break;
        case TASK_PRED:
            job = &PreprocessWorker::count_pred_requests_job;
            break;
        default:
            abort();
        }
        cond.notify_one();
    }

private:
    void fill_get_requests_job()
    {
        for (int i = start; i < end; i++) {
            const key_int64_t key = requests[i];
            const auto idx_dpu = host_tree->dpu_resposible_for_get_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][idx_in_buf].key = key;
        }
    }
    void fill_insert_requests_job()
    {
        for (int i = start; i < end; i++) {
            const key_int64_t key = requests[i];
            const auto idx_dpu = host_tree->dpu_resposible_for_insert_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][idx_in_buf].key = key;
            dpu_requests[idx_dpu][idx_in_buf].write_val_ptr = key;
        }
    }
    void fill_pred_requests_job()
    {
        for (int i = start; i < end; i++) {
            const key_int64_t key = requests[i];
            const auto idx_dpu = host_tree->dpu_resposible_for_pred_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][idx_in_buf].key = key;
        }
    }

public:
    void fill_requests(uint64_t task)
    {
        std::lock_guard<std::mutex> lock{mtx};
        assert(job == nullptr);
        switch (task) {
        case TASK_GET:
            job = &PreprocessWorker::fill_get_requests_job;
            break;
        case TASK_INSERT:
            job = &PreprocessWorker::fill_insert_requests_job;
            break;
        case TASK_PRED:
            job = &PreprocessWorker::fill_pred_requests_job;
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

    void set_partial_sum_of_request_counts(std::array<unsigned, NR_DPUS>& acc_count)
    {
        for (unsigned idx = 0; idx < acc_count.size(); idx++) {
            const auto orig_cnt = acc_count[idx];
            acc_count[idx] += count[idx];
            count[idx] = orig_cnt;
        }
    }
    void add_request_count(std::array<unsigned, NR_DPUS>& acc_count)
    {
        for (dpu_id_t i = 0; i < NR_DPUS; i++)
            acc_count[i] += count[i];
    }
};

PreprocessWorker ppwk[HOST_MULTI_THREAD];
#endif /* HOST_MULTI_THREAD */

int do_one_batch(const uint64_t task, int batch_num, int migrations_per_batch, uint64_t& total_num_keys, const int max_key_num, WorkloadBuffer& workload_buffer, HostTree& host_tree, BatchCtx& batch_ctx)
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

    /* 0. prepare workload */
    const auto tmp_input = workload_buffer.take(NUM_REQUESTS_PER_BATCH);
    const auto batch_keys = tmp_input.first;
    const auto num_keys_batch = tmp_input.second;
    if (num_keys_batch == 0) {
        return 0;
    }

    /* 1. count number of queries for each DPU */
    auto count_requests = [&] {
#ifdef HOST_MULTI_THREAD
        for (int i = 0; i < HOST_MULTI_THREAD; i++) {
            int start = num_keys_batch * i / HOST_MULTI_THREAD;
            int end = num_keys_batch * (i + 1) / HOST_MULTI_THREAD;
            ppwk[i].initialize(batch_keys, start, end, host_tree);
            ppwk[i].count_requests(task);
        }
        for (int i = 0; i < HOST_MULTI_THREAD; i++) {
            ppwk[i].join();
            ppwk[i].add_request_count(batch_ctx.num_keys_for_DPU);
        }
#else  /* HOST_MULTI_THREAD */
        switch (task) {
        case TASK_GET:
            for (int i = 0; i < num_keys_batch; i++) {
                batch_ctx.num_keys_for_DPU[host_tree.dpu_resposible_for_get_query_with(batch_keys[i])]++;
            }
            break;
        case TASK_INSERT:
            for (int i = 0; i < num_keys_batch; i++) {
                batch_ctx.num_keys_for_DPU[host_tree.dpu_resposible_for_insert_query_with(batch_keys[i])]++;
            }
            break;
        case TASK_PRED:
            for (int i = 0; i < num_keys_batch; i++) {
                batch_ctx.num_keys_for_DPU[host_tree.dpu_resposible_for_pred_query_with(batch_keys[i])]++;
            }
            break;
        default:
            abort();
        }
#endif /* HOST_MULTI_THREAD */
    };
    preprocess_time1 = measure_time(count_requests).count();

    /* 2. migration planning */
    Migration migration_plan;
    migration_plan_time = measure_time([&] {
        migration_plan.migration_plan_memory_balancing();
        migration_plan.migration_plan_query_balancing(batch_ctx, num_migration);
    }).count();

    /* 3. execute migration according to migration_plan */
    migration_time = measure_time([&] {
        if (migration_plan.execute(host_tree)) {
            count_requests();
        }
    }).count();

    /* 4. prepare requests to send to DPUs */
    preprocess_time2 = measure_time([&] {
#ifdef HOST_MULTI_THREAD
        std::array<unsigned, NR_DPUS> request_count_accumulator{};
        for (int i = HOST_MULTI_THREAD - 1; i >= 0; i--)
            ppwk[i].set_partial_sum_of_request_counts(request_count_accumulator);
        for (int i = HOST_MULTI_THREAD - 1; i >= 0; i--)
            ppwk[i].fill_requests(task);
        for (int i = HOST_MULTI_THREAD - 1; i >= 0; i--)
            ppwk[i].join();
#else  /* HOST_MULTI_THREAD */
        std::array<unsigned, NR_DPUS> request_count{};
        switch (task) {
        case TASK_GET:
            for (int i = 0; i < num_keys_batch; i++) {
                const key_int64_t key = batch_keys[i];
                const auto idx_dpu = host_tree.dpu_resposible_for_get_query_with(key);
                const auto idx_in_buf = request_count[idx_dpu]++;
                dpu_requests[idx_dpu][idx_in_buf].key = key;
            }
            break;
        case TASK_INSERT:
            for (int i = 0; i < num_keys_batch; i++) {
                const key_int64_t key = batch_keys[i];
                const auto idx_dpu = host_tree.dpu_resposible_for_insert_query_with(key);
                const auto idx_in_buf = request_count[idx_dpu]++;
                dpu_requests[idx_dpu][idx_in_buf].key = key;
                dpu_requests[idx_dpu][idx_in_buf].write_val_ptr = key;
            }
            break;
        case TASK_PRED:
            for (int i = 0; i < num_keys_batch; i++) {
                const key_int64_t key = batch_keys[i];
                const auto idx_dpu = host_tree.dpu_resposible_for_pred_query_with(key);
                const auto idx_in_buf = request_count[idx_dpu]++;
                dpu_requests[idx_dpu][idx_in_buf].key = key;
            }
            break;
        default:
            abort();
        }
#endif /* HOST_MULTI_THREAD */
#ifdef DEBUG_ON
        if (task == TASK_INSERT)
            for (int i = 0; i < num_keys_batch; i++)
                verify_db.emplace(batch_keys[i], batch_keys[i]);
#endif /* DEBUG_ON */

#ifdef PRINT_DEBUG
        print_nr_queries(&batch_ctx);
#endif /* PRINT_DEBUG */

#ifndef RANK_ORIENTED_XFER
        /* count the number of requests for each DPU, determine the send size */
        batch_ctx.send_size = *std::max_element(batch_ctx.num_keys_for_DPU.cbegin(), batch_ctx.num_keys_for_DPU.cend());
#endif /* RANK_ORIENTED_XFER */
    }).count();

    /* 5. query deliver + 6. DPU query execution */
    upmem_send_task(task, batch_ctx, &send_time, &execution_time);

    /* 7. receive results (and update CPU structs) */
    receive_result_time = measure_time([&] {
        upmem_receive_num_kvpairs(&host_tree, NULL);
        if (task == TASK_GET) {
            upmem_receive_get_results(batch_ctx, NULL);
#ifdef DEBUG_ON
            check_get_results(dpu_results->get, batch_ctx.num_keys_for_DPU);
#endif /* DEBUG_ON */
        }
        if (task == TASK_SUCC) {
            upmem_receive_succ_results(batch_ctx, NULL);
#ifdef DEBUG_ON
            check_succ_results(dpu_results->succ, batch_ctx.num_keys_for_DPU);
#endif /* DEBUG_ON */
        }
    }).count();

#ifdef PRINT_DEBUG
    print_num_kvpairs(&host_tree);
#endif /* PRINT_DEBUG */

    /* 8. merge small subtrees in DPU*/
    merge_time = measure_time([&] {
#ifdef MERGE
        for (uint32_t i = 0; i < NR_DPUS; i++)
            std::fill(&merge_info[i].merge_to[0], &merge_info[i].merge_to[NR_SEATS_IN_DPU], INVALID_SEAT_ID);
        Migration migration_plan_for_merge;
        // migration_plan_for_merge.migration_plan_for_merge(host_tree, merge_info);
        migration_plan_for_merge.execute(host_tree);
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
              << "MAX_NUM_NODES_IN_SEAT:" << MAX_NUM_NODES_IN_SEAT << std::endl
              << "estm. max elems in seat:" << (MAX_NUM_NODES_IN_SEAT * MAX_CHILD) << std::endl;
#endif

    upmem_init(opt.dpu_binary, opt.is_simulator);

    /* load workload file */
    PiecewiseConstantWorkload workload;
    {
        std::ifstream file_input(opt.workload_file, std::ios_base::binary);
        if (!file_input) {
            printf("cannot open file\n");
            std::quick_exit(1);
        }

        cereal::BinaryInputArchive iarchive(file_input);
        iarchive(workload);
    }

    /* initialization */
    HostTree host_tree = initialize_bpforest(workload.metadata);
#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    WorkloadBuffer workload_buffer{std::move(workload.data)};

    /* main routine */
    int num_keys = 0;
    int batch_num = 0;
    uint64_t total_num_keys = 0;
    printf("zipfian_const, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time, execution_time, receive_result_time, merge_time, batch_time, throughput\n");
    while (total_num_keys < opt.nr_total_queries) {
        BatchCtx batch_ctx;
        switch (opt.op_type) {
        case Option::OP_TYPE_GET:
            num_keys = do_one_batch(TASK_GET, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, workload_buffer, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_INSERT:
            num_keys = do_one_batch(TASK_INSERT, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, workload_buffer, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_SUCC:
            num_keys = do_one_batch(TASK_SUCC, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, workload_buffer, host_tree, batch_ctx);
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
        printf("%.2f, %d, %d, %d, %d, %d, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            opt.zipfian_const, NR_DPUS, NR_TASKLETS, batch_num,
            num_keys, batch_ctx.send_size, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time,
            execution_time, receive_result_time, merge_time, batch_time, throughput);
    }


#ifdef PRINT_DEBUG
    printf("zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%%], send_and_execution_time, total_time, throughput\n");
#endif
    //printf("%ld,%ld,%ld\n", total_num_keys_cpu, total_num_keys_dpu, total_num_keys_cpu + total_num_keys_dpu);
    //printf("%s, %d, %d, %d, %d, %ld, %ld, %ld, %ld, %0.5f, %0.5f, %0.5f, %0.3f, %0.5f, %0.0f\n", zipfian_const.c_str(), NR_DPUS, NR_TASKLETS, NUM_BPTREE_IN_CPU, NUM_BPTREE_IN_DPU * NR_DPUS, (long int)2 * total_num_keys, 2 * total_num_keys_cpu, 2 * total_num_keys_dpu, 100 * total_num_keys_cpu / total_num_keys, send_time, cpu_time,
    //    execution_time, 100 * cpu_time / execution_time, send_and_execution_time, total_time, throughput);
    double throughput = total_num_keys / total_batch_time;
    printf("%.2f, %d, %d, total, %ld,, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f\n",
        opt.zipfian_const, NR_DPUS, NR_TASKLETS,
        total_num_keys, total_preprocess_time1, total_preprocess_time2, total_migration_plan_time, total_migration_time, total_send_time,
        total_execution_time, total_receive_result_time, total_merge_time, total_batch_time, throughput);

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print(stdout);
#endif /* MEASURE_XFER_BYTES */

    upmem_release();
    return 0;
}

// void HostTree::apply_migration(Migration* m)
// {
//     for (auto it = m->begin(); it != m->end(); ++it) {
//         seat_addr_t from = (*it).first;
//         seat_addr_t to = (*it).second;
//         key_int64_t key = inverse(from);
//         inv_map_del(from);
//         inv_map_add(to, key);
//         key_to_tree_map[key] = to;
//     }
// }

//
// Printer
//

static void print_num_kvpairs(HostTree* host_tree)
{
    if (!opt.print_subtree_size)
        return;
    int nr_dpus = opt.print_subtree_size_rc.first;
    printf("===== #KV-pairs =====\n");
    for (dpu_id_t i = 0; i < nr_dpus; i++) {
        printf("DPU[%4d] %4d\n", i, host_tree->get_num_kvpairs(i));
    }
}

static void print_merge_info()
{
    printf("===== merge info =====\n");
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        printf("%2d", i);
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            printf(" %2d", merge_info[i].merge_to[j]);
        printf("\n");
    }
}

static void print_nr_queries(BatchCtx* batch_ctx)
{
    if (!opt.print_load)
        return;
    int nr_dpus = opt.print_load_rc.first;
    printf("===== nr queries =====\n");
    for (dpu_id_t i = 0; i < nr_dpus; i++) {
        printf("DPU[%4d] %4d\n", i, batch_ctx->num_keys_for_DPU[i]);
    }
}
