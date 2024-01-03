#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <cassert>
#include <algorithm>
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
#include "upmem.hpp"
#include "utils.hpp"
#include "statistics.hpp"

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
float recieve_result_time = 0;
float merge_time;
float batch_time = 0;
float total_preprocess_time = 0;
float total_preprocess_time1 = 0;
float total_preprocess_time2 = 0;
float total_migration_plan_time = 0;
float total_migration_time = 0;
float total_send_time = 0;
float total_execution_time = 0;
float total_recieve_result_time = 0;
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

static void print_merge_info();
static void print_subtree_size(HostTree* host_tree);
static void print_nr_queries(BatchCtx* batch_ctx, Migration* mig);

struct Option {
    void parse(int argc, char* argv[]) {
        cmdline::parser a;
        a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, NUM_REQUESTS_PER_BATCH * DEFAULT_NR_BATCHES);
        a.add<std::string>("zipfianconst", 'a', "zipfian consttant", false, "0.99");
        a.add<int>("migration_num", 'm', "migration_num per batch", false, 5);
        a.add<std::string>("directory", 'd', "execution directory, offset from bp-forest directory. ex)bp-forest-exp", false, ".");
        a.add("simulator", 's', "if declared, the binary for simulator is used");
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert", false, "get");
        a.add<std::string>("print-load", 'q', "print number of queries sent for each seat", false, "");
        a.add<std::string>("print-subtree-size", 'e', "print number of elements for each seat", false, "");
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
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
        }
#ifdef HOST_ONLY
        dpu_binary = NULL;
#else /* HOST_ONLY */
        std::string db = a.get<std::string>("directory");
        if (is_simulator)
            db += "/build/dpu/dpu_program_simulator";
        else
            db += "/build/dpu/dpu_program_UPMEM";
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
        OP_TYPE_INSERT
    } op_type;
    bool print_load;
    std::pair<int, int> print_load_rc;
    bool print_subtree_size;
    std::pair<int, int> print_subtree_size_rc;
} opt;

#ifdef DEBUG_ON
void check_results(dpu_results_t* dpu_results, int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1])
{
    for (uint32_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (seat_id_t seat = 0; seat < NR_SEATS_IN_DPU; seat++) {
            for (int index = seat == 0 ? 0 : key_index[dpu][seat - 1]; index < key_index[dpu][seat]; index++) {
                key_int64_t key = dpu_requests[dpu].requests[index].key;
                auto it = verify_db.lower_bound(key);
                if (it == verify_db.end() || it->first != key)
                    assert(dpu_results[dpu].results[index].get_result == 0);
                else
                    assert(dpu_results[dpu].results[index].get_result == it->second);
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
    for (auto& it: tree->key_to_tree_map) {
        key_int64_t max_in_range = it.first;
        seat_addr_t& sa = it.second;
        dpu_init_param_t& param = dpu_init_param[sa.dpu][sa.seat];
        param.use = 1;
        param.start = (min_in_range + interval - 1) / interval * interval;
        param.end_inclusive = max_in_range;
        param.interval = interval;
        min_in_range = max_in_range + 1;
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
                host_tree->inv_map_del(old_sa); // TODO: insearted this line. correct?
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

int do_one_batch(const uint64_t task, int batch_num, int migrations_per_batch, uint64_t& total_num_keys, const int max_key_num, std::ifstream& file_input, HostTree* host_tree, BatchCtx& batch_ctx)
{
    struct timeval start, end;

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
    int num_keys_batch;
    if (max_key_num - total_num_keys >= NUM_REQUESTS_PER_BATCH) {
        num_keys_batch = NUM_REQUESTS_PER_BATCH;
    } else {
        num_keys_batch = max_key_num - total_num_keys;
    }
    if (num_keys_batch == 0) {
        return 0;
    }
    file_input.read(reinterpret_cast<char*>(batch_keys), sizeof(batch_keys) * num_keys_batch);
    num_keys_batch = file_input.tellg() / sizeof(key_int64_t) - total_num_keys;
    
    gettimeofday(&start, NULL);
    /* 1. count number of queries for each DPU, tree */
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
    gettimeofday(&end, NULL);
    preprocess_time1 = time_diff(&start, &end);

    /* 2. migration planning */
    Migration migration_plan(host_tree);
    gettimeofday(&start, NULL);
    migration_plan.migration_plan_memory_balancing();
    migration_plan.migration_plan_query_balancing(batch_ctx, num_migration);
    gettimeofday(&end, NULL);
    migration_plan_time = time_diff(&start, &end);

    /* 3. execute migration according to migration_plan */
    gettimeofday(&start, NULL);
    migration_plan.execute();
    host_tree->apply_migration(&migration_plan);
    gettimeofday(&end, NULL);
    migration_time = time_diff(&start, &end);
    //migration_plan.print_plan();

    // for (int i = 0; i < NR_DPUS; i++) {
    //     printf("after migration: DPU #%d has %d queries\n", i, num_keys_for_DPU[i]);
    // }
    // PRINT_LOG_ONE_DPU(0);
    gettimeofday(&start, NULL);
    /* 4. prepare requests to send to DPUs */
    /* 4.1 key_index (starting index for queries to the j-th seat of the i-th DPU) */
    for (uint32_t i = 0; i < NR_DPUS; i++) {
        for (seat_id_t j = 0; j <= NR_SEATS_IN_DPU; j++) {
            batch_ctx.key_index[i][j] = 0;
        }
    }

    for (uint32_t i = 0; i < NR_DPUS; i++) {
        for (seat_id_t j = 1; j <= NR_SEATS_IN_DPU; j++) {
            batch_ctx.key_index[i][j] = batch_ctx.key_index[i][j - 1] + migration_plan.get_num_queries_for_source(batch_ctx, i, j - 1);
            //printf("key_index[%d][%d] = %d, num_queries_for_source[%d][%d] = %d\n", i, j - 1, batch_ctx.key_index[i][j - 1], i, j - 1, migration_plan.get_num_queries_for_source(batch_ctx, i, j - 1));
        }
    }

    /* 4.2. make requests to send to DPUs*/
    switch(task) {
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
    default:
        abort();
    }

#ifdef PRINT_DEBUG
    print_nr_queries(&batch_ctx, &migration_plan);
#endif /* PRINT_DEBUG */

    /* count the number of requests for each DPU, determine the send size */
    for (uint32_t dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        /* send size: maximum number of requests to a DPU */
        if (batch_ctx.send_size < batch_ctx.key_index[dpu_i][NR_SEATS_IN_DPU])
            batch_ctx.send_size = batch_ctx.key_index[dpu_i][NR_SEATS_IN_DPU];
    }
#ifdef PRINT_DEBUG
// for (key_int64_t x : num_keys) {
//     std::cout << x << std::endl;
// }
#endif
    gettimeofday(&end, NULL);
    preprocess_time2 = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_PER_BATCH, upmem_get_nr_dpus());
#endif
    /* 5. query deliver + 6. DPU query execution */
    upmem_send_task(task, batch_ctx, &send_time, &execution_time);

    /* 7. recieve results (and update CPU structs) */
    
    gettimeofday(&start, NULL);
    upmem_recieve_num_kvpairs(host_tree, NULL);
    if (task == TASK_INSERT) {
        upmem_recieve_split_info(NULL);
        update_cpu_struct(host_tree);
    }
    if (task == TASK_GET) {
        upmem_receive_results(batch_ctx, NULL);
#ifdef DEBUG_ON
        check_results(dpu_results, batch_ctx.key_index);
#endif /* DEBUG_ON */
    }
    gettimeofday(&end, NULL);
    recieve_result_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    print_subtree_size(host_tree);
#endif /* PRINT_DEBUG */

    /* 8. merge small subtrees in DPU*/
    gettimeofday(&start, NULL);
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
    gettimeofday(&end, NULL);
    merge_time = time_diff(&start, &end);

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
    printf("zipfian_const, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time, execution_time, recieve_result_time, merge_time, batch_time, throughput\n");
    while (total_num_keys < opt.nr_total_queries) {
        BatchCtx batch_ctx;
        switch (opt.op_type) {
        case Option::OP_TYPE_GET:
            num_keys = do_one_batch(TASK_GET, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, file_input, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_INSERT:
            num_keys = do_one_batch(TASK_INSERT, batch_num, opt.nr_migrations_per_batch, total_num_keys, opt.nr_total_queries, file_input, host_tree, batch_ctx);
            break;
        default:
            abort();
        }
        total_num_keys += num_keys;
        batch_num++;
        batch_time = preprocess_time1 + preprocess_time2 + migration_plan_time + migration_time + send_time + execution_time + recieve_result_time + merge_time;
        total_preprocess_time1 += preprocess_time1;
        total_preprocess_time2 += preprocess_time2;
        total_migration_plan_time += migration_plan_time;
        total_migration_time += migration_time;
        total_send_time += send_time;
        total_execution_time += execution_time;
        total_recieve_result_time += recieve_result_time;
        total_merge_time += merge_time;
        total_batch_time += batch_time;
        double throughput = num_keys / batch_time;
        printf("%.2f, %d, %d, %d, %d, %d, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            opt.zipfian_const, NR_DPUS, NR_TASKLETS, batch_num,
            num_keys, batch_ctx.send_size, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time,
            execution_time, recieve_result_time, merge_time, batch_time, throughput);
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
        total_execution_time, total_recieve_result_time, total_merge_time, total_batch_time, throughput);

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print(stdout);
#endif /* MEASURE_XFER_BYTES */

    upmem_release();
    delete host_tree;
    return 0;
}

void
HostTree::apply_migration(Migration* m)
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