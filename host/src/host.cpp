#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "cmdline.h"
#include "host_data_structures.hpp"
#include "migration.hpp"
#include <cassert>
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#include "node_defs.hpp"
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

#include "common.h"

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define NUM_TOTAL_INIT_TREES (NR_DPUS * NUM_INIT_TREES_IN_DPU)
#ifndef NUM_INIT_REQS
#define NUM_INIT_REQS (1000 * NUM_TOTAL_INIT_TREES)
#endif
#define GET_AND_PRINT_TIME(CODES, LABEL) \
    gettimeofday(&start, NULL);          \
    CODES                                \
    gettimeofday(&end, NULL);            \
    printf("time spent for %s: %0.8f sec\n", #LABEL, time_diff(&start, &end));
#define PRINT_LOG_ALL_DPUS                     \
    DPU_FOREACH(set, dpu, each_dpu)            \
    {                                          \
        DPU_ASSERT(dpu_log_read(dpu, stdout)); \
    }
#define PRINT_LOG_ONE_DPU(i)                       \
    DPU_FOREACH(set, dpu, each_dpu)                \
    {                                              \
        if (each_dpu == i) {                       \
            DPU_ASSERT(dpu_log_read(dpu, stdout)); \
        }                                          \
    }

constexpr key_int64_t RANGE = std::numeric_limits<uint64_t>::max() / (NUM_TOTAL_INIT_TREES);

/* For working in some functions */
dpu_id_t each_dpu;
uint64_t total_num_keys;
uint32_t nr_of_dpus;
int migrated_tree_num;
/* for stats */
struct timeval start, end, start_total, end_total;
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
float batch_time = 0;
float total_preprocess_time = 0;
float total_migration_time = 0;
float total_send_time = 0;
float total_execution_time = 0;
float total_batch_time = 0;
float init_time = 0;
/* tasks */
const uint64_t task_from = TASK_FROM;
const uint64_t task_to = TASK_TO;
const uint64_t task_init = TASK_INIT;
const uint64_t task_get = TASK_GET;
const uint64_t task_insert = TASK_INSERT;
const uint64_t task_invalid = 999 + (1ULL << 32);
/* data for communication between host and DPUs */
dpu_requests_t* dpu_requests;
dpu_results_t* dpu_results;
BPTreeNode nodes_buffer[MAX_NUM_NODES_IN_SEAT];
uint64_t nodes_num;
split_info_t split_result[NR_DPUS][NR_SEATS_IN_DPU];


float time_diff(struct timeval* start, struct timeval* end)
{
    float timediff = (end->tv_sec - start->tv_sec) + 1e-6 * (end->tv_usec - start->tv_usec);
    return timediff;
}

void send_requests(struct dpu_set_t set, struct dpu_set_t dpu, const uint64_t* task, BatchCtx& batch_ctx)
{
    DPU_ASSERT(dpu_broadcast_to(set, "task_no", 0, task, sizeof(uint64_t), DPU_XFER_DEFAULT));
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &batch_ctx.key_index[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "end_idx", 0, sizeof(int) * (NR_SEATS_IN_DPU), DPU_XFER_DEFAULT));
#ifdef PRINT_DEBUG
    // printf("[INFO at %s:%d] send_size: %ld / buffer_size: %ld\n", __FILE__, __LINE__, sizeof(each_request_t) * send_size, sizeof(each_request_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &dpu_requests[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "request_buffer", 0, sizeof(each_request_t) * batch_ctx.send_size, DPU_XFER_DEFAULT));
}

void receive_results(struct dpu_set_t set, struct dpu_set_t dpu, BatchCtx& batch_ctx)
{
#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(each_result_t) * batch_ctx.send_size, sizeof(each_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &dpu_results[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "result", 0, sizeof(each_result_t) * batch_ctx.send_size, DPU_XFER_DEFAULT));
}

#ifdef DEBUG_ON
void check_results(dpu_results_t* dpu_results, int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1])
{
    for (dpu_id_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (seat_id_t seat = 0; seat < NR_SEATS_IN_DPU; seat++) {
            for (int index = seat == 0 ? 0 : key_index[dpu][seat - 1]; index < key_index[dpu][seat]; index++) {
                assert(dpu_results[dpu].results[index].get_result == dpu_requests[dpu].requests[index].write_val_ptr);
            }
        }
    }
}
#endif

void initialize_dpus(int num_init_reqs, HostTree* tree, struct dpu_set_t set, struct dpu_set_t dpu)
{
    BatchCtx batch_ctx;
    dpu_requests = (dpu_requests_t*)malloc((NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "]heap size is not enough\n");
        return;
    }
    key_int64_t* keys = (key_int64_t*)malloc(num_init_reqs * sizeof(key_int64_t));
    key_int64_t interval = (key_int64_t)std::numeric_limits<uint64_t>::max() / num_init_reqs;

    for (int i = 0; i < num_init_reqs; i++) {
        keys[i] = interval * i;
        std::map<key_int64_t, std::pair<int, int>>::iterator it = tree->key_to_tree_map.lower_bound(keys[i]);
        if (it != tree->key_to_tree_map.end()) {
            batch_ctx.num_keys_for_tree[it->second.first][it->second.second]++;
            batch_ctx.num_keys_for_DPU[it->second.first]++;
        } else {
            printf("ERROR: the key is out of range 1\n");
        }
    }
    /* compute key_index */
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        for (seat_id_t j = 1; j <= NR_SEATS_IN_DPU; j++) {
            batch_ctx.key_index[i][j] = batch_ctx.key_index[i][j - 1] + batch_ctx.num_keys_for_tree[i][j - 1];
        }
    }
    /* make requests to send to DPUs */
    for (int i = 0; i < num_init_reqs; i++) {
        auto it = tree->key_to_tree_map.lower_bound(keys[i]);
        if (it != tree->key_to_tree_map.end()) {
            dpu_requests[it->second.first].requests[batch_ctx.key_index[it->second.first][it->second.second]].key = keys[i];
            dpu_requests[it->second.first].requests[batch_ctx.key_index[it->second.first][it->second.second]++].write_val_ptr = keys[i];
        } else {
            printf("ERROR: the key is out of range 2\n");
        }
    }

    /* count the number of requests for each DPU, determine the send size */
    for (dpu_id_t dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        /* send size: maximum number of requests to a DPU */
        if (batch_ctx.num_keys_for_DPU[dpu_i] > batch_ctx.send_size)
            batch_ctx.send_size = batch_ctx.num_keys_for_DPU[dpu_i];
    }

    /* init BPTree in DPUs */
    DPU_ASSERT(dpu_broadcast_to(set, "task_no", 0, &task_init, sizeof(uint64_t), DPU_XFER_DEFAULT));
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    dpu_sync(set);

#ifdef PRINT_DEBUG
    // DPU_FOREACH(set, dpu)
    // {
    //     DPU_ASSERT(dpu_log_read(dpu, stdout));
    // }
    // PRINT_POSITION_AND_MESSAGE(initialized bptrees);
#endif
    /* insert initial keys for each tree */
#ifdef PRINT_DEBUG
    PRINT_POSITION_AND_MESSAGE(inserting initial keys);
#endif
    send_requests(set, dpu, &task_insert, batch_ctx);
    //printf("sent reqs\n");
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    dpu_sync(set);

#ifdef PRINT_DEBUG
    PRINT_LOG_ONE_DPU(0);
#endif
#ifdef DEBUG_ON
    /* checking result of search for inserted keys */
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));
    receive_results(set, dpu);
    check_results(dpu_results, batch_ctx.key_index);
    free(dpu_results);
#endif
    gettimeofday(&end, NULL);
    init_time = time_diff(&start, &end);
    printf("DPU initialization:%0.5f\n", init_time);
#ifdef PRINT_DEBUG
    // DPU_FOREACH(set, dpu, each_dpu)
    // {
    //     if (each_dpu == 0)
    //         DPU_ASSERT(dpu_log_read(dpu, stdout));
    // }
#endif
    free(dpu_requests);
    free(keys);
    return;
}

void recieve_split_info(struct dpu_set_t set, struct dpu_set_t dpu)
{
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &split_result[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "split_result", 0,
        sizeof(split_info_t) * NR_SEATS_IN_DPU,
        DPU_XFER_DEFAULT));
}

/* update cpu structs according to results of split after insertion from DPUs */
void update_cpu_struct(HostTree* host_tree)
{
    for (dpu_id_t dpu = 0; dpu < NR_DPUS; dpu++) {
        for (seat_id_t old_tree = 0; old_tree < NR_SEATS_IN_DPU; old_tree++) {
            host_tree->num_seats_used[NR_DPUS] += split_result[dpu][old_tree].num_split;
            for (int new_tree = 0; new_tree < split_result[dpu][old_tree].num_split; new_tree++) {
                seat_id_t new_seat_id = split_result[dpu][old_tree].new_tree_index[new_tree];
                printf("split: DPU %d seat %d -> seat %d\n", dpu, old_tree, new_seat_id);
                if (new_tree != 0) {
                    key_int64_t border_key = split_result[dpu][old_tree].split_key[new_tree - 1];
                    host_tree->key_to_tree_map[border_key] = std::make_pair(dpu, new_seat_id);
                    host_tree->tree_to_key_map[dpu][new_seat_id] = border_key;
                    host_tree->tree_bitmap[dpu] |= (1 << new_seat_id);
                }
            }
        }
    }
}

/* make batch, do migration, prepare queries for dpus */
int batch_preprocess(const uint64_t* task, std::ifstream& fs, int n, uint64_t& total_num_keys, int num_migration, HostTree* host_tree, BatchCtx& batch_ctx, struct dpu_set_t set, struct dpu_set_t dpu)
{
    /* read workload file */
    key_int64_t* batch_keys = (key_int64_t*)malloc(n * sizeof(key_int64_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }
    // std::cout << "malloc batch_keys" << std::endl;
    int key_count;
    fs.read(reinterpret_cast<char*>(batch_keys), sizeof(batch_keys) * n);
    key_count = fs.tellg() / sizeof(key_int64_t) - total_num_keys;
    gettimeofday(&start, NULL);
    /* 1. count number of queries for each DPU, tree */
    for (int i = 0; i < key_count; i++) {
        //printf("i: %d, batch_keys[i]:%ld\n", i, batch_keys[i]);
        auto it = host_tree->key_to_tree_map.lower_bound(batch_keys[i]);
        if (it != host_tree->key_to_tree_map.end()) {
            batch_ctx.num_keys_for_tree[it->second.first][it->second.second]++;
        } else {
            printf("ERROR: the key is out of range 3\n");
        }
    }
    preprocess_time1 = time_diff(&start, &end);

    /* 2. migration planning */
    Migration migration_plan(host_tree);
    gettimeofday(&start, NULL);
    migration_plan.migration_plan_memory_balancing();
    // migration_plan.migration_plan_query_balancing(batch_ctx, num_migration);
    gettimeofday(&end, NULL);
    migration_plan_time = time_diff(&start, &end);

    /* 3. execute migration according to migration_plan */
    migration_plan.print_plan();
    gettimeofday(&start, NULL);
    migration_plan.execute(set, dpu);
    host_tree->apply_migration(&migration_plan);
    gettimeofday(&end, NULL);
    migration_time = time_diff(&start, &end);
    migration_plan.print_plan();

    // for (int i = 0; i < NR_DPUS; i++) {
    //     printf("after migration: DPU #%d has %d queries\n", i, num_keys_for_DPU[i]);
    // }
    gettimeofday(&start, NULL);
    /* 4. key_index(i番目のDPUのj番目の木へのクエリの開始インデックス)の作成 */
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        for (seat_id_t j = 0; j <= NR_SEATS_IN_DPU; j++) {
            batch_ctx.key_index[i][j] = 0;
        }
    }

    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        for (seat_id_t j = 1; j <= NR_SEATS_IN_DPU; j++) {
            batch_ctx.key_index[i][j] = batch_ctx.key_index[i][j - 1] + migration_plan.get_num_queries_for_source(batch_ctx, i, j - 1);
            //printf("key_index[%d][%d] = %d, num_queries_for_source[%d][%d] = %d\n", i, j - 1, batch_ctx.key_index[i][j - 1], i, j - 1, migration_plan.get_num_queries_for_source(batch_ctx, i, j - 1));
        }
    }
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        printf("key_index before (DPU %d) ", i);
        for (seat_id_t j = 0; j <= NR_SEATS_IN_DPU; j++) {
            printf("[%d]=%4d ", j, batch_ctx.key_index[i][j]);
        }
        printf("\n");
    }
    /* 5. make requests to send to DPUs*/
    for (int i = 0; i < key_count; i++) {
        auto it = host_tree->key_to_tree_map.lower_bound(batch_keys[i]);
        if (it != host_tree->key_to_tree_map.end()) {
            dpu_requests[it->second.first].requests[batch_ctx.key_index[it->second.first][it->second.second]].key = batch_keys[i];
            /* key_index is incremented here, so batch_ctx.key_index[i][j] represents
             * the first index for seat j in DPU i BEFORE this for loop, then
             * the first index for seat j+1 in DPU i AFTER this for loop. */
            dpu_requests[it->second.first].requests[batch_ctx.key_index[it->second.first][it->second.second]++].write_val_ptr = batch_keys[i];
        } else {
            PRINT_POSITION_AND_MESSAGE(ERROR
                                       : the key is out of range);
        }
    }
#ifdef PRINT_DEBUG
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        printf("before (DPU %d) ", i);
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++) {
            printf("[%d]=%4d ", j, batch_ctx.num_keys_for_tree[i][j]);
        }
        printf("\n");
        printf("after  (DPU %d) ", i);
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++) {
            printf("[%d]=%4d ", j, j == 0 ? batch_ctx.key_index[i][j] : batch_ctx.key_index[i][j] - batch_ctx.key_index[i][j - 1]);
        }
        printf("\n");
    }
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        printf("key_index after (DPU %d) ", i);
        for (seat_id_t j = 0; j <= NR_SEATS_IN_DPU; j++) {
            printf("[%d]=%4d ", j, batch_ctx.key_index[i][j]);
        }
        printf("\n");
    }
#endif
    /* count the number of requests for each DPU, determine the send size */
    for (dpu_id_t dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
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
    preprocess_time = preprocess_time1 + preprocess_time2;
    free(batch_keys);
    return key_count;
}

// void show_requests(int i)
// {
//     if (i >= NR_DPUS) {
//         printf("[invalid argment]i must be less than NR_DPUS");
//         return;
//     }
//     printf("[debug_info]DPU:%d\n", i);
//     for (seat_id_t tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
//         if (dpu_requests[i].end_idx[tree_i] != 0) {
//             printf("tree %d first req:%ld,WRITE\n", tree_i, dpu_requests[i].requests[0].key);
//         }
//     }
// }

int do_one_batch(const uint64_t* task, int batch_num, int migrations_per_batch, uint64_t& total_num_keys, const int max_key_num, std::ifstream& file_input, HostTree* host_tree, BatchCtx& batch_ctx, dpu_set_t set, dpu_set_t dpu)
{
#ifdef PRINT_DEBUG
    printf("======= batch %d =======\n", batch_num);
#endif
    dpu_requests = (dpu_requests_t*)malloc(
        (NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }
    /* preprocess */
    int num_migration;
    int num_keys;
    if (batch_num == 0) {
        num_migration = 0;  // batch 0: no migration
    } else {
        num_migration = migrations_per_batch;
    }
    if (max_key_num - total_num_keys >= NUM_REQUESTS_PER_BATCH) {
        num_keys = batch_preprocess(task, file_input, NUM_REQUESTS_PER_BATCH, total_num_keys, num_migration, host_tree, batch_ctx, set, dpu);
    } else {
        num_keys = batch_preprocess(task, file_input, (max_key_num - total_num_keys), total_num_keys, num_migration, host_tree, batch_ctx, set, dpu);
    }
    if (num_keys == 0) {
        free(dpu_requests);
        return 0;
    }
#ifdef PRINT_DEBUG
    printf("[1/4] preprocess finished %0.5fsec\n", preprocess_time);
#endif

#ifdef PRINT_DEBUG
    printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_PER_BATCH, nr_of_dpus);
#endif
    /* query deliver */
    gettimeofday(&start, NULL);
    send_requests(set, dpu, task, batch_ctx);
    gettimeofday(&end, NULL);
    send_time = time_diff(&start, &end);
#ifdef PRINT_DEBUG
    printf("[2/4] send finished %0.5fsec\n", send_time);
#endif

    /* execution */
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    dpu_sync(set);
    gettimeofday(&end, NULL);
    execution_time = time_diff(&start, &end);
#ifdef PRINT_DEBUG
    printf("[3/4]all the dpu execution finished: %0.5fsec\n", execution_time);
#endif
#ifdef PRINT_DEBUG
    PRINT_LOG_ONE_DPU(0);
    //PRINT_LOG_ALL_DPUS;
#endif

    /* postprocess */
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));
    gettimeofday(&start, NULL);
    if (*task == TASK_INSERT) {
        recieve_split_info(set, dpu);
        update_cpu_struct(host_tree);
    }
    if (*task == TASK_GET) {
        receive_results(set, dpu, batch_ctx);
#ifdef DEBUG_ON
        check_results(dpu_results, batch_ctx.key_index);
#endif
    }
    gettimeofday(&end, NULL);
    free(dpu_results);
#ifdef PRINT_DEBUG
    printf("[4/4] batch postprocess finished: %0.5fsec\n", time_diff(&start, &end));
#endif

#ifdef PRINT_DEBUG
    // PRINT_LOG_ONE_DPU(0);
#endif
    free(dpu_requests);
    PRINT_POSITION_AND_VARIABLE(num_keys, % d);
    return num_keys;
}

int main(int argc, char* argv[])
{
    printf("\n");
    /* In current implementation, bitmap word is 64 bit. So NR_SEAT_IN_DPU must not be greater than 64. */
    assert(NR_SEATS_IN_DPU <= 64);
    assert(sizeof(dpu_requests_t) == sizeof(dpu_requests[0]));
#ifdef PRINT_DEBUG
    std::cout << "NR_DPUS:" << NR_DPUS << std::endl
              << "NR_TASKLETS:" << NR_TASKLETS << std::endl
              << "NR_SEATS_PER_DPU:" << NR_SEATS_IN_DPU << std::endl
              << "NUM Trees per DPU(init):" << NUM_INIT_TREES_IN_DPU << std::endl
              << "NUM Trees per DPU(max):" << NR_SEATS_IN_DPU << std::endl;
    printf("no. of requests per batch: %ld\n", (uint64_t)NUM_REQUESTS_PER_BATCH);
#endif
    cmdline::parser a;
    a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, 1000000);
    a.add<std::string>("zipfianconst", 'a', "zipfian consttant", false, "0.99");
    a.add<int>("migration_num", 'm', "migration_num per batch", false, 5);
    a.add<std::string>("directory", 'd', "execution directory, offset from bp-forest directory. ex)bp-forest-exp", false, ".");
    a.add("simulator", 's', "if declared, the binary for simulator is used");
    a.parse_check(argc, argv);
    std::string zipfian_const = a.get<std::string>("zipfianconst");
    const int max_key_num = a.get<int>("keynum");
    std::string file_name = (a.get<std::string>("directory") + "/workload/zipf_const_" + zipfian_const + ".bin");
    std::string dpu_binary;
    if (a.exist("simulator")) {
        dpu_binary = a.get<std::string>("directory") + "/build_simulator/dpu/dpu_program";
    } else {
        dpu_binary = a.get<std::string>("directory") + "/build_UPMEM/dpu/dpu_program";
    }

#ifdef PRINT_DEBUG
    std::cout << "[INFO] zipf_const:" << zipfian_const << ", workload file:" << file_name << std::endl;
#endif
    /* allocate DPUS */
    struct dpu_set_t set, dpu;
    if (a.exist("simulator")) {
        DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set));
    } else {
        DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &set));
    }

    DPU_ASSERT(dpu_load(set, dpu_binary.c_str(), NULL));
    DPU_ASSERT(dpu_get_nr_dpus(set, &nr_of_dpus));
#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", nr_of_dpus);
#endif
    /* initialization */
    HostTree* host_tree = new HostTree(RANGE);
    int num_init_reqs = NUM_INIT_REQS;
    initialize_dpus(num_init_reqs, host_tree, set, dpu);
#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    /* load workload file */
    std::ifstream file_input(file_name, std::ios_base::binary);
    if (!file_input) {
        printf("cannot open file\n");
        return 1;
    }

    /* main routine */
    int num_keys = 0;
    int batch_num = 0;
    total_num_keys = 0;
    int migrations_per_batch = a.get<int>("migration_num");
    printf("zipfian_const, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, migration_num, preprocess_time1, preprocess_time2, preprocess_time, migration_time, send_time, execution_time, batch_time, throughput\n");
    while (total_num_keys < max_key_num) {
        BatchCtx batch_ctx;
        num_keys = do_one_batch(&task_insert, batch_num, migrations_per_batch, total_num_keys, max_key_num, file_input, host_tree, batch_ctx, set, dpu);
        total_num_keys += num_keys;
        batch_num++;
        batch_time = preprocess_time + migration_time + send_time + execution_time;
        total_preprocess_time += preprocess_time;
        total_migration_time += migration_time;
        total_send_time += send_time;
        total_execution_time += execution_time;
        total_batch_time += batch_time;
        double throughput = num_keys / batch_time;
        printf("%s, %d, %d, %d, %d, %d, %d, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            zipfian_const.c_str(), NR_DPUS, NR_TASKLETS, batch_num,
            num_keys, batch_ctx.send_size, migrated_tree_num, preprocess_time1, preprocess_time2, preprocess_time, migration_time, send_time,
            execution_time, batch_time, throughput);
    }


#ifdef PRINT_DEBUG
    printf("zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%%], send_and_execution_time, total_time, throughput\n");
#endif
    //printf("%ld,%ld,%ld\n", total_num_keys_cpu, total_num_keys_dpu, total_num_keys_cpu + total_num_keys_dpu);
    //printf("%s, %d, %d, %d, %d, %ld, %ld, %ld, %ld, %0.5f, %0.5f, %0.5f, %0.3f, %0.5f, %0.0f\n", zipfian_const.c_str(), NR_DPUS, NR_TASKLETS, NUM_BPTREE_IN_CPU, NUM_BPTREE_IN_DPU * NR_DPUS, (long int)2 * total_num_keys, 2 * total_num_keys_cpu, 2 * total_num_keys_dpu, 100 * total_num_keys_cpu / total_num_keys, send_time, cpu_time,
    //    execution_time, 100 * cpu_time / execution_time, send_and_execution_time, total_time, throughput);
    double throughput = total_num_keys / total_batch_time;
    printf("%s, %d, %d, total, %ld,, %0.5f, %0.5f, %0.5f,%0.5f,%0.5f, %0.5f, %0.5f, %0.0f\n",
        zipfian_const.c_str(), NR_DPUS, NR_TASKLETS,
        total_num_keys, total_preprocess_time, total_preprocess_time, total_preprocess_time, total_migration_time, total_send_time,
        total_execution_time, total_batch_time, throughput);
    DPU_ASSERT(dpu_free(set));
    delete host_tree;
    return 0;
}
