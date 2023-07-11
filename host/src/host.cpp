#pragma once
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "cmdline.h"
#include <assert.h>
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>
#include <list>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <vector>
extern "C" {
#include "bplustree.h"
}

#include "common.h"

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#ifndef DPU_BINARY1
#define DPU_BINARY1 "./build/dpu/dpu_program"
#endif

#ifndef DPU_BINARY2
#define DPU_BINARY2 "./build/dpu/dpu_program_redundant"
#endif

#ifndef NUM_TOTAL_TREES
#define NUM_TOTAL_TREES (NR_DPUS * 20)
#endif

#define CPU_THRESHOLD_COEFF (15)
#define GET_AND_PRINT_TIME(CODES, LABEL) \
    gettimeofday(&start, NULL);          \
    CODES                                \
    gettimeofday(&end, NULL);            \
    printf("time spent for %s: %0.8f sec\n", #LABEL, time_diff(&start, &end));

// dpu_request_t dpu_requests[NR_DPUS];
dpu_requests_t* dpu_requests;
typedef std::vector<std::vector<each_request_t>> cpu_requests_t;
cpu_requests_t cpu_requests(MAX_NUM_BPTREE_IN_CPU);
std::list<std::pair<int, int>> global_tree_idx_to_local_tree_idx(NUM_TOTAL_TREES, std::make_pair(0, 0));

uint64_t nb_cycles_insert[NR_DPUS];
uint64_t nb_cycles_get[NR_DPUS];
uint64_t total_cycles_insert;
float send_time;
float execution_time;
float send_and_execution_time, total_time, cpu_time, migration_time;
int each_dpu;
int send_size;
uint64_t num_requests[NR_DPUS];
int num_reqs_for_cpus[MAX_NUM_BPTREE_IN_CPU];
uint64_t total_num_keys;
uint64_t total_num_keys_cpu;
uint64_t total_num_keys_dpu;
uint32_t nr_of_dpus1;
uint32_t nr_of_dpus2;
struct timeval start, end, start_total, end_total, end_cpu;
BPlusTree* bplustrees[MAX_NUM_BPTREE_IN_CPU];
pthread_t threads[MAX_NUM_BPTREE_IN_CPU];
int thread_ids[MAX_NUM_BPTREE_IN_CPU];
BPlusTree* bptrees[MAX_NUM_BPTREE_IN_CPU];
uint64_t malloced_tree_index;


constexpr key_int64_t RANGE
    = std::numeric_limits<uint64_t>::max() / (NUM_TOTAL_TREES);
uint64_t EMPTY_TASK = 0;
uint64_t INIT_TASK = 1;
uint64_t SEARCH_TASK = 10;
uint64_t INSERT_TASK = 11;
uint64_t REMOVE_TASK = 12;
uint64_t RANGE_SEARCH_TASK = 13;
uint64_t SERIALIZE_TASK = 20;
uint64_t DESERIALIZE_TASK = 21;
#ifdef GENERATE_DATA
uint64_t generate_requests()
{
    dpu_requests = (dpu_request_t*)malloc(NR_DPUS * NR_TASKLETS * sizeof(dpu_request_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "]heap size is not enough\n");
        return 0;
    }
    for (int i = 0; i < NR_DPUS * NR_TASKLETS; i++) {
        dpu_requests[i].num_req = 0;
    }
    int range = NR_ELEMS_PER_TASKLET;
    for (int i = 0; i < 10000 * NR_DPUS * NR_TASKLETS; i++) {
        // printf("%d\n",i);
        key_int64_t key = (key_int64_t)rand();
        // key_int64_t key = (key_int64_t)rand();
        int which_tasklet = key / range;
        // printf("key:%ld,DPU:%d,tasklet:%d\n", key, which_DPU, which_tasklet);
        if (which_tasklet >= NR_TASKLETS * NR_DPUS) {
            continue;
            // printf("[debug_info]tasklet remainder: key = %ld\n", key);
        }
        int idx = dpu_requests[which_tasklet].num_req;
        dpu_requests[which_tasklet].key[idx] = key;
        dpu_requests[which_tasklet].read_or_write[idx] = WRITE;
        dpu_requests[which_tasklet].write_val_ptr[idx] = key;
        // printf("[debug_info]request
        // %d,key:%ld,which_tasklet:%d,dpu_requests[which_tasklet].num_req:%d\n",i,key,which_tasklet,dpu_requests[which_tasklet].num_req);
        dpu_requests[which_tasklet].num_req++;
        if (dpu_requests[which_tasklet].num_req > NUM_REQUESTS_PER_BATCH) {
            printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
                   "] request buffer size exceeds the limit because of skew\n");
            assert(false);
        }
    }
    for (int i = 0; i < NR_DPUS * NR_TASKLETS; i++) {
        // num_requests[batch_num][i/NR_TASKLETS] +=
        // (uint64_t)dpu_requests[i].num_req; printf("[debug_info]num_req[%d][%d] =
        // %d\n", i/NR_TASKLETS,i%NR_TASKLETS,dpu_requests[i].num_req);
    }
    uint64_t ret = 0;
    for (int i = 0; i < NR_DPUS; i++) {
        // ret += num_requests[batch_num][i];
    }
    return ret;
    // printf("[debug_info]MAX_REQ_NUM_PER_TASKLET_IN_BATCH =
    // %d\n",MAX_REQ_NUM_PER_TASKLET_IN_BATCH);
}
#endif

#ifdef CYCLIC_DIST
int generate_requests_fromfile(std::ifstream& fs, int n)
{
#ifdef PRINT_DEBUG
    std::cout << "cyclic" << std::endl;
#endif
    dpu_requests = (dpu_requests_t*)malloc(
        (NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }

    /* read workload file */
    key_int64_t* batch_keys = (key_int64_t*)malloc(n * sizeof(key_int64_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }
    // std::cout << "malloc batch_keys" << std::endl;
    int key_count = 0;
    fs.read(reinterpret_cast<char*>(batch_keys), sizeof(batch_keys) * n);
    key_count = fs.tellg() / sizeof(key_int64_t) - total_num_keys;
    // std::cout << "key_count: " << key_count << std::endl;
    /* sort by which tree the requests should be processed */
    std::sort(batch_keys, batch_keys + key_count, [](auto a, auto b) { return a / RANGE < b / RANGE; });
    /* count the number of requests for each tree */
    int num_keys[NUM_TOTAL_TREES] = {};
    int num_keys_for_each_dpu[NR_DPUS] = {};
    for (int i = 0; i < key_count; i++) {
        num_keys[batch_keys[i] / RANGE]++;
    }
#ifdef PRINT_DEBUG
    int sum = 0;
    for (int i = 0; i < NUM_TOTAL_TREES; i++) {
        sum += num_keys[i];
        printf("num_keys[%d] = %d\n", i, num_keys[i]);
    }
    printf("sum:%d\n", sum);

#endif
    /* count the number of requests for each DPU, determine the send size */
    send_size = 0;
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        if (dpu_i < NR_DPUS_REDUNDANT) {
            num_keys_for_each_dpu[dpu_i]
                += num_keys[MAX_NUM_BPTREE_IN_CPU + dpu_i];
        } else {
            for (int tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
/* cyclic */
#ifdef PRINT_DEBUG
                printf("range_index for dpu_%d, tree_%d: %d\n", dpu_i, tree_i, MAX_NUM_BPTREE_IN_CPU + NR_DPUS_REDUNDANT + (dpu_i - NR_DPUS_REDUNDANT) + NR_DPUS_MULTIPLE * tree_i);
#endif
                num_keys_for_each_dpu[dpu_i]
                    += num_keys[MAX_NUM_BPTREE_IN_CPU + NR_DPUS_REDUNDANT + (dpu_i - NR_DPUS_REDUNDANT) + NR_DPUS_MULTIPLE * tree_i];
                dpu_requests[dpu_i].end_idx[tree_i] = num_keys_for_each_dpu[dpu_i];
#ifdef PRINT_DEBUG
                printf("dpu_%d's end_idx of tree%d = %d\n", dpu_i, tree_i, dpu_requests[dpu_i].end_idx[tree_i]);
#endif
            }
        }
        total_num_keys_dpu += num_keys_for_each_dpu[dpu_i];
        /* send size: maximum number of requests to a DPU */
        if (num_keys_for_each_dpu[dpu_i] > send_size)
            send_size = num_keys_for_each_dpu[dpu_i];
    }
    int current_idx = 0;
    /* make cpu requests */
    for (int cpu_i = 0; cpu_i < MAX_NUM_BPTREE_IN_CPU; cpu_i++) {
        cpu_requests.at(cpu_i).clear();
        for (int i = 0; i < num_keys[cpu_i]; i++) {
            cpu_requests.at(cpu_i).push_back({batch_keys[current_idx + i], batch_keys[current_idx + i], WRITE});
        }
        current_idx += num_keys[cpu_i];
#ifdef DEBUG_ON
        std::cout << "current idx: " << current_idx << std::endl;
#endif
        num_reqs_for_cpus[cpu_i] = num_keys[cpu_i];
        total_num_keys_cpu += num_reqs_for_cpus[cpu_i];
    }
    /* check num of requests */
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        if (num_keys_for_each_dpu[dpu_i] >= MAX_REQ_NUM_IN_A_DPU) {
            printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
                   "] request buffer size %d exceeds the limit %d because of skew\n",
                num_keys_for_each_dpu[dpu_i], MAX_REQ_NUM_IN_A_DPU);
            assert(false);
        }
    }
    /* make dpu requests: redundant */
    for (int dpu_i = 0; dpu_i < NR_DPUS_REDUNDANT; dpu_i++) {
        for (int i = 0; i < num_keys_for_each_dpu[dpu_i]; i++) {
            dpu_requests[dpu_i].requests[i].key = batch_keys[current_idx + i];
            dpu_requests[dpu_i].requests[i].write_val_ptr = batch_keys[current_idx + i];
            dpu_requests[dpu_i].requests[i].operation = WRITE;
        }
        current_idx += num_keys_for_each_dpu[dpu_i];
#ifdef DEBUG_ON
        std::cout << "current idx: " << current_idx << std::endl;
#endif
    }
    /* make dpu requests: multiple */
    for (int tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
        for (int dpu_i = NR_DPUS_REDUNDANT; dpu_i < NR_DPUS; dpu_i++) {
            for (int i = 0; i < num_keys[MAX_NUM_BPTREE_IN_CPU + NR_DPUS_REDUNDANT + (dpu_i - NR_DPUS_REDUNDANT) + NR_DPUS_MULTIPLE * tree_i]; i++) {
                int offset;
                if (tree_i == 0) {
                    offset = 0;
                } else {
                    offset = dpu_requests[dpu_i].end_idx[tree_i - 1];
                }
                dpu_requests[dpu_i].requests[offset + i].key = batch_keys[current_idx + i];
                dpu_requests[dpu_i].requests[offset + i].write_val_ptr = batch_keys[current_idx + i];
                dpu_requests[dpu_i].requests[offset + i].operation = WRITE;
            }
            current_idx += num_keys[MAX_NUM_BPTREE_IN_CPU + NR_DPUS_REDUNDANT + (dpu_i - NR_DPUS_REDUNDANT) + NR_DPUS_MULTIPLE * tree_i];
#ifdef DEBUG_ON
            std::cout << "tree_i = " << tree_i << ", dpu_i = " << dpu_i << ", current idx: " << current_idx << std::endl;
#endif
        }
    }
#ifdef PRINT_DEBUG
    // for (key_int64_t x : num_keys) {
    //     std::cout << x << std::endl;
    // }
#endif
    free(batch_keys);
    return key_count;
}
#endif

#ifndef CYCLIC_DIST
int generate_requests_fromfile(std::ifstream& fs, int n, dpu_set_t set, dpu_set_t dpu)
{
#ifdef PRINT_DEBUG
    std::cout << "not cyclic" << std::endl;
#endif
    dpu_requests = (dpu_requests_t*)malloc(
        (NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }

    /* read workload file */
    key_int64_t* batch_keys = (key_int64_t*)malloc(n * sizeof(key_int64_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }
    int key_count = 0;
    fs.read(reinterpret_cast<char*>(batch_keys), sizeof(batch_keys) * n);
    key_count = fs.tellg() / sizeof(key_int64_t) - total_num_keys;
    /* sort by which tree the requests should be processed */
    std::sort(batch_keys, batch_keys + key_count, [](auto a, auto b) { return a / RANGE < b / RANGE; });
    /* count the number of requests for each tree */
    int num_keys[NUM_TOTAL_TREES] = {};
    int num_keys_for_each_dpu[NR_DPUS] = {};
    int average_num_req_per_tree_in_batch = key_count / NUM_TOTAL_TREES;
    /* if the number of queries exceeds the threshold below, they are executed on the CPU */
    int cpu_threshold = CPU_THRESHOLD_COEFF * average_num_req_per_tree_in_batch;

    for (int i = 0; i < key_count; i++) {
        num_keys[batch_keys[i] / RANGE]++;
    }
    int count = 0;
    int count_cpu_tree = 0;
    std::pair<int, int> current_local_tree_idx = std::make_pair(0, 0);
    std::pair<int, int> local_tree_idx_before = std::make_pair(0, 0);
    int count_each_dpu_tree[NR_DPUS] = {0};
    int num_threads_for_tree[NUM_TOTAL_TREES] = {0};
    int tmp_key_count = 0;
    for (auto& pair : global_tree_idx_to_local_tree_idx) {
        local_tree_idx_before.first = pair.first;
        local_tree_idx_before.second = pair.second;
        if (num_keys[count] >= cpu_threshold) {  // CPU
            pair.first = -1;                     // CPU
            pair.second = count_cpu_tree++;
        } else if (num_keys[count] >= average_num_req_per_tree_in_batch) { /* more than average: use more than 1 tasklet(s) */
            tmp_key_count = 0;
            num_threads_for_tree[count] = num_keys[count] / average_num_req_per_tree_in_batch;
            /* exceed the DPU */
            if (current_local_tree_idx.second + num_threads_for_tree >= NR_TASKLETS) {
                pair.first = ++current_local_tree_idx.first;
                pair.second = 0;
                current_local_tree_idx.second = num_threads_for_tree[count];
            } else { /* don't exceed the DPU */
                pair.first = current_local_tree_idx.first;
                pair.second = current_local_tree_idx.second;
                current_local_tree_idx.second += num_threads_for_tree[count];
            }
        } else { /* less than average: 1 tasklet processes multiple trees (up to average num of queries in a tasklet)*/
            while (tmp_key_count <= average_num_req_per_tree_in_batch) {
                pair.first = current_local_tree_idx.first;
                pair.second = current_local_tree_idx.second;
                tmp_key_count += num_keys[count];
            }
        }
        tree_migration(local_tree_idx_before, pair, set, dpu);
        count++;
    }
#ifdef PRINT_DEBUG
    for (int i = 0; i < NUM_TOTAL_TREES; i++)
        printf("num_keys[%d] = %d\n", i, num_keys[i]);
#endif
    /* count the number of requests for each DPU, determine the send size */
    send_size = 0;
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        if (dpu_i < NR_DPUS_REDUNDANT) {
            num_keys_for_each_dpu[dpu_i]
                += num_keys[MAX_NUM_BPTREE_IN_CPU + dpu_i];
        } else {
            for (int tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
                num_keys_for_each_dpu[dpu_i]
                    += num_keys[MAX_NUM_BPTREE_IN_CPU + NR_DPUS_REDUNDANT + (dpu_i - NR_DPUS_REDUNDANT) * NUM_BPTREE_IN_DPU + tree_i];
                dpu_requests[dpu_i].end_idx[tree_i] = num_keys_for_each_dpu[dpu_i];
#ifdef PRINT_DEBUG
                printf("dpu_%d's end_idx of tree%d = %d\n", dpu_i, tree_i, dpu_requests[dpu_i].end_idx[tree_i]);
#endif
            }
        }
        total_num_keys_dpu += num_keys_for_each_dpu[dpu_i];
        /* send size: maximum number of requests to a DPU */
        if (num_keys_for_each_dpu[dpu_i] > send_size)
            send_size = num_keys_for_each_dpu[dpu_i];
    }
    int current_idx = 0;
    /* make cpu requests */
    for (int cpu_i = 0; cpu_i < MAX_NUM_BPTREE_IN_CPU; cpu_i++) {
        cpu_requests.at(cpu_i).clear();
        for (int i = 0; i < num_keys[cpu_i]; i++) {
            cpu_requests.at(cpu_i).push_back({batch_keys[current_idx + i], batch_keys[current_idx + i], WRITE});
        }
        current_idx += num_keys[cpu_i];
        num_reqs_for_cpus[cpu_i] = num_keys[cpu_i];
        total_num_keys_cpu += num_reqs_for_cpus[cpu_i];
    }
    /* make dpu requests */
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        if (num_keys_for_each_dpu[dpu_i] >= MAX_REQ_NUM_IN_A_DPU) {
            printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
                   "] request buffer size %d exceeds the limit %d because of skew\n",
                num_keys_for_each_dpu[dpu_i], MAX_REQ_NUM_IN_A_DPU);
            assert(false);
        }
        for (int i = 0; i < num_keys_for_each_dpu[dpu_i]; i++) {
            dpu_requests[dpu_i].requests[i].key = batch_keys[current_idx + i];
            dpu_requests[dpu_i].requests[i].write_val_ptr = batch_keys[current_idx + i];
            dpu_requests[dpu_i].requests[i].operation = WRITE;
        }
        current_idx += num_keys_for_each_dpu[dpu_i];
    }
#ifdef PRINT_DEBUG
    // for (key_int64_t x : num_keys) {
    //     std::cout << x << std::endl;
    // }
#endif
    free(batch_keys);
    return key_count;
}
#endif

// void generate_requests_same(){
//   dpu_requests.num_req = 0;
//   for (int i = 0; i < NUM_REQUESTS/NR_DPUS; i++){
//     key_int64_t key = (key_int64_t)rand();
//     dpu_requests.key[i] = key;
//     dpu_requests.read_or_write[i] = WRITE;
//     sprintf(dpu_requests.write_val[i], "%ld", key);
//     dpu_requests.num_req++;
//   }
// }
#ifdef VARY_REQUESTNUM
void send_experiment_vars(struct dpu_set_t set)
{
    DPU_ASSERT(dpu_broadcast_to(set, "expvars", 0, &expvars,
        sizeof(dpu_experiment_var_t), DPU_XFER_DEFAULT));
}

void receive_stats(struct dpu_set_t set)
{
    struct dpu_set_t dpu;
    uint32_t each_dpu;
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &stats[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "stats", 0,
        NUM_VARS * sizeof(dpu_stats_t), DPU_XFER_DEFAULT));
}
#endif

void show_requests(int i)
{
    if (i >= NR_DPUS) {
        printf("[invalid argment]i must be less than NR_DPUS");
        return;
    }
    printf("[debug_info]DPU:%d\n", i);
    for (int tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
        if (dpu_requests[i].end_idx[tree_i] != 0) {
            printf("tree %d first req:%ld,WRITE\n", tree_i, dpu_requests[i].requests[0].key);
        }
    }
}

void send_requests_redundant(struct dpu_set_t set, struct dpu_set_t dpu)
{
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu].end_idx));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "end_idx", 0,
        sizeof(int) * NUM_BPTREE_IN_DPU,
        DPU_XFER_DEFAULT));
#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(each_request_t) * send_size, sizeof(each_request_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu].requests));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "request_buffer", 0,
        sizeof(each_request_t) * send_size,
        DPU_XFER_DEFAULT));
}

void send_requests(struct dpu_set_t set, struct dpu_set_t dpu)
{
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu + NR_DPUS_REDUNDANT].end_idx));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "end_idx", 0,
        sizeof(int) * NUM_BPTREE_IN_DPU,
        DPU_XFER_DEFAULT));
#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(each_request_t) * send_size, sizeof(each_request_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu + NR_DPUS_REDUNDANT].requests));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "request_buffer", 0,
        sizeof(each_request_t) * send_size,
        DPU_XFER_DEFAULT));
}

// void send_requests_same(struct dpu_set_t set)
// {
//     DPU_ASSERT(dpu_broadcast_to(set, "request_buffer", 0, &dpu_requests,
//         sizeof(dpu_request_t), DPU_XFER_DEFAULT));
// }

float time_diff(struct timeval* start, struct timeval* end)
{
    float timediff = (end->tv_sec - start->tv_sec) + 1e-6 * (end->tv_usec - start->tv_usec);
    return timediff;
}

void* local_bptree_init(void* thread_id)
{
    int tid = *((int*)thread_id);
    bplustrees[tid] = init_BPTree();
    return NULL;
}

void* execute_queries(void* thread_id)
{
    int tid = *((int*)thread_id);
    value_ptr_t_ volatile getval;
    // printf("thread %d: %d times of insertion\n", tid, end_idx[tid]);
    for (int i = 0; i < num_reqs_for_cpus[tid]; i++) {
        BPTreeInsert(bplustrees[tid], cpu_requests.at(tid).at(i).key,
            cpu_requests.at(tid).at(i).write_val_ptr);
    }
// printf("thread %d: search\n", tid);
#if WORKLOAD == W05R95
    /* read intensive */
    for (int j = 0; j < 19; j++) {
        for (int i = 0; i < num_reqs_for_cpus[tid]; i++) {
            getval = BPTreeGet(bplustrees[tid], cpu_requests.at(tid).at(i).key);
        }
    }
#endif
#if WORKLOAD == W50R50
    /* write intensive */
    for (int i = 0; i < num_reqs_for_cpus[tid]; i++) {
        getval = BPTreeGet(bplustrees[tid], cpu_requests.at(tid).at(i).key);
    }
#endif

    getval++;
#ifdef PRINT_DEBUG
    if (tid == 0 || tid == 10) {
        printf("CPU thread %d: num of nodes = %d, height = %d\n", tid,
            BPTree_GetNumOfNodes(bplustrees[tid]), BPTree_GetHeight(bplustrees[tid]));
    }
#endif
    return NULL;
}

void initialize_cpu()
{
    for (int i = 0; i < MAX_NUM_BPTREE_IN_CPU; i++) {
        thread_ids[i] = i;
    }
    for (int i = 0; i < MAX_NUM_BPTREE_IN_CPU; i++) {
        pthread_create(&threads[i], NULL, local_bptree_init, thread_ids + i);
    }
    for (int i = 0; i < MAX_NUM_BPTREE_IN_CPU; i++) {
        pthread_join(threads[i], NULL);
    }
}
void execute_cpu(int num_trees_in_cpu)
{
    for (int i = 0; i < num_trees_in_cpu; i++) {
        pthread_create(&threads[i], NULL, execute_queries, thread_ids + i);
    }
    for (int i = 0; i < num_trees_in_cpu; i++) {
        pthread_join(threads[i], NULL);
    }
}

void execute_one_batch(struct dpu_set_t set, struct dpu_set_t set2, struct dpu_set_t dpu, struct dpu_set_t dpu2)
{
    // printf("\n");
    // printf("======= batch start=======\n");
    // show_requests(0);
    // gettimeofday(&start_total, NULL);
    // CPU→DPU
    // printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_IN_BATCH,
    // nr_of_dpus);
    gettimeofday(&start, NULL);
    send_requests(set, dpu);
    send_requests_redundant(set2, dpu2);
    gettimeofday(&end, NULL);
#ifdef PRINT_DEBUG
    printf("[2/4] send finished\n");
#endif
    send_and_execution_time += time_diff(&start, &end);
    send_time += time_diff(&start, &end);

    /* execution */
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_ASYNCHRONOUS));
    DPU_ASSERT(dpu_launch(set2, DPU_ASYNCHRONOUS));
    execute_cpu();
    gettimeofday(&end_cpu, NULL);
#ifdef PRINT_DEBUG
    printf("[3/4]cpu finished: %0.5fsec\n", time_diff(&start, &end_cpu));
#endif
    dpu_sync(set);
    dpu_sync(set2);
    gettimeofday(&end_total, NULL);
#ifdef PRINT_DEBUG
    printf("[4/4]cpu and all dpus finished: %0.5fsec\n", time_diff(&start, &end_total));
#endif
    cpu_time += time_diff(&start, &end_cpu);
    send_and_execution_time += time_diff(&start, &end_total);
    execution_time += time_diff(&start, &end_total);

// DPU→CPU
#ifdef STATS_ON
    GET_AND_PRINT_TIME(
        {
#ifdef VARY_REQUESTNUM
            each_dpu = 0;
            receive_stats(set);
#endif
            each_dpu = 0;
            DPU_FOREACH(set, dpu, each_dpu)
            {
                DPU_ASSERT(dpu_prepare_xfer(dpu, &nb_cycles_get[each_dpu]));
            }
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "nb_cycles_get", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_FOREACH(set, dpu, each_dpu)
            {
                DPU_ASSERT(dpu_prepare_xfer(dpu, &nb_cycles_insert[each_dpu]));
            }
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "nb_cycles_insert", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
        },
        receive results)
    for (int each_dpu = 0; each_dpu < NR_DPUS; each_dpu++) {
#ifdef VARY_REQUESTNUM
        for (int x = 0; x < NUM_VARS; x++) {
            printf("[DPU#%u]requestnum:around_%d cycles/request:[insert:%dcycles, "
                   "get:%dcycles]\n",
                each_dpu, stats[each_dpu][x].x,
                stats[each_dpu][x].cycle_insert / (expvars.gap * 2),
                stats[each_dpu][x].cycle_get / (expvars.gap * 2));
        }
#endif
        printf("[DPU#%u]nb_cycles_insert=%ld(average %ld cycles)\n", each_dpu,
            nb_cycles_insert[each_dpu],
            nb_cycles_insert[each_dpu] / num_requests[each_dpu]);
        printf("[DPU#%u]nb_cycles_get=%ld(average %ld cycles)\n", each_dpu,
            nb_cycles_get[each_dpu],
            nb_cycles_get[each_dpu] / num_requests[each_dpu]);
        printf("\n");
    }
#endif
    // print results
    // printf("total time spent: %0.8f sec\n", time_diff(&start_total,&end));
}

void tree_migration(std::pair<int, int>& from_pair, std::pair<int, int>& to_pair, dpu_set_t set, dpu_set_t dpu)
{
    /* no need to migrate */
    if (from_pair.first == to_pair.first)
        return;
    /* from CPU (to DPU)*/
    if (from_pair.first == -1) {
        serialize(bptrees[from_pair.second]);
    } else { /* from DPU */
        DPU_FOREACH(set, dpu, each_dpu)
        {
            printf("ed = %d\n", each_dpu);
            if (each_dpu == from_pair.first) {
                DPU_ASSERT(dpu_prepare_xfer(dpu, &SERIALIZE_TASK));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
                DPU_ASSERT(dpu_prepare_xfer(dpu, &from_pair.second));
                DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "transfer_tree", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
                DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
                DPU_ASSERT(dpu_copy_from(dpu, "nodes_transfer_num", 0, &nodes_num, sizeof(uint64_t)));
                DPU_ASSERT(dpu_copy_from(dpu, "nodes_transfer_buffer", 0, &nodes_buffer, nodes_num * sizeof(BPTreeNode)));
                break;
                printf("%lu\n", nodes_num);
            }
        }
    }
    /* (from DPU) to CPU */
    if (to_pair.first == -1) {
        to_pair.second = deserialize()->tree_index;
    } /* to DPU */
    DPU_FOREACH(set, dpu, each_dpu)
    {
        printf("ed = %d\n", each_dpu);
        if (each_dpu == to_pair.first) {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_num));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "nodes_transfer_num", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_buffer));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "nodes_transfer_buffer", 0, nodes_num * sizeof(BPTreeNode), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &DESERIALIZE_TASK));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            DPU_ASSERT(dpu_copy_from(dpu, "malloced_tree_index", 0, &malloced_tree_index, nodes_num * sizeof(BPTreeNode)));
            break;
        }
    }
}

void execute_one_batch(struct dpu_set_t set, struct dpu_set_t dpu)
{
    // printf("\n");
    // printf("======= batch start=======\n");
    // show_requests(0);
    // gettimeofday(&start_total, NULL);
    // CPU→DPU
    // printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_IN_BATCH,
    // nr_of_dpus);
    /* migration phase */
    gettimeofday(&start, NULL);
    gettimeofday(&end, NULL);
    migration_time += time_diff(&start, &end);
    /* send requests, including tree migration */
    gettimeofday(&start, NULL);
    send_requests(set, dpu);
    gettimeofday(&end, NULL);
#ifdef PRINT_DEBUG
    printf("[2/4] send finished\n");
#endif
    send_and_execution_time += time_diff(&start, &end);
    send_time += time_diff(&start, &end);
    /* execution */
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_ASYNCHRONOUS));
    execute_cpu();
    gettimeofday(&end_cpu, NULL);
#ifdef PRINT_DEBUG
    printf("[3/4]cpu finished: %0.5fsec\n", time_diff(&start, &end_cpu));
#endif
    dpu_sync(set);
    gettimeofday(&end_total, NULL);
#ifdef PRINT_DEBUG
    printf("[4/4]cpu and all dpus finished: %0.5fsec\n", time_diff(&start, &end_total));
#endif
    cpu_time += time_diff(&start, &end_cpu);
    send_and_execution_time += time_diff(&start, &end_total);
    execution_time += time_diff(&start, &end_total);

// DPU→CPU
#ifdef STATS_ON
    GET_AND_PRINT_TIME(
        {
#ifdef VARY_REQUESTNUM
            each_dpu = 0;
            receive_stats(set);
#endif
            each_dpu = 0;
            DPU_FOREACH(set, dpu, each_dpu)
            {
                DPU_ASSERT(dpu_prepare_xfer(dpu, &nb_cycles_get[each_dpu]));
            }
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "nb_cycles_get", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_FOREACH(set, dpu, each_dpu)
            {
                DPU_ASSERT(dpu_prepare_xfer(dpu, &nb_cycles_insert[each_dpu]));
            }
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "nb_cycles_insert", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
        },
        receive results)
    for (int each_dpu = 0; each_dpu < NR_DPUS; each_dpu++) {
#ifdef VARY_REQUESTNUM
        for (int x = 0; x < NUM_VARS; x++) {
            printf("[DPU#%u]requestnum:around_%d cycles/request:[insert:%dcycles, "
                   "get:%dcycles]\n",
                each_dpu, stats[each_dpu][x].x,
                stats[each_dpu][x].cycle_insert / (expvars.gap * 2),
                stats[each_dpu][x].cycle_get / (expvars.gap * 2));
        }
#endif
        printf("[DPU#%u]nb_cycles_insert=%ld(average %ld cycles)\n", each_dpu,
            nb_cycles_insert[each_dpu],
            nb_cycles_insert[each_dpu] / num_requests[each_dpu]);
        printf("[DPU#%u]nb_cycles_get=%ld(average %ld cycles)\n", each_dpu,
            nb_cycles_get[each_dpu],
            nb_cycles_get[each_dpu] / num_requests[each_dpu]);
        printf("\n");
    }
#endif
    // print results
    // printf("total time spent: %0.8f sec\n", time_diff(&start_total,&end));
}


std::set<std::pair<key_int64_t, int>> key_to_tree_index;
int num_reqs_for_each_tree[NUM_TOTAL_TREES];
split_info_t split_result[MAX_NUM_BPTREE_IN_DPU];

int key_to_tree(key_int64_t key)
{
    for (auto itr = key_to_tree_index.begin(); itr != key_to_tree_index.end(); ++itr) {
        if (key < (*itr).first) {
            itr--;
            return (*itr).second;
        }
    }
    assert(false);
    return -1;
}

void initialize()
{
    int count = 0;
    for (auto itr = global_tree_idx_to_local_tree_idx.begin(); itr != global_tree_idx_to_local_tree_idx.end(); ++itr) {
        *itr = std::make_pair(count, 0);
        count++;
    }
}

int main(int argc, char* argv[])
{
    // printf("\n");
    // printf("size of dpu_request_t:%lu,%lu\n", sizeof(dpu_request_t),
    // sizeof(dpu_requests[0])); printf("total num of
    // requests:%ld\n",(uint64_t)NUM_REQUESTS);
    cmdline::parser a;
    a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, 1000000);
    a.add<std::string>("zipfianconst", 'a', "zipfianconst", false, "1.2");
    a.parse_check(argc, argv);
    std::string zipfian_const = a.get<std::string>("zipfianconst");
    int max_key_num = a.get<int>("keynum");
    std::string file_name = ("./workload/zipf_const_" + zipfian_const + ".bin");
    int query_num_coefficient;
#if WORKLOAD == (W50R50)
    query_num_coefficient = 2;
#endif
#if WORKLOAD == (W05R95)
    query_num_coefficient = 20;
#endif
#ifdef PRINT_DEBUG
    std::cout << "zipf_const:" << zipfian_const << ", file:" << file_name << ", WORKLOAD:" << WORKLOAD << ", coefficient:" << query_num_coefficient << std::endl;
#endif
    struct dpu_set_t set, dpu;
    DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
    DPU_ASSERT(dpu_get_nr_dpus(set, &nr_of_dpus));

#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", nr_of_dpus1);
    printf("Allocated %d DPU(s)\n", nr_of_dpus2);
    std::cout << "num trees in CPU:" << MAX_NUM_BPTREE_IN_CPU << std::endl;
    std::cout << "num trees in DPU:" << NUM_BPTREE_IN_DPU << std::endl;
    std::cout << "num total trees:" << NUM_TOTAL_TREES << std::endl;
#endif
// set expvars
#ifdef VARY_REQUESTNUM
    expvars.gap = 50;
    int init = 100;
    int var = 1000;  // vars = {100,1000,2000,4000,8000,16000,32000,64000,...}
    expvars.vars[0] = init;
    for (int i = 1; i < NUM_VARS; i++) {
        expvars.vars[i] = var;
        var = var << 1;
    }
    printf("max_expvars:%d\n", expvars.vars[NUM_VARS - 1]);
    printf("NUM_REQUESTS_PER_DPU:%d\n", NUM_REQUESTS_PER_DPU);
    if (expvars.vars[NUM_VARS - 1] >= NUM_REQUESTS_PER_DPU) {
        printf(ANSI_COLOR_RED "please reduce NUM_VARS in common.h to ceil(%f).\n",
            (log2(NUM_REQUESTS_PER_DPU / 1000) + 1));
        return 1;
    }

    send_experiment_vars(set);
#endif
    initialize_cpu();
    // printf("initializing trees (10000 keys average)\n");
    // generate_requests();
    // send_requests(set,dpu);
    // DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    // free(dpu_requests);
    // printf("initialization finished\n");
    /* load workload file */
    std::ifstream file_input(file_name, std::ios_base::binary);
    if (!file_input) {
        printf("cannot open file\n");
        return 1;
    }
    /* main routine */
    int batch_num = 0;
    int num_keys = 0;
    gettimeofday(&start_total, NULL);
    while (total_num_keys < max_key_num) {
        // printf("%d\n", num_keys);
        if (max_key_num - total_num_keys >= NUM_REQUESTS_PER_BATCH) {
            num_keys = query_num_coefficient * generate_requests_fromfile(file_input, NUM_REQUESTS_PER_BATCH / query_num_coefficient);
        } else {
            num_keys = query_num_coefficient * generate_requests_fromfile(file_input, (max_key_num - total_num_keys) / query_num_coefficient);
        }
        if (num_keys == 0)
            break;
        total_num_keys += num_keys;
#ifdef PRINT_DEBUG
        std::cout << std::endl
                  << "===== batch " << batch_num << " =====" << std::endl;
        std::cout << "[1/4] " << num_keys << " requests generated" << std::endl;
        std::cout << "executing " << total_num_keys << "/" << max_key_num << "..." << std::endl;
#endif
#if (NR_DPUS_REDUNDANT)
        execute_one_batch(set, set2, dpu, dpu2);
#else
        execute_one_batch(set, dpu);
#endif
#ifdef PRINT_DEBUG
        // DPU_FOREACH(set, dpu, each_dpu)
        // {
        //     if (each_dpu == 0)
        //         DPU_ASSERT(dpu_log_read(dpu, stdout));
        // }
        // DPU_FOREACH(set2, dpu2, each_dpu)
        // {
        //     if (each_dpu == 0)
        //         DPU_ASSERT(dpu_log_read(dpu2, stdout));
        // }
#endif
#ifdef DEBUG_ON
        printf("results from DPUs: batch %d\n", total_num_keys / num_keys);
#endif
        free(dpu_requests);
        batch_num++;
    }
    gettimeofday(&end_total, NULL);
    total_time = time_diff(&start_total, &end_total);

    double throughput = total_num_keys / execution_time;
#ifdef PRINT_DEBUG
    printf("zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%%], send_and_execution_time, total_time, throughput\n");
#endif
    //printf("%ld,%ld,%ld\n", total_num_keys_cpu, total_num_keys_dpu, total_num_keys_cpu + total_num_keys_dpu);
    //printf("%s, %d, %d, %d, %d, %ld, %ld, %ld, %ld, %0.5f, %0.5f, %0.5f, %0.3f, %0.5f, %0.0f\n", zipfian_const.c_str(), NR_DPUS, NR_TASKLETS, MAX_NUM_BPTREE_IN_CPU, NUM_BPTREE_IN_DPU * NR_DPUS, (long int)2 * total_num_keys, 2 * total_num_keys_cpu, 2 * total_num_keys_dpu, 100 * total_num_keys_cpu / total_num_keys, send_time, cpu_time,
    //    execution_time, 100 * cpu_time / execution_time, send_and_execution_time, total_time, throughput);
    printf("%s, %d, %d, %d, %d, %d, %ld, %ld, %ld, %ld, %0.5f, %0.5f, %0.5f, %0.2f, %0.5f, %0.5f, %0.0f\n",
        zipfian_const.c_str(), NR_DPUS_REDUNDANT, NR_DPUS_MULTIPLE, NR_TASKLETS,
        MAX_NUM_BPTREE_IN_CPU, NUM_TOTAL_TREES - MAX_NUM_BPTREE_IN_CPU, total_num_keys, query_num_coefficient * total_num_keys_cpu,
        query_num_coefficient * total_num_keys_dpu, 100 * query_num_coefficient * total_num_keys_cpu / total_num_keys, send_time, cpu_time,
        execution_time, 100 * cpu_time / execution_time, send_and_execution_time, total_time, throughput);
    DPU_ASSERT(dpu_free(set));
#if (NR_DPUS_REDUNDANT)
    DPU_ASSERT(dpu_free(set2));
#endif
    return 0;
}
