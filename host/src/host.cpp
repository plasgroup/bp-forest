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

#ifndef DPU_BINARY
#define DPU_BINARY "./build/dpu/dpu_program"
#endif

#define NUM_TOTAL_TREES (NR_DPUS * NUM_BPTREE_IN_DPU + NUM_BPTREE_IN_CPU)
#define GET_AND_PRINT_TIME(CODES, LABEL) \
    gettimeofday(&start, NULL);          \
    CODES                                \
    gettimeofday(&end, NULL);            \
    printf("time spent for %s: %0.8f sec\n", #LABEL, time_diff(&start, &end));

// dpu_request_t dpu_requests[NR_DPUS];
dpu_requests_t* dpu_requests;
typedef std::vector<std::vector<each_request_t>> cpu_requests_t;
cpu_requests_t cpu_requests(NUM_BPTREE_IN_CPU);
#ifdef VARY_REQUESTNUM
dpu_experiment_var_t expvars;
dpu_stats_t stats[NR_DPUS][NUM_VARS];
#endif

uint64_t nb_cycles_insert[NR_DPUS];
uint64_t nb_cycles_get[NR_DPUS];
uint64_t total_cycles_insert;
float total_time_sendrequests;
float total_time_execution;
float total_time;
int each_dpu;
int send_size;
uint64_t num_requests[NR_DPUS];
int num_reqs_for_cpus[NUM_BPTREE_IN_CPU];
uint64_t total_num_keys;
uint32_t nr_of_dpus;
struct timeval start, end, start_total;

BPlusTree* bplustrees[NUM_BPTREE_IN_CPU];
pthread_t threads[NUM_BPTREE_IN_CPU];
int thread_ids[NUM_BPTREE_IN_CPU];
constexpr key_int64_t RANGE = std::numeric_limits<uint64_t>::max() / (NUM_TOTAL_TREES);
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

int generate_requests_fromfile(std::ifstream& fs)
{
    dpu_requests = (dpu_requests_t*)malloc(
        (NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }

    /* read workload file */
    key_int64_t* batch_keys = (key_int64_t*)malloc(NUM_REQUESTS_PER_BATCH * sizeof(key_int64_t));
    if (dpu_requests == NULL) {
    printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
            "] heap size is not enough\n");
    return 0;
    }
    // std::cout << "malloc batch_keys" << std::endl;
    int key_count = 0;
    fs.read(reinterpret_cast<char*>(batch_keys), sizeof(batch_keys) * NUM_REQUESTS_PER_BATCH);
    key_count = fs.tellg() / sizeof(key_int64_t) - total_num_keys;
    //std::cout << "key_count: " << key_count << std::endl;
    /* sort by which tree the requests should be processed */
    std::sort(batch_keys, batch_keys + key_count, [](auto a, auto b) { return a / RANGE < b / RANGE; });
    /* count the number of requests for each tree */
    int num_keys[NUM_TOTAL_TREES] = {};
    int num_keys_for_each_dpu[NR_DPUS] = {};
    for (int i = 0; i < key_count; i++) {
        num_keys[batch_keys[i] / RANGE]++;
    }
    /* count the number of requests for each DPU, determine the send size */
    send_size = 0;
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        for (int tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
            dpu_requests[dpu_i].end_idx[tree_i] = num_keys_for_each_dpu[dpu_i];
            num_keys_for_each_dpu[dpu_i] += num_keys[NUM_BPTREE_IN_CPU + dpu_i * NUM_BPTREE_IN_DPU + tree_i];
        }
        if (num_keys_for_each_dpu[dpu_i] > send_size)
            send_size = num_keys_for_each_dpu[dpu_i];
    }
    int current_idx = 0;
    /* make cpu requests */
    for (int cpu_i = 0; cpu_i < NUM_BPTREE_IN_CPU; cpu_i++) {
        for (int i = 0; i < num_keys[cpu_i]; i++) {
            cpu_requests.at(cpu_i).push_back({batch_keys[current_idx + i], batch_keys[current_idx + i], WRITE});
        }
        current_idx += num_keys[cpu_i];
        num_reqs_for_cpus[cpu_i] = num_keys[cpu_i];
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
        printf("end_idx for tree %d = %d\n", tree_i, dpu_requests[i].end_idx[tree_i]);
        if (dpu_requests[i].end_idx[tree_i] != 0) {
            printf("tree %d first req:%ld,WRITE\n", tree_i, dpu_requests[i].requests[0].key);
        }
    }
}

void send_requests(struct dpu_set_t set, struct dpu_set_t dpu)
{
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu].end_idx));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "end_idx", 0,
        sizeof(int) * NUM_BPTREE_IN_DPU,
        DPU_XFER_DEFAULT));
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(each_request_t) * send_size, sizeof(each_request_t) * MAX_REQ_NUM_IN_A_DPU);
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu].requests));
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
    for (int i = 0; i < num_reqs_for_cpus[tid]; i++) {
        getval = BPTreeGet(bplustrees[tid], cpu_requests.at(tid).at(i).key);
    }
    getval++;
    return NULL;
    // printf("thread %d: num of nodes = %d, height = %d\n", tid,
    // BPTree_GetNumOfNodes(bplustrees[tid]) ,BPTree_GetHeight(bplustrees[tid]));
}

void initialize_cpu()
{
    for (int i = 0; i < NUM_BPTREE_IN_CPU; i++) {
        thread_ids[i] = i;
    }
    for (int i = 0; i < NUM_BPTREE_IN_CPU; i++) {
        pthread_create(&threads[i], NULL, local_bptree_init, thread_ids + i);
    }
    for (int i = 0; i < NUM_BPTREE_IN_CPU; i++) {
        pthread_join(threads[i], NULL);
    }
}
void execute_cpu()
{
    for (int i = 0; i < NUM_BPTREE_IN_CPU; i++) {
        pthread_create(&threads[i], NULL, execute_queries, thread_ids + i);
    }
}

void execute_one_batch(struct dpu_set_t set, struct dpu_set_t dpu)
{
    // printf("\n");
    // printf("======= batch start=======\n");
    // show_requests(0);
    gettimeofday(&start_total, NULL);
    // CPU→DPU
    // printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_IN_BATCH,
    // nr_of_dpus);
    gettimeofday(&start, NULL);
    send_requests(set, dpu);
    gettimeofday(&end, NULL);
#ifdef PRINT_DEBUG
    printf("[2/4] send finished\n");
#endif
    total_time += time_diff(&start, &end);
    total_time_sendrequests += time_diff(&start, &end);

    // DPU execution
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_ASYNCHRONOUS));
    execute_cpu();
    for (int i = 0; i < NUM_BPTREE_IN_CPU; i++) {
        pthread_join(threads[i], NULL);
    }
    gettimeofday(&end, NULL);
    printf("cpu finished: %0.5fsec\n", time_diff(&start, &end));
    dpu_sync(set);
#ifdef PRINT_DEBUG
    printf("[4/4] DPU_finished\n");
#endif
    gettimeofday(&end, NULL);
    printf("cpu and all dpus finished: %0.5fsec\n", time_diff(&start, &end));
    total_time += time_diff(&start, &end);
    total_time_execution += time_diff(&start, &end);

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

int main(int argc, char* argv[])
{
    // printf("\n");
    // printf("size of dpu_request_t:%lu,%lu\n", sizeof(dpu_request_t),
    // sizeof(dpu_requests[0])); printf("total num of
    // requests:%ld\n",(uint64_t)NUM_REQUESTS);

    cmdline::parser a;
    a.add<int>("keynum", 'n', "num of generated_keys to generate", false, 100000000);
    a.add<std::string>("zipfianconst", 'a', "zipfianconst", false, "1.2");
    a.parse_check(argc, argv);
    std::string zipfian_const = a.get<std::string>("zipfianconst");
    int key_num = a.get<int>("keynum");
    std::string file_name = ("./workload/zipf_const_" + zipfian_const +  ".bin");
    std::cout << "zipf_const:" << zipfian_const << ", file:" << file_name << std::endl;
    
    struct dpu_set_t set, dpu;
    DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
    DPU_ASSERT(dpu_get_nr_dpus(set, &nr_of_dpus));
    printf("Allocated %d DPU(s)\n", nr_of_dpus);
    #ifdef PRINT_DEBUG
    std::cout << num trees in CPU: << NUM_BPTREE_IN_CPU << std::endl;
    std::cout << num trees in DPU: << NUM_BPTREE_IN_DPU << std::endl;
    std::cout << num total trees: << NUM_TOTAL_TREES << std::endl;
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
    while (true && total_num_keys < 20000000) {
        // printf("%d\n", num_keys);
        int num_keys = generate_requests_fromfile(file_input);
        if (num_keys == 0)
            break;
        total_num_keys += num_keys;
        std::cout << "=== executing "<< total_num_keys << "/20000000 ===" << std::endl;
        execute_one_batch(set, dpu);
        #ifdef PRINT_DEBUG
        DPU_FOREACH(set, dpu, each_dpu) { if(each_dpu==0)DPU_ASSERT(dpu_log_read(dpu, stdout)); }
        #endif
#ifdef DEBUG_ON
        printf("results from DPUs: batch %d\n", total_num_keys / num_keys);
#endif
        free(dpu_requests);
    }

    double throughput = 2 * total_num_keys / total_time_execution;
    printf("dpus, tasklets, queries, send_time, execution_time, total_time, throughput\n");
    printf("%d, %d, %ld, %0.8f, %0.8f, %0.8f, %0.0f\n", NR_DPUS , NR_TASKLETS,
        (long int)2 * total_num_keys, total_time_sendrequests,
        total_time_execution, total_time, throughput);
    DPU_ASSERT(dpu_free(set));
#ifdef WRITE_CSV
    // write results to a csv file
    char* fname = "data/taskletnum_upmem_pointerdram_1thread.csv";
    FILE* fp;
    fp = fopen(fname, "a");
    if (fp == NULL) {
        printf("file %s cannot be open¥n", fname);
        return -1;
    }
    if (NR_TASKLETS == 1) {
        fprintf(fp, "num of tasklets, total_num_requests[s], time_sendrequests[s], "
                    "time_dpu_execution[s], total_time[s], throughput[OPS/s]\n");
    }
    fprintf(fp, "%d, %ld, %0.8f, %0.8f, %0.8f, %0.0f\n", NR_TASKLETS,
        2 * total_num_keys, total_time_sendrequests, total_time_execution,
        total_time, throughput);
    fclose(fp);
#endif
    return 0;
}
