#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "cmdline.h"
#include <assert.h>
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#include "node_defs.hpp"
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
// extern "C" {
// #include "bplustree.h"
// }
#include <map>

#include "common.h"

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#ifndef NUM_TREES_PER_DPU
#define NUM_TREES_PER_DPU (10)
#endif
#ifndef NUM_INIT_REQS
#define NUM_INIT_REQS (1000 * NUM_TOTAL_TREES)
#endif
#define NUM_TOTAL_TREES (NR_DPUS * NUM_TREES_PER_DPU)
#define MAX_NUM_TREES_IN_DPU (NR_TASKLETS)
#define GET_AND_PRINT_TIME(CODES, LABEL) \
    gettimeofday(&start, NULL);          \
    CODES                                \
    gettimeofday(&end, NULL);            \
    printf("time spent for %s: %0.8f sec\n", #LABEL, time_diff(&start, &end));

dpu_requests_t* dpu_requests;

uint64_t nb_cycles_insert[NR_DPUS];
uint64_t nb_cycles_get[NR_DPUS];
uint64_t total_cycles_insert;
float preprocess_time1;
float preprocess_time2;
float preprocess_time;
float migration_time;
float send_time;
float execution_time;
float batch_time = 0;
float total_preprocess_time = 0;
float total_migration_time = 0;
float total_send_time = 0;
float total_execution_time = 0;
float total_batch_time = 0;
float init_time = 0;
int each_dpu;
int send_size;
int batch_num = 0;
int migrated_tree_num = 0;
uint64_t total_num_keys;
uint32_t nr_of_dpus;
struct timeval start, end, start_total, end_total;
constexpr key_int64_t RANGE = std::numeric_limits<uint64_t>::max() / (NUM_TOTAL_TREES);

uint64_t task_from = TASK_FROM;
uint64_t task_to = TASK_TO;
uint64_t task_init = TASK_INIT;
uint64_t task_get = TASK_GET;
uint64_t task_insert = TASK_INSERT;
uint64_t task_invalid = 999 + ((uint64_t)1U << 32);
int migration_per_batch;
uint32_t tree_bitmap[NR_DPUS] = {0};
std::map<key_int64_t, std::pair<int, int>> key_to_tree_map;
key_int64_t tree_to_key_map[NR_DPUS][MAX_NUM_TREES_IN_DPU];
int key_index[NR_DPUS][MAX_NUM_TREES_IN_DPU + 1] = {0};
BPTreeNode nodes_buffer[MAX_NODE_NUM];
uint64_t nodes_num;
void init_key_to_tree_map()
{
    for (int i = 0; i < NUM_TOTAL_TREES; i++) {
        key_to_tree_map[RANGE * (i + 1)] = std::make_pair(i / NUM_TREES_PER_DPU, i % NUM_TREES_PER_DPU);
    }
}

void init_tree_to_key_map()
{
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < NUM_TREES_PER_DPU; j++)
            tree_to_key_map[i][j] = RANGE * (i * NUM_TREES_PER_DPU + j + 1);
    }
}

void init_tree_bitmap()
{
    for (int i = 0; i < NR_DPUS; i++) {
        tree_bitmap[i] = (i << NUM_TREES_PER_DPU) - 1;
    }
}

float time_diff(struct timeval* start, struct timeval* end)
{
    float timediff = (end->tv_sec - start->tv_sec) + 1e-6 * (end->tv_usec - start->tv_usec);
    return timediff;
}

// 比較関数
auto comp_idx(int* ptr)
{
    return [ptr](int l_idx, int r_idx) {
        return ptr[l_idx] > ptr[r_idx];
    };
}

void send_requests(struct dpu_set_t set, struct dpu_set_t dpu, uint64_t* task)
{
    DPU_ASSERT(dpu_broadcast_to(set, "task_no", 0, task, sizeof(uint64_t), DPU_XFER_DEFAULT));
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &key_index[each_dpu]));
        for (int i = 0; i < 10; i++) {
            //printf("key_index[%d][%d] = %d\n", each_dpu, i, key_index[each_dpu][i]);
        }
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "end_idx", 0,
        sizeof(int) * (MAX_NUM_BPTREE_IN_DPU),
        DPU_XFER_DEFAULT));
#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(each_request_t) * send_size, sizeof(each_request_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &dpu_requests[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "request_buffer", 0,
        sizeof(each_request_t) * send_size,
        DPU_XFER_DEFAULT));
}

void send_nodes_from_dpu_to_dpu(int from_DPU, int from_tree, int to_DPU, int to_tree, dpu_set_t set, dpu_set_t dpu)
{
    uint64_t task;
    DPU_FOREACH(set, dpu, each_dpu)
    {
        //printf("ed = %d\n", each_dpu);
        if (each_dpu == from_DPU) {
            task = (task_from << 32) | from_tree;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            DPU_ASSERT(dpu_copy_from(dpu, "nodes_transfer_num", 0, &nodes_num, sizeof(uint64_t)));
            DPU_ASSERT(dpu_copy_from(dpu, "nodes_transfer_buffer", 0, &nodes_buffer, nodes_num * sizeof(BPTreeNode)));
            break;
            //printf("%lu\n", nodes_num);
        }
    }
    tree_bitmap[from_DPU] &= ~(1 << from_tree);  // treeが無くなったことを記録
    DPU_FOREACH(set, dpu, each_dpu)
    {
        // printf("ed = %d\n", each_dpu);
        if (each_dpu == to_DPU) {
            task = (task_to << 32) | to_tree;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_num));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "nodes_transfer_num", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_buffer));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "nodes_transfer_buffer", 0, nodes_num * sizeof(BPTreeNode), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            break;
        }
    }
    tree_bitmap[to_DPU] |= (1 << to_tree);  // treeができたことを記録
    // キーと木の対応を更新
    key_to_tree_map[tree_to_key_map[from_DPU][from_tree]] = std::make_pair(to_DPU, to_tree);
    tree_to_key_map[to_DPU][to_tree] = tree_to_key_map[from_DPU][from_tree];
    tree_to_key_map[from_DPU][from_tree] = -1;
}

void initialize_dpus(int num_init_reqs, struct dpu_set_t set, struct dpu_set_t dpu)
{
    init_key_to_tree_map();
    init_tree_to_key_map();
    init_tree_bitmap();
    dpu_requests
        = (dpu_requests_t*)malloc((NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "]heap size is not enough\n");
        return;
    }
    key_int64_t* keys = (key_int64_t*)malloc(NUM_INIT_REQS * sizeof(key_int64_t));
    key_int64_t interval = (key_int64_t)std::numeric_limits<uint64_t>::max() / num_init_reqs;
    //printf("interval = %ld\n", interval);
    int num_keys_for_tree[NR_DPUS][MAX_NUM_TREES_IN_DPU] = {0};
    int num_keys_for_DPU[NR_DPUS] = {0};
    //printf("MAX_NUM_TREES_IN_DPU = %d\n", MAX_NUM_TREES_IN_DPU);
    for (int i = 0; i < num_init_reqs; i++) {
        keys[i] = interval * i;
        //printf("keys[i] = %ld\n", keys[i]);
        std::map<key_int64_t, std::pair<int, int>>::iterator it = key_to_tree_map.lower_bound(keys[i]);

        //printf("%d,%d\n", it->second.first, it->second.second);
        if (it != key_to_tree_map.end()) {
            //printf("key:%ld,DPU:%d,tree:%d\n", keys[i], it->second.first, it->second.second);
            num_keys_for_tree[it->second.first][it->second.second]++;
            //printf("num_keys_for_tree[%d][%d]++\n", it->second.first, it->second.second);
            num_keys_for_DPU[it->second.first]++;
        } else {
            printf("ERROR: the key is out of range 1\n");
        }
    }
    /* key_index(i番目のDPUのj番目の木へのクエリの開始インデックス)の作成 */
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j <= MAX_NUM_TREES_IN_DPU; j++) {
            key_index[i][j] = 0;
        }
    }
    /* key_index(i番目のDPUのj番目の木へのクエリの開始インデックス)の作成 */
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < MAX_NUM_TREES_IN_DPU; j++) {
            //printf("num_keys_for_tree[%d][%d] = %d\n", i, j, num_keys_for_tree[i][j]);
        }
    }
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 1; j <= MAX_NUM_TREES_IN_DPU; j++) {
            key_index[i][j] = key_index[i][j - 1] + num_keys_for_tree[i][j - 1];
            //printf("key_index[%d][%d] = %d\n", i, j, key_index[i][j]);
        }
    }
    for (int i = 0; i < num_init_reqs; i++) {
        auto it = key_to_tree_map.lower_bound(keys[i]);
        if (it != key_to_tree_map.end()) {
            dpu_requests[it->second.first].requests[key_index[it->second.first][it->second.second]].key = keys[i];
            //printf("key_index[%d][%d] = %d\n", it->second.first, it->second.second, key_index[it->second.first][it->second.second]);
            dpu_requests[it->second.first].requests[key_index[it->second.first][it->second.second]++].write_val_ptr = keys[i];
        } else {
            printf("ERROR: the key is out of range 2\n");
        }
    }
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j <= MAX_NUM_TREES_IN_DPU; j++) {
            //printf("key_index[%d][%d] = %d\n", i, j, key_index[i][j]);
        }
    }

    /* count the number of requests for each DPU, determine the send size */
    send_size = 0;
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        /* send size: maximum number of requests to a DPU */
        if (num_keys_for_DPU[dpu_i] > send_size)
            send_size = num_keys_for_DPU[dpu_i];
    }

    /* init BPTree in DPUs */
    DPU_ASSERT(dpu_broadcast_to(set, "task_no", 0, &task_init, sizeof(uint64_t), DPU_XFER_DEFAULT));
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    dpu_sync(set);
    // DPU_FOREACH(set, dpu)
    // {
    //     DPU_ASSERT(dpu_log_read(dpu, stdout));
    // }
    //printf("initialized bptrees\n");
#ifdef PRINT_DEBUG
    DPU_FOREACH(set, dpu, each_dpu)
    {
        if (each_dpu == 0)
            DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif
    /* insert initial keys for each tree */
    send_requests(set, dpu, &task_insert);
    //printf("sent reqs\n");
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    dpu_sync(set);
    gettimeofday(&end, NULL);
    init_time = time_diff(&start, &end);
    printf("DPU initialization:%0.5f\n", init_time);
#ifdef PRINT_DEBUG
    DPU_FOREACH(set, dpu, each_dpu)
    {
        if (each_dpu == 0)
            DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif
    free(dpu_requests);
    free(keys);
    return;
}

/* make batch, do migration, prepare queries for dpus */
int batch_preprocess(std::ifstream& fs, int n, struct dpu_set_t set, struct dpu_set_t dpu)
{
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
    gettimeofday(&start, NULL);
    /* 1. それぞれの木へのクエリ数を数える */
    int num_keys_for_tree[NR_DPUS][MAX_NUM_TREES_IN_DPU] = {0};
    int num_keys_for_DPU[NR_DPUS] = {0};
    for (int i = 0; i < key_count; i++) {
        auto it = key_to_tree_map.lower_bound(batch_keys[i]);
        if (it != key_to_tree_map.end()) {
            num_keys_for_tree[it->second.first][it->second.second]++;
            num_keys_for_DPU[it->second.first]++;
        } else {
            printf("ERROR: the key is out of range 3\n");
        }
    }

    // 2.多い順にインデックスを記録
    int idx[NR_DPUS];
    for (int i = 0; i < NR_DPUS; i++) {
        idx[i] = i;
    }
    std::sort(idx, idx + NR_DPUS, comp_idx(num_keys_for_DPU));
    // for (int i = 0; i < NR_DPUS; i++) {
    //     printf("DPU #%d has %d queries\n", i, num_keys_for_DPU[i]);
    // }
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < MAX_NUM_TREES_IN_DPU; j++) {
            //printf("num_keys[%d][%d] = %d\n", i, j, num_keys_for_tree[i][j]);
        }
    }
    gettimeofday(&end, NULL);
    preprocess_time1 = time_diff(&start, &end);
    // 3.木のmigration
    migrated_tree_num = 0;
    gettimeofday(&start, NULL);
    int num_trees_to_be_migrated = migration_per_batch;  // 何個の木を移動するか
    int count = 0;                                       // チェックしたDPUの数(移動しなかった場合も含む)
    while (num_trees_to_be_migrated) {
        if (count >= NR_DPUS - 1 - (migration_per_batch - num_trees_to_be_migrated)) {
            //printf("migration limit because of NR_DPUS, %d migration done\n", migration_per_batch - num_trees_to_be_migrated);
            break;
        }
        int from_DPU = idx[count];
        int to_DPU = idx[NR_DPUS - 1 - (migration_per_batch - num_trees_to_be_migrated)];
        int diff_before = num_keys_for_DPU[from_DPU] - num_keys_for_DPU[to_DPU];
        //printf("from_DPU=%d,to_DPU=%d,diff_before=%d\n", from_DPU, to_DPU, diff_before);
        if (diff_before < 200) {  // すでに負荷分散出来ているのでこの先のペアは移動不要
            //printf("migration limit because the workload is already balanced, %d migration done\n", migration_per_batch - num_trees_to_be_migrated);
            break;
        }
        if (__builtin_popcount(tree_bitmap[from_DPU]) > 1) {  // 木が1つだけだったら移動しない
            int from_tree = 0;
            int min_diff_after = 1 << 30;
            for (int i = 0; i < MAX_NUM_TREES_IN_DPU; i++) {  // 最も負荷を分散出来る木を移動
                //printf("from_DPU=%d,tree=%d\n", from_DPU, i);
                int diff_after = std::abs((num_keys_for_DPU[from_DPU] - num_keys_for_tree[from_DPU][i]) - (num_keys_for_DPU[to_DPU] + num_keys_for_tree[from_DPU][i]));  // 移動後のクエリ数の差
                //printf("%d,%d,%d\n", i, diff_before, min_diff_after);
                if (diff_after < min_diff_after) {
                    from_tree = i;
                    min_diff_after = diff_after;
                }
            }
            int to_tree;
            for (int i = 0; i < MAX_NUM_TREES_IN_DPU; i++) {
                if (!(tree_bitmap[to_DPU] & (1 << i))) {                                                                         // i番目の木が空いているかどうか
                    if (num_keys_for_DPU[from_DPU] > num_keys_for_DPU[to_DPU] + 2 * (num_keys_for_tree[from_DPU][from_tree])) {  // 木を移動した結果、さらに偏ってしまう場合はのぞく
                        to_tree = i;
                        //printf("migration: (%d,%d)->(%d,%d)\n", from_DPU, from_tree, to_DPU, to_tree);
                        send_nodes_from_dpu_to_dpu(from_DPU, from_tree, to_DPU, to_tree, set, dpu);
                        num_keys_for_DPU[from_DPU] -= num_keys_for_tree[from_DPU][from_tree];
                        num_keys_for_DPU[to_DPU] += num_keys_for_tree[from_DPU][from_tree];
                        num_keys_for_tree[to_DPU][to_tree] = num_keys_for_tree[from_DPU][from_tree];
                        num_keys_for_tree[from_DPU][from_tree] = 0;
                        num_trees_to_be_migrated--;
                        migrated_tree_num++;
                        break;
                    }
                }
            }
        }
        count++;
    }
    gettimeofday(&end, NULL);
    migration_time = time_diff(&start, &end);

    // for (int i = 0; i < NR_DPUS; i++) {
    //     printf("after migration: DPU #%d has %d queries\n", i, num_keys_for_DPU[i]);
    // }
    gettimeofday(&start, NULL);
    // 4.key_index(i番目のDPUのj番目の木へのクエリの開始インデックス)の作成
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j <= MAX_NUM_TREES_IN_DPU; j++) {
            key_index[i][j] = 0;
        }
    }

    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < MAX_NUM_TREES_IN_DPU; j++) {
            //printf("after migration: num_keys[%d][%d] = %d\n", i, j, num_keys_for_tree[i][j]);
        }
    }
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 1; j <= MAX_NUM_TREES_IN_DPU; j++) {
            key_index[i][j] = key_index[i][j - 1] + num_keys_for_tree[i][j - 1];
            //printf("key_index[%d][%d] = %d\n", i, j, key_index[i][j]);
        }
    }

    // 5.requestsの作成
    for (int i = 0; i < key_count; i++) {
        auto it = key_to_tree_map.lower_bound(batch_keys[i]);
        if (it != key_to_tree_map.end()) {
            dpu_requests[it->second.first].requests[key_index[it->second.first][it->second.second]].key = batch_keys[i];
            dpu_requests[it->second.first].requests[key_index[it->second.first][it->second.second]++].write_val_ptr = batch_keys[i];
        } else {
            printf("ERROR: the key is out of range 4\n");
        }
    }

#ifdef PRINT_DEBUG
    for (int i = 0; i < NR_DPUS; i++)
        printf("num_keys[%d] = %d\n", i, num_keys_for_DPU[i]);
#endif
    /* count the number of requests for each DPU, determine the send size */
    send_size = 0;
    for (int dpu_i = 0; dpu_i < NR_DPUS; dpu_i++) {
        /* send size: maximum number of requests to a DPU */
        if (num_keys_for_DPU[dpu_i] > send_size)
            send_size = num_keys_for_DPU[dpu_i];
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
//     for (int tree_i = 0; tree_i < NUM_BPTREE_IN_DPU; tree_i++) {
//         if (dpu_requests[i].end_idx[tree_i] != 0) {
//             printf("tree %d first req:%ld,WRITE\n", tree_i, dpu_requests[i].requests[0].key);
//         }
//     }
// }


void execute_one_batch(struct dpu_set_t set, struct dpu_set_t dpu, uint64_t* task)
{
    // printf("\n");
    // printf("======= batch start=======\n");
    // show_requests(0);
    // gettimeofday(&start_total, NULL);
    // CPU→DPU
    // printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_IN_BATCH,
    // nr_of_dpus);
    gettimeofday(&start, NULL);
    send_requests(set, dpu, task);
    gettimeofday(&end, NULL);
#ifdef PRINT_DEBUG
    printf("[2/4] send finished\n");
#endif
    send_time = time_diff(&start, &end);

    /* execution */
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    gettimeofday(&end, NULL);

    dpu_sync(set);
#ifdef PRINT_DEBUG
    printf("[4/4]all dpus finished: %0.5fsec\n", time_diff(&start, &end_total));
#endif
    execution_time = time_diff(&start, &end);

    // DPU→CPU
}

int main(int argc, char* argv[])
{
    printf("\n");
    // printf("size of dpu_request_t:%lu,%lu\n", sizeof(dpu_request_t),
    // sizeof(dpu_requests[0])); printf("total num of
    // requests:%ld\n",(uint64_t)NUM_REQUESTS);
    cmdline::parser a;
    a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, 1000000);
    a.add<std::string>("zipfianconst", 'a', "zipfian consttant", false, "0.99");
    a.add<int>("migration_num", 'm', "migration_num per batch", false, 5);
    a.add<std::string>("directory", 'd', "execution directory, ex)bp-forest-exp", false, ".");
    a.parse_check(argc, argv);
    std::string zipfian_const = a.get<std::string>("zipfianconst");
    int max_key_num = a.get<int>("keynum");
    std::string file_name = (a.get<std::string>("directory") + "/workload/zipf_const_" + zipfian_const + ".bin");
    std::string dpu_binary = (a.get<std::string>("directory") + "/build/dpu/dpu_program");

#ifdef PRINT_DEBUG
    std::cout << "zipf_const:" << zipfian_const << ", file:" << file_name << std::endl;
#endif
    struct dpu_set_t set, dpu;
    DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &set));
    //printf("%s\n", dpu_binary.c_str());
    DPU_ASSERT(dpu_load(set, dpu_binary.c_str(), NULL));
    DPU_ASSERT(dpu_get_nr_dpus(set, &nr_of_dpus));
    int num_init_reqs = NUM_INIT_REQS;
    //printf("num_init_reqs=%d\n", num_init_reqs);
    // std::cout << "NUM Trees per DPU(init):" << NUM_TREES_PER_DPU << ", NUM Trees per DPU(max):" << MAX_NUM_TREES_IN_DPU << ", num total trees:" << NUM_TOTAL_TREES << ", NR_DPUS:" << NR_DPUS << ", NR_TASKLETS:" << NR_TASKLETS << std::endl;
    initialize_dpus(num_init_reqs, set, dpu);
    //printf("initialization finished\n");

#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", nr_of_dpus);

#endif
    /* load workload file */
    std::ifstream file_input(file_name, std::ios_base::binary);
    if (!file_input) {
        printf("cannot open file\n");
        return 1;
    }
    /* main routine */
    int num_keys = 0;
    gettimeofday(&start_total, NULL);
    printf("zipfian_const, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, migration_num, preprocess_time1, preprocess_time2, preprocess_time, migration_time, send_time, execution_time, batch_time, throughput\n");
    while (total_num_keys < max_key_num) {
        if (batch_num == 0)
            migration_per_batch = 0;  // batch 0: no migration
        else
            migration_per_batch = a.get<int>("migration_num");
        //printf("%d\n", num_keys);
        // std::cout << std::endl
        //           << "===== batch " << batch_num << " =====" << std::endl;
        if (max_key_num - total_num_keys >= NUM_REQUESTS_PER_BATCH) {
            num_keys = batch_preprocess(file_input, NUM_REQUESTS_PER_BATCH, set, dpu);
        } else {
            num_keys = batch_preprocess(file_input, (max_key_num - total_num_keys), set, dpu);
        }
        if (num_keys == 0)
            break;
        gettimeofday(&end, NULL);
        total_num_keys += num_keys;
#ifdef PRINT_DEBUG

        std::cout << "[1/4] " << num_keys << " requests generated" << std::endl;
        std::cout << "executing " << total_num_keys << "/" << max_key_num << "..." << std::endl;
#endif
        execute_one_batch(set, dpu, &task_get);
        batch_time = preprocess_time + migration_time + send_time + execution_time;
        total_preprocess_time += preprocess_time;
        total_migration_time += migration_time;
        total_send_time += send_time;
        total_execution_time += execution_time;
        total_batch_time += batch_time;
        double throughput = num_keys / batch_time;
        printf("%s, %d, %d, %d, %d, %d, %d, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            zipfian_const.c_str(), NR_DPUS, NR_TASKLETS, batch_num,
            num_keys, send_size, migrated_tree_num, preprocess_time1, preprocess_time2, preprocess_time, migration_time, send_time,
            execution_time, batch_time, throughput);
#ifdef PRINT_DEBUG
        DPU_FOREACH(set, dpu, each_dpu)
        {
            if (each_dpu == 0)
                DPU_ASSERT(dpu_log_read(dpu, stdout));
        }
#endif
#ifdef DEBUG_ON
        printf("results from DPUs: batch %d\n", total_num_keys / num_keys);
#endif
        free(dpu_requests);
        batch_num++;
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
    return 0;
}
