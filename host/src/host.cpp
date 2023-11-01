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

#define NUM_TOTAL_INIT_TREES (NR_DPUS * NUM_INIT_TREES_IN_DPU)
#ifndef NUM_INIT_REQS
#define NUM_INIT_REQS (1000 * NUM_TOTAL_INIT_TREES)
#endif
#define PRINT_POSITION_AND_VARIABLE(NAME,FORMAT) \
    printf("[Debug]%s:%d\n", __FILE__, __LINE__); \
    printf("[Debug]" #NAME" = " #FORMAT "\n", NAME);
#define PRINT_POSITION_AND_MESSAGE(MESSAGE) \
    printf("[Debug]%s:%d\n", __FILE__, __LINE__); \
    printf("[Debug]" #MESSAGE"\n");
#define GET_AND_PRINT_TIME(CODES, LABEL) \
    gettimeofday(&start, NULL);          \
    CODES                                \
    gettimeofday(&end, NULL);            \
    printf("time spent for %s: %0.8f sec\n", #LABEL, time_diff(&start, &end));

dpu_requests_t* dpu_requests;
dpu_results_t* dpu_results;

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
constexpr key_int64_t RANGE = std::numeric_limits<uint64_t>::max() / (NUM_TOTAL_INIT_TREES);

const uint64_t task_from = TASK_FROM;
const uint64_t task_to = TASK_TO;
const uint64_t task_init = TASK_INIT;
const uint64_t task_get = TASK_GET;
const uint64_t task_insert = TASK_INSERT;
const uint64_t task_invalid = 999 + (1ULL << 32);
int migration_per_batch;
uint64_t tree_bitmap[NR_DPUS] = {0};
std::map<key_int64_t, std::pair<int, int>> key_to_tree_map;
key_int64_t tree_to_key_map[NR_DPUS][NR_SEATS_IN_DPU];
int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1] = {0};
BPTreeNode nodes_buffer[MAX_NUM_NODES_IN_SEAT];
uint64_t nodes_num;
std::pair<int,int> migration_dest[NR_DPUS][NR_SEATS_IN_DPU];
split_info_t split_result[NR_DPUS][NR_SEATS_IN_DPU];
void init_key_to_tree_map()
{
    for (int i = 0; i < NUM_TOTAL_INIT_TREES; i++) {
        key_to_tree_map[RANGE * (i + 1)] = std::make_pair(i / NUM_INIT_TREES_IN_DPU, i % NUM_INIT_TREES_IN_DPU);
    }
}

void init_tree_to_key_map()
{
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < NUM_INIT_TREES_IN_DPU; j++)
            tree_to_key_map[i][j] = RANGE * (i * NUM_INIT_TREES_IN_DPU + j + 1);
    }
}

void init_tree_bitmap()
{
    for (uint64_t i = 0; i < NR_DPUS; i++) {
        tree_bitmap[i] = (1 << NUM_INIT_TREES_IN_DPU) - 1;
    }
}

void init_migration_dest()
{
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < NR_SEATS_IN_DPU; j++)
            migration_dest[i][j] = std::make_pair<int,int>(-1,-1);
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

void send_requests(struct dpu_set_t set, struct dpu_set_t dpu, const uint64_t* task)
{
    DPU_ASSERT(dpu_broadcast_to(set, "task_no", 0, task, sizeof(uint64_t), DPU_XFER_DEFAULT));
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &key_index[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "end_idx", 0, sizeof(int) * (NR_SEATS_IN_DPU), DPU_XFER_DEFAULT));
#ifdef PRINT_DEBUG
    printf("[INFO at %s:%d] send_size: %ld / buffer_size: %ld\n", __FILE__, __LINE__, sizeof(each_request_t) * send_size, sizeof(each_request_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &dpu_requests[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "request_buffer", 0, sizeof(each_request_t) * send_size, DPU_XFER_DEFAULT));
}

void receive_results(struct dpu_set_t set, struct dpu_set_t dpu){
#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(dpu_results_t) * send_size, sizeof(dpu_results_t) * MAX_REQ_NUM_IN_A_DPU);
#endif
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &dpu_results[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "result", 0, sizeof(each_result_t) * send_size, DPU_XFER_DEFAULT));
}

#ifdef DEBUG_ON
void check_results(dpu_results_t* dpu_results, int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1]){
    for(int dpu = 0; dpu < NR_DPUS; dpu++){
        for(int seat = 0; seat < NR_SEATS_IN_DPU; seat++)[
            for(int index = seat==0 ? 0 : key_index[seat-1]; idnex < key_index[seat]; index++){
                assert(dpu_results[dpu].results[index].get_result == dpu_requests[dpu].requests[index].write_value_ptr);
            }
        ]
    }
}
#endif

void send_nodes_from_dpu_to_dpu(int from_DPU, int from_tree, int to_DPU, int to_tree, dpu_set_t set, dpu_set_t dpu)
{
    uint64_t task;
    DPU_FOREACH(set, dpu, each_dpu)
    {
        //printf("ed = %d\n", each_dpu);
        if (each_dpu == from_DPU) {
            task = (TASK_FROM << 32) | from_tree;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            //TODO: nodes_bufferを増やす
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
            task = (TASK_TO << 32) | to_tree;
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
    init_migration_dest();
    dpu_requests
        = (dpu_requests_t*)malloc((NR_DPUS) * sizeof(dpu_requests_t));
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "]heap size is not enough\n");
        return;
    }
    key_int64_t* keys = (key_int64_t*)malloc(num_init_reqs * sizeof(key_int64_t));
    key_int64_t interval = (key_int64_t)std::numeric_limits<uint64_t>::max() / num_init_reqs;
    //printf("interval = %ld\n", interval);
    int num_keys_for_tree[NR_DPUS][NR_SEATS_IN_DPU] = {0};
    int num_keys_for_DPU[NR_DPUS] = {0};
    //printf("NR_SEATS_IN_DPU = %d\n", NR_SEATS_IN_DPU);
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
        for (int j = 0; j <= NR_SEATS_IN_DPU; j++) {
            key_index[i][j] = 0;
        }
    }
    /* key_index(i番目のDPUのj番目の木へのクエリの開始インデックス)の作成 */
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
            //printf("num_keys_for_tree[%d][%d] = %d\n", i, j, num_keys_for_tree[i][j]);
        }
    }
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 1; j <= NR_SEATS_IN_DPU; j++) {
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
        for (int j = 0; j <= NR_SEATS_IN_DPU; j++) {
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
    send_requests(set, dpu, &task_insert);
    //printf("sent reqs\n");
    gettimeofday(&start, NULL);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    dpu_sync(set);

#ifdef PRINT_DEBUG
    DPU_FOREACH(set, dpu, each_dpu)
    {
        if (each_dpu == 0)
            DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
    PRINT_POSITION_AND_MESSAGE(inserted initial keys);
#endif
#ifdef DEBUG_ON
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));
    receive_results(set,dpu);
    check_results(dpu_results, key_index);
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

void recieve_split_info(struct dpu_set_t set, struct dpu_set_t dpu){
    DPU_FOREACH(set, dpu, each_dpu)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &split_result[each_dpu]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "split_result", 0,
        sizeof(split_info_t) * NR_SEATS_IN_DPU,
        DPU_XFER_DEFAULT));
}

void update_cpu_struct(){
    for(int dpu = 0; dpu < NR_DPUS; dpu++){
        for(int old_tree = 0; old_tree < NR_SEATS_IN_DPU; old_tree++){
            for(int new_tree = 0; new_tree < MAX_NUM_SPLIT; new_tree++){
                key_int64_t border_key = split_result[dpu][old_tree].split_key[new_tree];
                int new_slot = split_result[dpu][old_tree].new_tree_index[new_tree];
                if(new_slot != 0){
                    key_to_tree_map[border_key] = std::make_pair(dpu, new_slot);
                    tree_to_key_map[dpu][new_slot] = border_key;
                }
            }
        }
    }
}

/* make batch, do migration, prepare queries for dpus */
int batch_preprocess(std::ifstream& fs, int n, uint64_t &total_num_keys, int num_migration, struct dpu_set_t set, struct dpu_set_t dpu)
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
    /* 1. それぞれの木へのクエリ数を数える */
    int num_keys_for_tree[NR_DPUS][NR_SEATS_IN_DPU] = {0};
    int num_keys_for_DPU[NR_DPUS] = {0};
    for (int i = 0; i < key_count; i++) {
        //printf("i: %d, batch_keys[i]:%ld\n", i, batch_keys[i]);
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
        for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
            //printf("num_keys[%d][%d] = %d\n", i, j, num_keys_for_tree[i][j]);
        }
    }
    gettimeofday(&end, NULL);
    preprocess_time1 = time_diff(&start, &end);
    // 3.木のmigration
    migrated_tree_num = 0;
    gettimeofday(&start, NULL);
    int num_trees_to_be_migrated = num_migration;  // 何個の木を移動するか
    int count = 0;                                       // チェックしたDPUの数(移動しなかった場合も含む)
    while (num_trees_to_be_migrated) {
        if (count >= NR_DPUS - 1 - (num_migration - num_trees_to_be_migrated)) {
            //printf("migration limit because of NR_DPUS, %d migration done\n", num_migration - num_trees_to_be_migrated);
            break;
        }
        int from_DPU = idx[count];
        int to_DPU = idx[NR_DPUS - 1 - (num_migration - num_trees_to_be_migrated)];
        int diff_before = num_keys_for_DPU[from_DPU] - num_keys_for_DPU[to_DPU];
        //printf("from_DPU=%d,to_DPU=%d,diff_before=%d\n", from_DPU, to_DPU, diff_before);
        if (diff_before < 200) {  // すでに負荷分散出来ているのでこの先のペアは移動不要
            //printf("migration limit because the workload is already balanced, %d migration done\n", num_migration - num_trees_to_be_migrated);
            break;
        }
        if (__builtin_popcount(tree_bitmap[from_DPU]) > 1) {  // 木が1つだけだったら移動しない
            int from_tree = 0;
            int min_diff_after = 1 << 30;
            for (int i = 0; i < NR_SEATS_IN_DPU; i++) {  // 最も負荷を分散出来る木を移動
                //printf("from_DPU=%d,tree=%d\n", from_DPU, i);
                int diff_after = std::abs((num_keys_for_DPU[from_DPU] - num_keys_for_tree[from_DPU][i]) - (num_keys_for_DPU[to_DPU] + num_keys_for_tree[from_DPU][i]));  // 移動後のクエリ数の差
                //printf("%d,%d,%d\n", i, diff_before, min_diff_after);
                if (diff_after < min_diff_after) {
                    from_tree = i;
                    min_diff_after = diff_after;
                }
            }
            int to_tree;
            for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
                if (!(tree_bitmap[to_DPU] & (1 << i))) {                                                                         // i番目の木が空いているかどうか
                    if (num_keys_for_DPU[from_DPU] > num_keys_for_DPU[to_DPU] + 2 * (num_keys_for_tree[from_DPU][from_tree])) {  // 木を移動した結果、さらに偏ってしまう場合はのぞく
                        to_tree = i;
                        //printf("migration: (%d,%d)->(%d,%d)\n", from_DPU, from_tree, to_DPU, to_tree);
                        migration_dest[from_DPU][from_tree] = std::make_pair(to_DPU,to_tree);
                        // send_nodes_from_dpu_to_dpu(from_DPU, from_tree, to_DPU, to_tree, set, dpu);
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
    for(int i = 0; i<NR_DPUS;i++){
        for(int j = 0; j < NR_SEATS_IN_DPU;j++){
            auto& to = migration_dest[i][j];
            if(to.first!=-1 && to.first != i){
                send_nodes_from_dpu_to_dpu(i, j, to.first, to.second, set, dpu);
                migration_dest[i][j] = std::make_pair(-1,-1);
            }
        }
    }
    gettimeofday(&end, NULL);
    migration_time = time_diff(&start, &end);

    // for (int i = 0; i < NR_DPUS; i++) {
    //     printf("after migration: DPU #%d has %d queries\n", i, num_keys_for_DPU[i]);
    // }
    gettimeofday(&start, NULL);
    // 4.key_index(i番目のDPUのj番目の木へのクエリの開始インデックス)の作成
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j <= NR_SEATS_IN_DPU; j++) {
            key_index[i][j] = 0;
        }
    }

    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
            //printf("after migration: num_keys[%d][%d] = %d\n", i, j, num_keys_for_tree[i][j]);
        }
    }
    for (int i = 0; i < NR_DPUS; i++) {
        for (int j = 1; j <= NR_SEATS_IN_DPU; j++) {
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

int do_one_batch(int batch_num, int migrations_per_batch, uint64_t &total_num_keys, const int max_key_num, std::ifstream& file_input, const uint64_t* task, dpu_set_t set, dpu_set_t dpu){
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
    if (batch_num == 0){
        num_migration = 0;  // batch 0: no migration
    } else {
        num_migration = migrations_per_batch;
    }
    if (max_key_num - total_num_keys >= NUM_REQUESTS_PER_BATCH) {
        num_keys = batch_preprocess(file_input, NUM_REQUESTS_PER_BATCH, total_num_keys, num_migration, set, dpu);
    } else {
        num_keys = batch_preprocess(file_input, (max_key_num - total_num_keys), total_num_keys, num_migration, set, dpu);
    }
    PRINT_POSITION_AND_VARIABLE(num_keys, %d);
    if (num_keys == 0) {
        free(dpu_requests);
        return 0;
    }
#ifdef PRINT_DEBUG
    printf("[1/4] preprocess finished %0.5fsec\n", preprocess_time);
#endif

    // show_requests(0);
    // CPU→DPU
#ifdef PRINT_DEBUG
    printf("sending %d requests for %d DPUS...\n", NUM_REQUESTS_PER_BATCH,
    nr_of_dpus);
#endif
/* query deliver */
    gettimeofday(&start, NULL);
    send_requests(set, dpu, task);
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
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

/* postprocess */
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));
    gettimeofday(&start, NULL);
    if (*task == TASK_INSERT){
        // recieve_split_info(set, dpu);
        // update_cpu_struct();
    }
    if (*task == TASK_GET){
        receive_results(set,dpu);
#ifdef DEBUG_ON
        check_results(dpu_results, key_index);
#endif
    }
    gettimeofday(&end, NULL);
    free(dpu_results);
#ifdef PRINT_DEBUG
    printf("[4/4] batch postprocess finished: %0.5fsec\n", time_diff(&start, &end));
#endif


#ifdef PRINT_DEBUG
        // DPU_FOREACH(set, dpu, each_dpu)
        // {
        //     if (each_dpu == 0)
        //         DPU_ASSERT(dpu_log_read(dpu, stdout));
        // }
#endif
#ifdef DEBUG_ON
        printf("results from DPUs: batch %d\n", total_num_keys / num_keys);
#endif
    free(dpu_requests);
    PRINT_POSITION_AND_VARIABLE(num_keys, %d);
    return num_keys;
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
    a.add<std::string>("directory", 'd', "execution directory, offset from bp-forest directory. ex)bp-forest-exp", false, ".");
    a.add("simulator", 's', "if declared, the binary for simulator is used");
    a.parse_check(argc, argv);
    std::string zipfian_const = a.get<std::string>("zipfianconst");
    const int max_key_num = a.get<int>("keynum");
    std::string file_name = (a.get<std::string>("directory") + "/workload/zipf_const_" + zipfian_const + ".bin");
    std::string dpu_binary;
    if(a.exist("simulator")){
        dpu_binary = a.get<std::string>("directory") + "/build_simulator/dpu/dpu_program";
    } else {
        dpu_binary = a.get<std::string>("directory") + "/build_UPMEM/dpu/dpu_program";
    }

#ifdef PRINT_DEBUG
    std::cout << "[INFO] zipf_const:" << zipfian_const << ", workload file:" << file_name << std::endl;
#endif
    struct dpu_set_t set, dpu;
    // printf("[Debug]%s:%d\n", __FILE__, __LINE__);
    if(a.exist("simulator")){
        DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set));
    } else{
        DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &set));
    }
    //printf("%s\n", dpu_binary.c_str());

    DPU_ASSERT(dpu_load(set, dpu_binary.c_str(), NULL));
    DPU_ASSERT(dpu_get_nr_dpus(set, &nr_of_dpus));
#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", nr_of_dpus);
#endif
    int num_init_reqs = NUM_INIT_REQS;
    //printf("num_init_reqs=%d\n", num_init_reqs);
    // std::cout << "NUM Trees per DPU(init):" << NR_SEATS_IN_DPU << ", NUM Trees per DPU(max):" << NR_SEATS_IN_DPU << ", num total trees:" << NUM_TOTAL_TREES << ", NR_DPUS:" << NR_DPUS << ", NR_TASKLETS:" << NR_TASKLETS << std::endl;
    initialize_dpus(num_init_reqs, set, dpu);
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
    total_num_keys = 0;
    int migrations_per_batch = a.get<int>("migration_num");
    printf("zipfian_const, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, migration_num, preprocess_time1, preprocess_time2, preprocess_time, migration_time, send_time, execution_time, batch_time, throughput\n");
    while (total_num_keys < max_key_num) {
        num_keys = do_one_batch(batch_num, migrations_per_batch, total_num_keys, max_key_num, file_input, &task_get, set, dpu);
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
            num_keys, send_size, migrated_tree_num, preprocess_time1, preprocess_time2, preprocess_time, migration_time, send_time,
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
    return 0;
}
