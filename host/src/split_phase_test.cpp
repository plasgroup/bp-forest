#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "cmdline.h"
#include "simulation_by_host.hpp"
#include <algorithm>
#include <assert.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <set>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <vector>
extern "C" {
#include "bplustree.h"
}

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#ifndef DPU_BINARY1
#define DPU_BINARY1 "./build/dpu/dpu_program"
#endif


dpu_requests_t dpu_requests(NUM_TOTAL_TREES);

int each_dpu;
int send_size;
uint64_t num_requests[NR_DPUS];
uint64_t total_num_keys;
uint64_t total_num_keys_dpu;
uint32_t nr_of_dpus1;
/* (tree_max_key,tree_id)) */
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

int generate_requests_fromfile(std::ifstream& fs, int n)
{
    /* read n keys from workload file */
    key_int64_t* batch_keys = (key_int64_t*)malloc(n * sizeof(key_int64_t));
    // std::cout << "malloc batch_keys" << std::endl;
    int key_count = 0;
    fs.read(reinterpret_cast<char*>(batch_keys), sizeof(batch_keys) * n);
    key_count = fs.tellg() / sizeof(key_int64_t) - total_num_keys;
    // std::cout << "key_count: " << key_count << std::endl;
    /* make dpu requests */
    for (int tree_i = 0; tree_i < MAX_NUM_BPTREE_IN_DPU; tree_i++) {
        dpu_requests.at(tree_i).clear();
        num_reqs_for_each_tree[tree_i] = 0;
    }
    for (int i = 0; i < key_count; i++) {
        key_int64_t key = batch_keys[i];
        int which_tree = key_to_tree(key);
        dpu_requests.at(which_tree).push_back({key, key, WRITE});
        num_reqs_for_each_tree[which_tree]++;
    }
    free(batch_keys);
    return key_count;
}

void execute_one_batch(dpu_set_t set1, dpu_set_t dpu1)
{
    /* execution */
    DPU_ASSERT(dpu_launch(set1, DPU_ASYNCHRONOUS));
    printf("finished\n");
    /* update tree index using split phase results */
    for (int i = 0; i < MAX_NUM_BPTREE_IN_DPU; i++) {
        int num_split = 0;
        for (int j = 0; j < MAX_NUM_SPLIT; j++) {
            if (split_result[i].new_tree_index[j] >= 0) {
                num_split++;
                int new_i = split_result[i].new_tree_index[j];
                key_to_tree_index.emplace(split_result[i].split_key[j], split_result[i].new_tree_index[j]);
                printf("tree_%d was split into ", i);
                printf("tree_%d(%d elems, border_key = %ld), ", new_i, i, split_result[i].num_elems[j], new_i, split_result[i].num_elems[j], split_result[i].split_key[j]);
            }
            printf("\n");
        }
    }
    int main(int argc, char* argv[])
    {
        printf("SPLIT_THRESHOLD: %ld\n", SPLIT_THRESHOLD);
        key_to_tree_index.emplace(0, 0);
        key_to_tree_index.emplace(std::numeric_limits<uint64_t>::max(), 0);
        // printf("\n");
        // printf("size of dpu_request_t:%lu,%lu\n", sizeof(dpu_request_t),
        // sizeof(dpu_requests[0])); printf("total num of
        // requests:%ld\n",(uint64_t)NUM_REQUESTS);
        cmdline::parser a;
        a.add<int>("keynum", 'n', "maximum num of keys for the experiment", false, 1000000);
        a.add<std::string>("zipfianconst", 'a', "zipfianconst", false, "0.5");
        a.parse_check(argc, argv);
        std::string zipfian_const = a.get<std::string>("zipfianconst");
        int max_key_num = a.get<int>("keynum");
        std::string file_name = ("../workload/zipf_const_" + zipfian_const + ".bin");
        std::cout << file_name << std::endl;
        struct dpu_set_t set1, dpu1;
        DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &set1));
        DPU_ASSERT(dpu_load(set1, DPU_BINARY1, NULL));
        /* load workload file */
        std::ifstream file_input(file_name, std::ios_base::binary);
        if (!file_input) {
            printf("cannot open file\n");
            return 1;
        }
        /* main routine */
        int batch_num = 0;
        int num_keys = 0;
        while (total_num_keys < max_key_num) {
            // printf("%d\n", num_keys);
            if (max_key_num - total_num_keys >= NUM_REQUESTS_PER_BATCH) {
                num_keys = generate_requests_fromfile(file_input, NUM_REQUESTS_PER_BATCH);
            } else {
                num_keys = generate_requests_fromfile(file_input, (max_key_num - total_num_keys));
            }
            if (num_keys == 0)
                break;
            total_num_keys += num_keys;
            std::cout << std::endl
                      << "===== batch " << batch_num << " =====" << std::endl;
            std::cout << num_keys << " requests generated" << std::endl;
            std::cout << "executing " << total_num_keys << "/" << max_key_num << "..." << std::endl;
            execute_one_batch(set1, dpu1);
            batch_num++;
        }
        DPU_ASSERT(dpu_free(set1));
        return 0;
    }
