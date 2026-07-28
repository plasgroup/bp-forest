#include <vector>
#include "common.h"
#include "pimtree_query.hpp"


template <typename K, typename V>
std::vector<std::pair<K, V>> load_init_data(const std::string &file_name)
{
    std::vector<std::pair<K, V>> kvs;
    pimtree_queries init_data = make_pimtree_queries(file_name);
    for (size_t i = 0; i < init_data.length; i++) {
        if (init_data.ops[i].type != insert_t) {
            fprintf(stderr, "invalid operation type in init file\n");
            exit(1);
        }
        auto& item = init_data.ops[i].tsk.i;
        kvs.push_back({(K) item.key, (V) item.value});
    }
    return kvs;
}

template <typename K>
std::vector<K> load_point_workload(const std::string &file_name, operation_t op_type = get_t)
{
    std::vector<K> workload;
    pimtree_queries queries = make_pimtree_queries(file_name);
    for (size_t i = 0; i < queries.length; i++) {
        if (queries.ops[i].type != op_type) {
            fprintf(stderr, "invalid operation type in workload file\n");
            exit(1);
        }
        if (op_type == predecessor_t)
            workload.push_back(queries.ops[i].tsk.p.key);
        else
            workload.push_back(queries.ops[i].tsk.g.key);
    }
    return workload;
}

template <typename K>
std::vector<std::pair<K, K>> load_range_workload(const std::string &file_name)
{
    std::vector<std::pair<K, K>> workload;
    pimtree_queries queries = make_pimtree_queries(file_name);
    for (size_t i = 0; i < queries.length; i++) {
        if (queries.ops[i].type != scan_t) {
            fprintf(stderr, "invalid operation type in workload file\n");
            exit(1);
        }
        workload.push_back({queries.ops[i].tsk.s.lkey, queries.ops[i].tsk.s.rkey});
    }
    return workload;
}

template <typename K, typename V>
void save_init_data(const std::string &file_name, const std::vector<std::pair<K, V>> &kvs)
{
    struct operation op;
    memset(&op, 0, sizeof(op));
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }

    for (auto [key, value]: kvs) {
        op.type = insert_t;
        op.tsk.i.key = (int64_t) key;
        op.tsk.i.value = (int64_t) value;
        fwrite(&op, sizeof(op), 1, fp);
    }
    fclose(fp);
}

template <typename K>
void save_point_workload(const std::string &file_name, const std::vector<K> &keys, operation_t op_type = get_t)
{
    struct operation op;
    memset(&op, 0, sizeof(op));
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    for (auto key: keys) {
        op.type = op_type;
        if (op_type == predecessor_t)
            op.tsk.p.key = (int64_t) key;
        else
            op.tsk.g.key = (int64_t) key;
        fwrite(&op, sizeof(op), 1, fp);
    }
    fclose(fp);
}

// both inclusive
template <typename K>
void save_range_workload(const std::string & file_name, const std::vector<std::pair<K, K>> &ranges)
{
    struct operation op;
    memset(&op, 0, sizeof(op));
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    for (auto [lkey, rkey]: ranges) {
        op.type = scan_t;
        op.tsk.s.lkey = (int64_t) lkey;
        op.tsk.s.rkey = (int64_t) rkey;
        fwrite(&op, sizeof(op), 1, fp);
    }
    fclose(fp);
}