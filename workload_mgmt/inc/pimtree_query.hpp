#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>

enum operation_t {
    empty_t,
    get_t,
    update_t,
    predecessor_t,
    scan_t,
    insert_t,
    remove_t
};
struct get_operation {
    int64_t key;
};
struct update_operation {
    int64_t key;
    int64_t value;
};
struct predecessor_operation {
    int64_t key;
};

struct scan_operation {
    int64_t lkey;
    int64_t rkey;
};

struct insert_operation {
    int64_t key;
    int64_t value;
};

struct remove_operation {
    int64_t key;
};

struct operation {
    union {
        get_operation g;
        update_operation u;
        predecessor_operation p;
        scan_operation s;
        insert_operation i;
        remove_operation r;
    } tsk;
    operation_t type;
};

struct pimtree_queries {
    operation* ops;
    size_t length;
};

inline pimtree_queries make_pimtree_queries(std::string filepath);
inline void show_pimtree_queries(pimtree_queries queries);

#include "pimtree_query.ipp"
