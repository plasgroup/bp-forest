#include <cmdline.h>
#include <random>
#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"

struct Option {
    cmdline::parser a;
    void parse(int argc, char* argv[])
    {
        a.add<std::string>("pimtree-workload-file", 'w', "file path to PIM-Tree workload file", false);
        a.add<std::string>("pimtree-init-file", 'i', "file path to PIM-Tree init file", false);
       
        a.add<double>("alpha", 'a', "zipf alpha", false, 0.99);
        a.add<double>("items", 'n', "number of items in millions", false, 500.0);
        a.add<int>("queries", 'q', "number of queries", false, 1024 * 1024);
        a.add<int>("slices", 's', "number of slices", false, 1024 * 10);

        a.add<int>("max-items-per-dpu", 'm', "maximum number of items per DPU", false, 40 * 1000);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add<int>("nr-dpus", 'p', "number of DPUs", false, 2500);
        a.parse_check(argc, argv);
    }

    const std::string& workload_file() {
        return a.get<std::string>("pimtree-workload-file");
    }

    const std::string& init_file() {
        return a.get<std::string>("pimtree-init-file");
    }

    int max_items_per_dpu() {
        return a.get<int>("max-items-per-dpu");
    }
    
    int nr_dpus() {
        return a.get<int>("nr-dpus");
    }

    double alpha() {
        return a.get<double>("alpha");
    }

    int num_slices() {
        return a.get<int>("slices");
    }

    size_t items() {
        return a.get<double>("items") * 1000 * 1000;
    }

    int queries() {
        return a.get<int>("queries");
    }

    operation_t op_type() {
        if (a.get<std::string>("ops") == "get")
            return get_t;
        else if (a.get<std::string>("ops") == "rmq")
            return scan_t;
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
            return empty_t;
        }
    }
} opt;

#ifdef PIM_TREE
std::vector<int64_t> load_keys(const std::string &init_file)
{
    std::vector<int64_t> keys;
    pimtree_queries init_data = make_pimtree_queries(opt.init_file());
    for (size_t i = 0; i < init_data.length; i++) {
        if (init_data.ops[i].type != insert_t) {
            fprintf(stderr, "invalid operation type in init file\n");
            exit(1);
        }
        keys.push_back(init_data.ops[i].tsk.i.key);
    }
    return keys;
}

std::vector<int64_t> load_queries(const std::string &workload_file)
{
    std::vector<int64_t> workload;
    pimtree_queries queries = make_pimtree_queries(opt.workload_file());
    for (size_t i = 0; i < queries.length; i++) {
        if (queries.ops[i].type != get_t) {
            fprintf(stderr, "invalid operation type in workload file\n");
            exit(1);
        }
        workload.push_back(queries.ops[i].tsk.g.key);
    }
    std::sort(workload.begin(), workload.end());
    return workload;
}
#else // PIM_TREE

#define KEY_MIN INT64_MIN
#define KEY_MAX INT64_MAX

std::vector<int64_t> keys_to_generate_queries;
std::vector<int64_t> load_keys(const std::string &init_file)
{
    size_t key_interval = KEY_MAX / (opt.items() - 1);
    std::vector<int64_t> keys;
    keys.reserve(opt.items());
    for (int64_t i = 0; i < opt.items(); i++) {
        keys.push_back(KEY_MIN + i * key_interval);
    }
    keys_to_generate_queries = keys;
    return keys;
}

std::vector<double> init_zipf_pos(size_t P, double alpha) {
    double sum = 0;
    std::vector<double> zipf_pos(P, 1.0);
    for (size_t i = 0; i < P; i++) {
        zipf_pos[i] /= pow((double) (i + 1), alpha);
        sum += zipf_pos[i];
    }
    for (size_t i = 0; i < P; i++) {
        zipf_pos[i] /= sum;
        if (i > 0) {
            zipf_pos[i] += zipf_pos[i - 1];
        }
    }
    return zipf_pos;
}

std::vector<size_t> zipf_over_items(size_t P, size_t ds_size, double alpha, size_t n) {
    std::random_device rnd;
    std::mt19937_64 mt(rnd());
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    std::vector<double> zipf_pos = init_zipf_pos(P, alpha);

    std::vector<size_t> order(P); // rank -> slice-id
    for (size_t i = 0; i < P; i++)
        order[i] = i;
    for (size_t i = 0; i < P; i++) {
        size_t r = std::uniform_int_distribution<uint64_t>(0, P)(mt) % (P - i);
        std::swap(order[i], order[i + r]);
    }
//    auto order = tabulate(P, [&](size_t i) { return i; });
//    for (int i = 0; i < P; i++) {
//        int r = abs(rn_gen::parallel_rand()) % (P - i);
//        swap(order[i], order[i + r]);
//    }

    std::vector<size_t> slice_id(n);  // query-index -> slice-id
    for (size_t i = 0; i < n; i++) {
        double rd = dist(mt);
        int l = -1, r = ((int) P) - 1;  // (]
        while (r - l > 1) {
            int mid = (l + r) >> 1;
            if (zipf_pos[mid] < rd) {
                l = mid;
            } else {
                r = mid;
            }
        }
        slice_id[i] = order[r];        
    }

    std::vector<size_t> ids(n);  // query-index -> item-id
    for (size_t i = 0; i < n; i++) {
        uint64_t rd = std::uniform_int_distribution<uint64_t>(0, ds_size)(mt);
        size_t left = (ds_size / P) * slice_id[i];
        size_t item_id = left + rd / P + 1;  // why +1?
        if (item_id >= ds_size)
            item_id = ds_size - 1;
        ids[i] = item_id;
    }

    return ids;
}

std::vector<int64_t> load_point_queries(const std::string &workload_file)
{
    std::vector<size_t> ids = zipf_over_items(opt.num_slices(), opt.items(), opt.alpha(), opt.queries());
    std::vector<int64_t> queries;
    for (size_t i: ids)
        queries.push_back(keys_to_generate_queries[i]);
    std::sort(queries.begin(), queries.end());
    return queries;
}

std::vector<std::pair<int64_t, int64_t>> load_range_queries(const std::string &workload_file)
{
    std::vector<size_t> ids = zipf_over_items(opt.num_slices(), opt.items(), opt.alpha(), opt.queries());
    std::vector<std::pair<int64_t, int64_t>> queries;
    for (size_t i: ids) {
        size_t left = keys_to_generate_queries[i];
        size_t right = left + (KEY_MAX / (opt.items() - 1) * 100);
        queries.push_back(std::make_pair(left, right));
    }
    return queries;
}
#endif // PIM_TREE

size_t compute_need_dpus_point(std::vector<int64_t> keys, std::vector<int64_t> workload, size_t max_items_per_dpu, size_t max_queries_per_dpu)
{
    size_t p = 0;
    size_t idx_key = 0;
    size_t idx_workload = 0;
    size_t nkeys = 0;
    size_t nqueries = 0;
    printf("max_items_per_dpu = %d, max_queries_per_dpu = %d, keys = %d, min dpus = %d\n", max_items_per_dpu, max_queries_per_dpu, keys.size(), keys.size() / max_items_per_dpu);
    while (idx_key < keys.size()) {
        int nq = 0;
        while (idx_workload < workload.size() && workload[idx_workload] == keys[idx_key]) {
            nq++;
            idx_workload++;
        }
        if (nqueries > 0 && nqueries + nq > max_queries_per_dpu) {
//            printf("Query limit: [%d] %d k = %d, q = %d\n", idx_key - 1, keys[idx_key - 1], nkeys, nqueries);
            p++;
            nkeys = 0;
            nqueries = 0;
        }
        nkeys++;
        nqueries += nq;
        if (nkeys == max_items_per_dpu) {
//            printf("Key limit: [%d] %d k = %d, q = %d\n", idx_key, keys[idx_key], nkeys, nqueries);
            p++;
            nkeys = 0;
            nqueries = 0;
        }
        idx_key++;
    }
    if (nkeys > 0) {
//        printf("End: [%d] %d k = %d, q = %d\n", idx_key, keys[idx_key], nkeys, nqueries);
        p++;
    }
//    printf("\n");
    return p;
}

size_t compute_need_dpus_range(std::vector<int64_t> keys, std::vector<int64_t> ls, std::vector<int64_t> rs, size_t max_items_per_dpu, size_t max_queries_per_dpu)
{
    size_t p = 0;
    size_t idx_key = 0;
    size_t idx_l = 0;
    size_t idx_r = 0;
    size_t nkeys = 0;
    size_t nqueries = 0;
    size_t ncovering = 0;
    bool not_changed = true;
    while (idx_key < keys.size()) {
        int nq = 0;
        while (idx_l < ls.size() && ls[idx_l] == keys[idx_key]) {
            nq++;
            idx_l++;
        }
        if (!not_changed && nq > 0 && nqueries + nq > max_queries_per_dpu) {
//            printf("Query limit: [%d] %d k = %d, q = %d, ncovering = %d\n", idx_key - 1, keys[idx_key - 1], nkeys, nqueries, ncovering);
            p++;
            nkeys = 0;
            nqueries = ncovering;
        }
        if (nkeys > 0 && nkeys + 1 > max_items_per_dpu) {
//            printf("Key limit: [%d] %d k = %d q = %d, ncovering = %d\n", idx_key - 1, keys[idx_key - 1], nkeys, nqueries, ncovering);
            p++;
            nkeys = 0;
            nqueries = ncovering;
        }
        
        nkeys++;
        nqueries += nq;
        ncovering += nq;
        if (nq > 0)
            not_changed = false;
        if (nkeys == 1)
            not_changed = true;

        while (idx_r < rs.size() && rs[idx_r] == keys[idx_key]) {
            ncovering--;
            not_changed = false;
            idx_r++;
        }

        idx_key++;
    }
    if (nkeys > 0) {
//        printf("End: [%d] %d k = %d q = %d, ncovering = %d\n", idx_key, keys[idx_key], nkeys, nqueries, ncovering);
        p++;
    }
    printf("\n");
    return p;
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    std::vector<int64_t> keys = load_keys(opt.init_file());

    if (opt.op_type() == get_t) {
        std::vector<int64_t> workload = load_point_queries(opt.workload_file());

//        printf("workload: ");
//        for (int i = 0; i < workload.size(); i++)
//            printf("%ld ", workload[i]);
//        printf("\n");

        int l = 1, r = workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            printf("start_computation with m = %d\n", m);
            int p = compute_need_dpus_point(keys, workload, opt.max_items_per_dpu(), m);
            printf("need %d dpus\n", p);
            if (p > opt.nr_dpus())
                l = m + 1;
            else
                r = m;
        }
        printf("query limit = %d\n", l);
    } else if (opt.op_type() == scan_t) {
        std::vector<std::pair<int64_t, int64_t>> workload = load_range_queries(opt.workload_file());

        std::vector<int64_t> ls;
        std::vector<int64_t> rs;
        for (int i = 0; i < workload.size(); i++) {
            ls.push_back(workload[i].first);
            rs.push_back(workload[i].second);
        }
        std::sort(ls.begin(), ls.end());
        std::sort(rs.begin(), rs.end());

/*
        printf("ls: ");
        for (int i = 0; i < ls.size(); i++)
            printf(" %d", ls[i]);
        printf("\n");
        printf("rs: ");
        for (int i = 0; i < rs.size(); i++)
            printf(" %d", rs[i]);
        printf("\n");
*/
        int l = 1, r = workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            printf("start_computation with m = %d\n", m);
            int p = compute_need_dpus_range(keys, ls, rs, opt.max_items_per_dpu(), m);
            printf("need %d dpus\n", p);
            if (p > opt.nr_dpus())
                l = m + 1;
            else
                r = m;
        }
        printf("query limit = %d\n", l);
    }
}