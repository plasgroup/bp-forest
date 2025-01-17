#include <cmdline.h>
#include <random>
#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"
#include "host/inc/statistics.hpp"
#include <queue>

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
        return (size_t)(a.get<double>("items") * 1000 * 1000);
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
    for (size_t i = 0; i < opt.items(); i++) {
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
#if 0
    for (size_t i = 0; i < P; i++) {
        size_t r = std::uniform_int_distribution<uint64_t>(0, P)(mt) % (P - i);
        std::swap(order[i], order[i + r]);
    }
#endif // 0
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

class Partitioner {
public:
    // left and right indeces of keys, left-inclusive.
    using partition_t = std::pair<int, int>;
    constexpr static partition_t INVALID_PARTITION = {-1, -1};

    virtual ~Partitioner() {}
    virtual std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus) = 0;
    virtual std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus) = 0;
};

constexpr Partitioner::partition_t Partitioner::INVALID_PARTITION;

class OraclePartitioner : public Partitioner {
    size_t max_items_per_dpu;

    // ls and rs are lists of left and right ends of ranges.  They must be sorted.
    // Ranges are both inclusive.
    // Returns the number of required DPUs and, if count_only is false, the number of elements in each DPU.
    std::pair<size_t, std::vector<size_t>>
    trial_pertition(std::vector<int64_t>& keys, std::vector<int64_t>* ls, std::vector<int64_t>* rs, size_t max_items_per_dpu, size_t max_queries_per_dpu,  bool count_only)
    {
        std::vector<size_t> elms;
        size_t ndpus = 0;
        size_t idx_l = 0;
        size_t idx_r = 0;
        size_t nkeys = 0;
        size_t nqueries = 0;
        size_t ncovering = 0;
        size_t sea_level = 0;

        for (size_t idx_key = 0; idx_key < keys.size(); idx_key++) {
            int nq = 0;
            while (idx_l < ls->size() && (*ls)[idx_l] == keys[idx_key]) {
                nq++;
                idx_l++;
            }
            if ((nq > 0 && nqueries > 0 && sea_level + nqueries + nq > max_queries_per_dpu) ||
                nkeys == max_items_per_dpu) {
                ndpus++;
                if (!count_only)
                    elms.push_back(nkeys);
                nkeys = 0;
                nqueries = 0;
                sea_level = ncovering;
            }
            nkeys++;
            nqueries += nq;
            ncovering += nq;
            if (rs == nullptr)
                ncovering = 0;
            else
                while (idx_r < rs->size() && (*rs)[idx_r] == keys[idx_key]) {
                    ncovering--;
                    idx_r++;
                }
        }
        if (nkeys > 0) {
            ndpus++;
            if (!count_only)
                elms.push_back(nkeys);
        }

        return {ndpus, elms};
    }

    std::vector<partition_t> elems_to_partitions(std::vector<size_t>& elms, size_t nr_dpus)
    {
        std::vector<partition_t> partitions;
        size_t left = 0;
        for (size_t e: elms) {
            partitions.push_back(partition_t(left, left + e));
            left += e;
        }
        std::cout << "elms.size() = " << elms.size() << std::endl;
        std::cout << "partitions.size() = " << partitions.size() << std::endl;
        std::cout << "nr_dpus = " << nr_dpus << std::endl;
        assert(partitions.size() <= nr_dpus);
        while (partitions.size() < nr_dpus)
            partitions.push_back(INVALID_PARTITION);
        return partitions;
    }

public:
    OraclePartitioner(size_t max_items_per_dpu)
        : max_items_per_dpu(max_items_per_dpu)
    {}

    ~OraclePartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, m, true).first;
            if (n > nr_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<size_t> elms = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, l, false).second;
        return elems_to_partitions(elms, nr_dpus);
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus)
    {
        std::vector<int64_t> ls;
        std::vector<int64_t> rs;
        for (size_t i = 0; i < workload.size(); i++) {
            ls.push_back(workload[i].first);
            rs.push_back(workload[i].second);
        }
        std::sort(ls.begin(), ls.end());
        std::sort(rs.begin(), rs.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(keys, &ls, &rs, max_items_per_dpu, m, true).first;
            if (n > nr_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<size_t> elms = trial_pertition(keys, &ls, &rs, max_items_per_dpu, l, false).second;
        return elems_to_partitions(elms, nr_dpus);
    }
};

class BPForestPartitioner : public Partitioner {
    int alpha;
    std::vector<partition_t> base_range;
    std::vector<partition_t> hot_range;

    void build_base_ranges(const std::vector<int64_t>& keys, size_t nr_dpus)
    {
        // Left partitions absorb remainder.
        for (size_t i = 0; i < nr_dpus; i++) {
            size_t left = keys.size() - keys.size() * (nr_dpus - i) / nr_dpus;
            size_t right = keys.size() - keys.size() * (nr_dpus - i - 1) / nr_dpus;
            base_range.push_back(partition_t(left, right));
        }
    }

    using key_it_t = std::vector<int64_t>::iterator;
    partition_t find_hot_range_one(key_it_t key_begin,
                                   key_it_t& left, key_it_t key_end,
                                   key_it_t& query_it, key_it_t query_end,
                                   size_t max_items, size_t min_queries)
    {
        key_it_t right = left;
        std::queue<size_t> nqueries;
        size_t total_nqueries = 0;
        while (right < key_end) {
            if ((size_t)(right - left) >= max_items) {
                size_t nq = nqueries.front();
                nqueries.pop();
                total_nqueries -= nq;
                left++;
            }
            int64_t right_key = *right;
            while (query_it != query_end && *query_it < right_key)
                query_it++;
            int nq = 0;
            while (query_it != query_end && *query_it == right_key) {
                nq++;
                query_it++;
            }
            right++;
            nqueries.push(nq);
            total_nqueries += nq;
            if (total_nqueries >= min_queries)
                return {left - key_begin, right - key_begin};
        }
        return INVALID_PARTITION;
    }

    bool find_hot_range_from_base(std::vector<int64_t>& keys,
                                  std::vector<int64_t>& workload,
                                  partition_t& base,
                                  std::vector<partition_t>& more_hot_ranges,
                                  size_t max_items, size_t min_queries)
    {
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[base.first]);
        bool has_hot_range = false;

        key_it_t left = keys.begin() + base.first;
        key_it_t key_end = keys.begin() + base.second;
        while (left < key_end) {
            partition_t hot_range =
                find_hot_range_one(keys.begin(), left, key_end,
                                   query_it, workload.end(),
                                   max_items, min_queries);
            if (hot_range == INVALID_PARTITION)
                break;
            if (!has_hot_range)
                has_hot_range = true;
            else
                more_hot_ranges.push_back(hot_range);
            left = keys.begin() + hot_range.second;
        }

        return has_hot_range;
    }

    void distribute_hot_ranges(std::vector<bool>& has_hot_range, std::vector<partition_t>& more_hot_ranges, size_t nr_dpus)
    {
        hot_range.resize(nr_dpus, INVALID_PARTITION);

        // distribute hot ranges from the left.
        auto it = has_hot_range.begin();
        printf("more_hot_ranges.size() = %ld\n", more_hot_ranges.size());
        std::cout << "more_hot_ranges.size() = " << more_hot_ranges.size() << std::endl;
        for (const auto& range: more_hot_ranges) {
            it = std::find(it, has_hot_range.end(), false);
            assert(it != has_hot_range.end());
            hot_range[it - has_hot_range.begin()] = range;
            it++;
        }
    }

    void build_hot_ranges(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        size_t min_hot_queries = workload.size() / nr_dpus;
        if (workload.size() % nr_dpus > 0)
            min_hot_queries++;
        
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        std::vector<bool> has_hot_range(nr_dpus, false);
        std::vector<partition_t> more_hot_ranges;
        for (size_t i = 0; i < nr_dpus; i++) {
            partition_t& base = base_range[i];
            size_t total_items = base.second - base.first;
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            bool has = find_hot_range_from_base(keys, sorted_workload, base, more_hot_ranges, max_hot_items, min_hot_queries);
            has_hot_range[i] = has;
        }

        distribute_hot_ranges(has_hot_range, more_hot_ranges, nr_dpus);
    }

public:
    BPForestPartitioner(int alpha)
        : alpha(alpha)
    {}

    ~BPForestPartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        build_base_ranges(keys, nr_dpus);
        build_hot_ranges(keys, workload, nr_dpus);
        return hot_range;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus)
    {
        build_base_ranges(keys, nr_dpus);
        return base_range;
    }
};

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    std::vector<int64_t> keys = load_keys(opt.init_file());
    std::vector<int64_t> workload = load_point_queries(opt.workload_file());

    //OraclePartitioner partitioner(opt.max_items_per_dpu());
    BPForestPartitioner partitioner(1);
    std::vector<Partitioner::partition_t> hot = partitioner.partition_point(keys, workload, opt.nr_dpus());

    for (size_t i = 0; i < hot.size(); i++) {
        if (hot[i] != Partitioner::INVALID_PARTITION)
            printf("hot[%ld] = (%d, %d) %d\n", i, hot[i].first, hot[i].second, hot[i].second - hot[i].first);
    }

    return 0;
}


#if 0
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

        std::cout << "computing frequency" << std::endl;
        std::vector<size_t> freq(keys.size(), 0);
        for (int i = 0; i < workload.size(); i++) {
            auto it = std::lower_bound(keys.begin(), keys.end(), workload[i]);
//            auto it = std::find(keys.begin(), keys.end(), workload[i]);
            if (it != keys.end())
                freq[it - keys.begin()]++;
        }
        std::cout << "sorting frequency" << std::endl;
        std::sort(freq.begin(), freq.end(), std::greater<size_t>());
        printf("freq: %d %d...  2500th:%d \n", freq[0], freq[1], freq[2499]);

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
#endif // 0