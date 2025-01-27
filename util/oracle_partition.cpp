#include <cmdline.h>
#include <random>
#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"
#include "host/inc/statistics.hpp"
#include "partitioner.hpp"
#include "workload.hpp"
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
        a.add<int>("dpus", 'p', "number of DPUs", false, 2500);
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
    
    int num_dpus() {
        return a.get<int>("dpus");
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

    int num_queries() {
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

#if 0
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
#endif // PIM_TRE
#endif // 0

#if 0
// create map {last possible key -> (dpu_id, partition)}
std::map<int64_t, std::pair<size_t, partition_t>> create_hot_to_dpu_map(
    std::vector<int64_t>& keys,
    std::vector<Partitioner::partition_t>& hot)
{
    std::map<int64_t, std::pair<size_t, Partitioner::partition_t>> hot_to_dpu;
    for (size_t i = 0; i < hot.size(); i++)
        if (hot[i] != Partitioner::INVALID_PARTITION) {
            int64_t last_key = hot[i].second == keys.size() ? KEY_MAX : keys[hot[i].second] - 1;
            hot_to_dpu[last_key] = std::make_pair(i, hot[i]);
            //printf("hot[%ld] = (%ld, %ld) %ld\n", i, hot[i].first, hot[i].second, hot[i].second - hot[i].first);
        }
    return hot_to_dpu;
}

std::pair<std::vector<size_t>, std::vector<size_t>> simulate_load_for_point_query(
    std::vector<int64_t>& keys,
    std::vector<Partitioner::partition_t>& base,
    std::vector<Partitioner::partition_t>& hot,
    std::vector<int64_t>& workload)
{
    std::map<int64_t, std::pair<size_t, Partitioner::partition_t>> hot_to_dpu = create_hot_to_dpu_map(keys, hot);
    
    std::vector<size_t> base_load(base.size(), 0);
    std::vector<size_t> hot_load(base.size(), 0);
    for (size_t i = 0; i < workload.size(); i++) {
        int64_t key = workload[i];
        auto it = hot_to_dpu.lower_bound(key);
        if (it != hot_to_dpu.end()) {
            size_t dpu_id = it->second.first;
            Partitioner::partition_t& p = it->second.second;
            if (key >= keys[p.first]) {
                hot_load[dpu_id]++;
                //printf("key = %ld, keys[%ld] = %ld, hot[%ld] = (%ld, %ld) %ld\n", key, p.first, keys[p.first], dpu_id, p.first, p.second, p.second - p.first);
                continue;
            }
        }
        auto it2 = std::lower_bound(base.begin(), base.end(), key,
            [&](const Partitioner::partition_t& p, const int64_t& key) {
                return keys[p.second - 1] < key;
            });
        if (it2 == base.end()) {
            printf("key = %ld last = %ld\n", key, keys[base.back().first]);
            exit(1);
        }
        assert(it2 != base.end());
        size_t dpu_id = it2 - base.begin();
        base_load[dpu_id]++;
    }

    return {base_load, hot_load};
}


std::pair<std::vector<size_t>, std::vector<size_t>> simulate_load_for_range_query(
    std::vector<int64_t>& keys,
    std::vector<Partitioner::partition_t>& base,
    std::vector<Partitioner::partition_t>& hot,
    std::vector<std::pair<int64_t, int64_t>>& workload)
{
    struct partition_info {
        Partitioner::partition_t partition;
        unsigned int dpu_id;
        bool is_hot;
    };

    std::vector<partition_info> partitions;
    auto base_it = base.begin();
    auto hot_it = hot.begin();
    while (base_it != base.end()) {
        while (hot_it->first <= base_it->first) {
            struct partition_info pi;
            pi.partition = *hot_it;
            pi.dpu_id = hot_it - hot.begin();
            pi.is_hot = true;
            partitions.push_back(pi);
            hot_it++;
        }
        while (hot_it->second <= base_it->)


        if (base_it->first < hot_it->first) {
            if (base_it->second <)

            partition_info p = {

            partitions.push_back({*base_it, partitions.size(), false, base_it - base.begin()});
            base_it++;
        } else {
            partitions.push_back({*hot_it, partitions.size(), true, hot_it - hot.begin()});
            hot_it++;

        }

    }





    std::map<int64_t, std::pair<size_t, Partitioner::partition_t>> hot_to_dpu = create_hot_to_dpu_map(keys, hot);

    std::vector<size_t> base_load(base.size(), 0);
    std::vector<size_t> hot_load(base.size(), 0);
    for (size_t i = 0; i < workload.size(); i++) {
        std::pair<int64_t, int64_t> range = workload[i];
        auto it = hot_to_dpu.lower_bound(range.first);
        while (it != hot_to_dpu.end()) {
            size_t dpu_id = it->second.first;
            Partitioner::partition_t& p = it->second.second;
            if (range.second < keys[p.first])
                break;
            hot_load[dpu_id]++;

            int64_t last_key = it->first;
            it++;
            if (range.second > last_key) {
                // range is longer than this hot partition
                Partitioner::partition_t& q = it->second.second;
                if (it == hot_to_dpu.end() || p.second + 1 < q.first) {
                    // there is a gap between this hot partition and the next hot partition

                }
                    
                    
                    it == hot_to_dpu.end() ||
                    p.second + 1 < (it + 1)->second.first)
                    break;
            }

            if (range.second > it->first) {
                // range is longer than this hot partition
                if (it + 1 == hot_to_dpu.end() ||
                    p->second + 1 < (it + 1)->second.first)

                
                )
            }

            it++; 
        }
        

        if (it != hot_to_dpu.end()) {
            size_t dpu_id = it->second.first;
            Partitioner::partition_t& p = it->second.second;
            if (key >= keys[p.first]) {
                hot_load[dpu_id]++;
                //printf("key = %ld, keys[%ld] = %ld, hot[%ld] = (%ld, %ld) %ld\n", key, p.first, keys[p.first], dpu_id, p.first, p.second, p.second - p.first);
                continue;
            }
        }
        auto it2 = std::lower_bound(base.begin(), base.end(), key,
            [&](const Partitioner::partition_t& p, const int64_t& key) {
                return keys[p.second - 1] < key;
            });
        if (it2 == base.end()) {
            printf("key = %ld last = %ld\n", key, keys[base.back().first]);
            exit(1);
        }
        assert(it2 != base.end());
        size_t dpu_id = it2 - base.begin();
        base_load[dpu_id]++;
    }

    return {base_load, hot_load};

}

void show_load(std::vector<int64_t>& keys)
{
    std::vector<int64_t> workload = SlicedZipfOverKeyGenerator<int64_t>(keys, opt.alpha(), opt.num_slices(), true /* scramble */, 0 /* seed */).generate(opt.num_queries());
    BPForestChunkBuilder builder(15, 20);
    ChunkedBPForestPartitioner partitioner(opt.num_dpus(), &builder, 5);
    partitioner.partition_point(keys, workload);
    auto [base_load, hot_load] = simulate_load_for_point_query(keys, partitioner.get_partition(0), partitioner.get_partition(1), workload);
    for (size_t i = 0; i < base_load.size(); i++) {
        printf("load[%ld] = %ld / %ld\n", i, base_load[i], hot_load[i]);
    }
}
#endif

void sanity_check_compair_BPForestPartitioner_and_ChunkedBPForestPartitioner(std::vector<int64_t> &keys)
{
    std::vector<int64_t> workload = SlicedZipfOverKeyGenerator<int64_t>(keys, opt.alpha(), opt.num_slices(), true /* scramble */, 0 /* seed */).generate(opt.num_queries());
    BPForestPartitioner partitioner(opt.num_dpus(), 5);
    SingletonChunkBuilder builder;
    ChunkedBPForestPartitioner chunked_partitioner(opt.num_dpus(), &builder, 5);
    auto par1 = partitioner.partition_point(keys, workload);
    auto par2 = chunked_partitioner.partition_point(keys, workload);
    if (par1.size() != par2.size()) {
        printf("par1.size() = %ld, par2.size() = %ld\n", par1.size(), par2.size());
        exit(1);
    }
    for (size_t i = 0; i < par1.size(); i++) {
        if (par1[i] != par2[i]) {
            printf("par1[%ld] = (%d, %d), par2[%ld] = (%d, %d)\n", i, par1[i].begin_idx, par1[i].end_idx, i, par2[i].begin_idx, par2[i].end_idx);
            exit(1);
        }
    }
    chunked_partitioner.print_hot_partitions();
}
    
int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    EvenGenerator<int64_t> init_gen(INT64_MIN, INT64_MAX);
    std::vector<int64_t> keys = init_gen.generate(opt.items());

#if 0
    SlicedZipfOverKeyGenerator<int64_t> point_gen(keys, opt.alpha(), opt.num_slices(), true /* scramble */, 0 /* seed */);
    ConstLengthRangeGenerator<int64_t> range_gen(&point_gen, keys, 100 /* items_in_range */);
    std::vector<std::pair<int64_t, int64_t>> workload = range_gen.generate(opt.num_queries());

    ChunkedBPForestPartitioner partitioner(new BPForestChunkBuilder(15, 20), 5);
    auto part = partitioner.partition_range(keys, workload, opt.num_dpus());
    for (size_t i = 0; i < part.size(); i++) {
        if (part[i] != Partitioner::INVALID_PARTITION)
            printf("part[%ld] = (%ld, %ld) %ld\n", i, part[i].first, part[i].second, part[i].second - part[i].first);
    }

#elif 0
    show_load(keys);
#else
    // sanity check
    // compair BPForestPartitioner and ChunkedBPForestPartitioner
    sanity_check_compair_BPForestPartitioner_and_ChunkedBPForestPartitioner(keys);
#endif

    printf("OK\n");

    return 0;
}

