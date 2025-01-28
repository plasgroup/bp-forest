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


// create map {last possible key -> partition}
std::map<int64_t, partition_t>
create_hot_to_dpu_map(std::vector<int64_t>& keys, std::vector<partition_t>& hot)
{
    std::map<int64_t, partition_t> hot_to_dpu;
    for (size_t i = 0; i < hot.size(); i++)
        if (hot[i] != INVALID_PARTITION) {
            int64_t last_key = hot[i].last_key(keys, INT64_MAX);
            hot_to_dpu.insert(std::make_pair(last_key, hot[i]));
        }
    return hot_to_dpu;
}

std::pair<std::vector<size_t>, std::vector<size_t>>
simulate_load_for_point_query(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& base,
    std::vector<partition_t>& hot,
    std::vector<int64_t>& workload)
{
    std::map<int64_t, partition_t> hot_to_dpu = create_hot_to_dpu_map(keys, hot);
    
    std::vector<size_t> base_load(base.size(), 0);
    std::vector<size_t> hot_load(base.size(), 0);
    for (size_t i = 0; i < workload.size(); i++) {
        int64_t key = workload[i];
        auto hot_it = hot_to_dpu.lower_bound(key);
        partition_t& p = hot_it->second;
        if (hot_it != hot_to_dpu.end()) {
            size_t dpu_id = p.dpu_id;
            if (key >= keys[p.begin_idx]) {
                hot_load[dpu_id]++;
                continue;
            }
        }
        auto base_it = std::lower_bound(base.begin(), base.end(), key,
            [&](const partition_t& p, const int64_t& key) {
                return p.last_key(keys, INT64_MAX) < key;
            });
        assert(base_it != base.end());
        base_load[base_it->dpu_id]++;
    }

    return {base_load, hot_load};
}

int next_begin_index(std::vector<partition_t> partitions)
{
    if (partitions.size() == 0)
        return 0;
    return partitions.back().end_idx;
}

std::vector<partition_t>
combine_partitions(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& base,
    std::vector<partition_t>& pardpu_hot)
{
    std::vector<partition_t> hot;
    for (size_t i = 0; i < pardpu_hot.size(); i++)
        if (pardpu_hot[i] != INVALID_PARTITION)
            hot.push_back(pardpu_hot[i]);
    std::sort(hot.begin(), hot.end(), [&](const partition_t& p1, const partition_t& p2) {
        return p1.begin_idx < p2.begin_idx;
    });

    std::vector<partition_t> partitions;

    auto base_it = base.begin();
    auto hot_it = hot.begin();
    while (base_it != base.end()) {
        assert(hot_it == hot.end() || hot_it->begin_idx >= base_it->begin_idx);
        while (hot_it != hot.end() && hot_it->begin_idx < base_it->end_idx) {
            int begin_idx = next_begin_index(partitions);
            if (begin_idx < hot_it->begin_idx) {
                // gap between hot partitions
                int end_idx = hot_it->begin_idx;
                partition_t p = base_it->subpartition(begin_idx, end_idx);
                partitions.push_back(p);
            } else {
                if (begin_idx != hot_it->begin_idx) {
                    printf("begin_idx = %d, hot_it->begin_idx = %d\n", begin_idx, hot_it->begin_idx);
                    exit(1);
                }
                assert(begin_idx == hot_it->begin_idx);
            }
            partitions.push_back(*hot_it);
            hot_it++;
        }
        if (next_begin_index(partitions) < base_it->end_idx) {
            // remaining base partition
            partition_t p = base_it->subpartition(next_begin_index(partitions), base_it->end_idx);
            partitions.push_back(p);
        }
        base_it++;
    }
    return partitions;
}

std::pair<std::vector<size_t>, std::vector<size_t>>
simulate_load_for_range_query(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& base,
    std::vector<partition_t>& hot,
    std::vector<std::pair<int64_t, int64_t>>& workload)
{
    std::vector<partition_t> partitions = combine_partitions(keys, base, hot);

    std::vector<size_t> base_load(base.size(), 0);
    std::vector<size_t> hot_load(base.size(), 0);

    for (size_t i = 0; i < workload.size(); i++) {
        std::pair<int64_t, int64_t>& range = workload[i];
        //printf("workload range = (%ld, %ld)\n", range.first, range.second);
        auto it = std::lower_bound(partitions.begin(), partitions.end(), range.first,
            [&](const partition_t& p, const int64_t& key) {
                return p.last_key(keys, INT64_MAX) < key;
            });
        assert(it != partitions.end());
        //printf("partition = (%ld, %ld) %d %d\n", keys[it->begin_idx], keys[it->end_idx], it->begin_idx, it->end_idx);
        while (it != partitions.end() && keys[it->begin_idx] <= range.second) {
            //printf("range = (%ld, %ld), partition = (%ld, %ld)\n", range.first, range.second, keys[it->begin_idx], keys[it->end_idx]);
            if (it->is_hot)
                hot_load[it->dpu_id]++;
            else
                base_load[it->dpu_id]++;
            it++;
        }
    }

    return {base_load, hot_load};
}


void show_load_point(std::vector<int64_t>& keys)
{
    std::vector<int64_t> workload = SlicedZipfOverKeyGenerator<int64_t>(keys, opt.alpha(), opt.num_slices(), true /* scramble */, 0 /* seed */).generate(opt.num_queries());
    BPForestChunkBuilder builder(15, 20);
    ChunkedBPForestPartitioner partitioner(opt.num_dpus(), &builder, 5);
    partitioner.partition_point(keys, workload);
    auto [base_load, hot_load] = simulate_load_for_point_query(keys, partitioner.ref_partition(0), partitioner.ref_partition(1), workload);
    for (size_t i = 0; i < base_load.size(); i++) {
        printf("load[%ld] = %ld / %ld\n", i, base_load[i], hot_load[i]);
    }
}

void show_load_range(std::vector<int64_t>& keys)
{
    SlicedZipfOverKeyGenerator<int64_t> pgen(keys, opt.alpha(), opt.num_slices(), true /* scramble */, 0 /* seed */);
    std::vector<std::pair<int64_t, int64_t>> workload = ConstLengthRangeGenerator<int64_t>(&pgen, keys, 100 /* items_in_range */).generate(opt.num_queries());
    BPForestChunkBuilder builder(15, 20);
    ChunkedBPForestPartitioner partitioner(opt.num_dpus(), &builder, 5);
    partitioner.partition_range(keys, workload);
    auto [base_load, hot_load] = simulate_load_for_range_query(keys, partitioner.ref_partition(0), partitioner.ref_partition(1), workload);
    for (size_t i = 0; i < base_load.size(); i++) {
        printf("load[%ld] = %ld / %ld\n", i, base_load[i], hot_load[i]);
    }
}


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
#elif 1
    show_load_range(keys);
#elif 1
    show_load_point(keys);
#else
    // sanity check
    // compair BPForestPartitioner and ChunkedBPForestPartitioner
    sanity_check_compair_BPForestPartitioner_and_ChunkedBPForestPartitioner(keys);
#endif

    printf("OK\n");

    return 0;
}

