#include <cmdline.h>
#include <random>
#include "pimtree_query.hpp"
#include "partition.hpp"
#include "partitioner.hpp"
#include "workload.hpp"
#include "load_simulator.hpp"
#include <queue>

struct Option {
    cmdline::parser a;
    void parse(int argc, char* argv[])
    {       
        // partitioner
        a.add<std::string>("partitioner", 'P', "partitioner type (bpforest, hwc, oracle, equal, data, query)", false, "bpforest");
        a.add<int>("bpforest-alpha", 'a', "[bpforest|hwc] alpha parameter", false, 5);
        a.add<int>("oracle-max-items-per-dpu", 'm', "[oracle] maximum number of items per DPU (default = #init-items / dpus * (1 + 1/bpforest-alpha) )", false, -1);

        // chunk builder
        a.add<std::string>("chunker", 'C', "chunk builder type (bpforest, random, singleton)", false, "bpforest");
        a.add<int>("bpforest-leaf-size", 'L', "[bpforest]number of keys in a leaf node", false, 15);
        a.add<int>("bpforest-node-size", 'N', "[bpforest]number of children in a node", false, 20);
        a.add<int>("random-chunk-max", 0, "[random] max chunk size", false, 16);
        a.add<int>("random-chunk-min", 0, "[random] min chunk size", false, 8);

        // workload
        a.add<std::string>("pimtree-workload-file", 'w', "file path to PIM-Tree workload file", true);

        // init data
        a.add<std::string>("pimtree-init-file", 'i', "file path to PIM-Tree init file", true);

        a.add<std::string>("ops", 'o', "kind of operation (get, pred, range)", false, "get");
        a.add<int>("dpus", 'p', "number of DPUs", false, 2500);

        a.add<std::string>("load-output", 0, "output file for load (CSV)", false, "");
        a.add<std::string>("partition-output", 0, "output file for partition (binary)", false, "");
        a.add<std::string>("workload-output", 0, "output file for workload in PIM-Tree format", false, "");
        a.add<std::string>("init-output", 0, "output file for init data in PIM-Tree format", false, "");

        a.add<int>("seed", 0, "random seed", false, 1000*1000);

        a.parse_check(argc, argv);
    }

    const std::string& workload_file() {
        return a.get<std::string>("pimtree-workload-file");
    }

    const std::string& init_file() {
        return a.get<std::string>("pimtree-init-file");
    }

    const std::string& partitioner() {
        return a.get<std::string>("partitioner");
    }

    int bpforest_alpha() {
        return a.get<int>("bpforest-alpha");
    }

    int oracle_max_items_per_dpu(size_t nitems) {
        if (a.exist("oracle-max-items-per-dpu"))
            return a.get<int>("oracle-max-items-per-dpu");
        else
            return nitems * (1.0 + 1.0 / bpforest_alpha()) / num_dpus();
    }

    const std::string& chunker() {
        return a.get<std::string>("chunker");
    }

    int bpforest_leaf_size() {
        return a.get<int>("bpforest-leaf-size");
    }

    int bpforest_node_size() {
        return a.get<int>("bpforest-node-size");
    }

    int random_chunk_max() {
        return a.get<int>("random-chunk-max");
    }

    int random_chunk_min() {
        return a.get<int>("random-chunk-min");
    }

    int num_dpus() {
        return a.get<int>("dpus");
    }

    operation_t op_type() {
        if (a.get<std::string>("ops") == "get")
            return get_t;
        else if (a.get<std::string>("ops") == "pred")
            return predecessor_t;
        else if (a.get<std::string>("ops") == "rmq")
            return scan_t;
        else if (a.get<std::string>("ops") == "count")
            return scan_t;
        else if (a.get<std::string>("ops") == "scan")
            return scan_t;
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
            return empty_t;
        }
    }

    int seed() {
        return a.get<int>("seed");
    }

    const std::string& load_output() {
        return a.get<std::string>("load-output");
    }

    const std::string& partition_output() {
        return a.get<std::string>("partition-output");
    }

    const char* workload_output() {
        if (a.exist("workload-output"))
            return a.get<std::string>("workload-output").c_str();
        else
            return nullptr;
    }

    const char* init_output() {
        if (a.exist("init-output"))
            return a.get<std::string>("init-output").c_str();
        else
            return nullptr;
    }
} opt;

void save_load(
    Partitioner* partitioner,
    std::vector<size_t>& base_load,
    std::vector<size_t>& hot_load,
    const char* file_name)
{
    FILE* fp = fopen(file_name, "w");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    printf("output load to %s\n", file_name);

    std::vector<partition_t>& base = partitioner->ref_partition(0);
    std::vector<partition_t>& hot = partitioner->ref_partition(1);

    fprintf(fp, "# dpu_id, base_load, hot_load, ");
    fprintf(fp, "base_begin_idx, base_end_idx, hot_begin_idx, hot_end_idx, total_load, hot_items\n");
    for (size_t i = 0; i < base_load.size(); i++)
        fprintf(fp, "%ld, %ld, %ld, %d, %d, %d, %d, %d, %d\n",
                i, base_load[i], hot_load[i],
                base[i].begin_idx, base[i].end_idx, hot[i].begin_idx, hot[i].end_idx,
                base_load[i] + hot_load[i], hot[i].end_idx - hot[i].begin_idx);
    fclose(fp);
}

static void append_partitions(
    std::vector<Partition>& external,
    std::vector<partition_t>& internal,
    std::vector<int64_t>& keys)
{
    for (int i = 0; i < opt.num_dpus(); i++) {
        Partition p;
        if (internal[i] == INVALID_PARTITION) {
            p.left_key = 0;
            p.length = 0;
        } else {
            p.left_key = internal[i].first_key(keys, INT64_MIN);
            p.length = internal[i].last_key(keys, INT64_MAX) - p.left_key + 1;
        }
        external.push_back(p);
    }
}

void save_partition(std::vector<int64_t>& keys, Partitioner* partitioner, const char* file_name)
{
    printf("output partitions to %s\n", file_name);

    std::vector<Partition> partitions;
    // base partition
    std::vector<partition_t>& base = partitioner->ref_partition(0);
    append_partitions(partitions, base, keys);
    // hot partition
    std::vector<partition_t>& hot = partitioner->ref_partition(1);
    append_partitions(partitions, hot, keys);
    store_partition(file_name, partitions);
}

void evaluate_workload(std::vector<int64_t>& keys, size_t num_dpus, ChunkBuilder* chunk_builder, std::vector<std::pair<int64_t, int64_t>>& workload)
{
    std::vector<ChunkBuilder::chunk> chunks;
    for_each_bpforest_baserange(keys, num_dpus, [&](unsigned int begin_idx, unsigned int end_idx) {
        chunk_builder->build_chunks(chunks, keys, begin_idx, end_idx);
    });

    size_t queries = 0;
    std::vector<size_t> queries_in_chunk(chunks.size(), 0);
    for (auto [lkey, rkey]: workload) {
        auto it = std::upper_bound(chunks.begin(), chunks.end(), lkey,
        [&](int64_t key, const ChunkBuilder::chunk& c) {
            return key < c.left_key;
        });
        for (auto it2 = it == chunks.begin() ? it : it - 1;
             it2 != chunks.end() && it2->left_key <= rkey; it2++) {
            queries_in_chunk[it2 - chunks.begin()]++;
            queries++;
        }
    }

    size_t max = 0;
    for (auto q: queries_in_chunk) {
        max = std::max(max, q);
    }
    printf("max queries in a chunk = %ld total = %ld\n", max, queries);
}

void evaluate_workload(std::vector<int64_t>& keys, size_t num_dpus, ChunkBuilder* chunk_builder, std::vector<int64_t>& workload)
{
    std::vector<std::pair<int64_t, int64_t>> range_workload;
    for (auto key : workload)
        range_workload.push_back({key, key});
    evaluate_workload(keys, num_dpus, chunk_builder, range_workload);
}

// A pred query for key k is answered by the partition owning the largest key
// strictly less than k (the host routes it with lower_bound over the partition
// delimiters, unlike upper_bound for get).  Hence it loads the same partition
// as a get query for k - 1.  Queries with k <= min key have no predecessor and
// are answered on the host without reaching any DPU, so they are dropped here.
static std::vector<int64_t> pred_routing_keys(const std::vector<int64_t>& workload, int64_t min_key)
{
    std::vector<int64_t> routing_keys;
    routing_keys.reserve(workload.size());
    for (int64_t key: workload)
        if (key > min_key)
            routing_keys.push_back(key - 1);
    if (routing_keys.size() < workload.size())
        printf("dropped %ld pred queries with no predecessor\n", workload.size() - routing_keys.size());
    return routing_keys;
}

void show_load(std::vector<int64_t>& keys)
{
    ChunkBuilder* builder = nullptr;
    if (opt.chunker() == "bpforest") {
        printf("chunker: bpforest(%d, %d)\n", opt.bpforest_leaf_size(), opt.bpforest_node_size());
        builder = new BPForestChunkBuilder(opt.bpforest_leaf_size(), opt.bpforest_node_size());
    } else if (opt.chunker() == "random") {
        printf("chunker: random(%d, %d)\n", opt.random_chunk_min(), opt.random_chunk_max());
        builder = new RandomChunkBuilder(opt.random_chunk_min(), opt.random_chunk_max(), opt.seed());
    } else if (opt.chunker() == "singleton") {
        printf("chunker: singleton\n");
        builder = new SingletonChunkBuilder();
    } else {
        fprintf(stderr, "invalid chunker type: %s\n", opt.chunker().c_str());
        exit(1);
    }

    Partitioner* partitioner = nullptr;
    if (opt.partitioner() == "hwc") {
        printf("partitioner: hwc(%d)\n", opt.bpforest_alpha());
        partitioner = new HWCBPForestPartitioner(opt.num_dpus(), builder, opt.bpforest_alpha());
    } else if (opt.partitioner() == "bpforest") {
        printf("partitioner: bpforest(%d)\n", opt.bpforest_alpha());
        partitioner = new ChunkedBPForestPartitioner(opt.num_dpus(), builder, opt.bpforest_alpha());
    } else if (opt.partitioner() == "oracle") {
        printf("partitioner: oracle(%d)\n", opt.oracle_max_items_per_dpu(keys.size()));
        partitioner = new ChunkedOraclePartitioner(opt.num_dpus(), builder, opt.oracle_max_items_per_dpu(keys.size()));
    } else if (opt.partitioner() == "equal") {
        printf("partitioner: equal\n");
        partitioner = new EqualSizePartitioner(opt.num_dpus());
    } else if (opt.partitioner() == "data") {
        printf("partitioner: data\n");
        partitioner = new EqualDataSizePartitioner(opt.num_dpus());
    } else if (opt.partitioner() == "query") {
        printf("partitioner: query\n");
        partitioner = new EqualQueryLoadPartitioner(opt.num_dpus());
    } else {
        fprintf(stderr, "invalid partitioner type: %s\n", opt.partitioner().c_str());
        exit(1);
    }

    std::pair<std::vector<size_t>, std::vector<size_t>> load;
    if (opt.op_type() == get_t || opt.op_type() == predecessor_t) {
        printf("load workload from %s\n", opt.workload_file().c_str());
        std::vector<int64_t> workload = load_point_workload<int64_t>(opt.workload_file(), opt.op_type());
        if (opt.workload_output() != nullptr)
            save_point_workload(opt.workload_output(), workload, opt.op_type());

        if (opt.op_type() == predecessor_t)
            workload = pred_routing_keys(workload, keys.front());

        partitioner->partition_point(keys, workload);
        load = simulate_load_for_point_query(keys, partitioner->ref_partition(0), partitioner->ref_partition(1), workload);
        evaluate_workload(keys, opt.num_dpus(), builder, workload);
    } else {
        printf("load workload from %s\n", opt.workload_file().c_str());
        std::vector<std::pair<int64_t, int64_t>> workload = load_range_workload<int64_t>(opt.workload_file());
        if (opt.workload_output() != nullptr)
            save_range_workload(opt.workload_output(), workload);

        partitioner->partition_range(keys, workload);
        load = simulate_load_for_range_query(keys, partitioner->ref_partition(0), partitioner->ref_partition(1), workload);
        evaluate_workload(keys, opt.num_dpus(), builder, workload);
    }

    auto& [base_load, hot_load] = load;
    size_t max_load = 0;
    size_t max_load_hot = 0;
    size_t max_load_cold = 0;
    size_t max_load_dpu = 0;
    for (size_t i = 0; i < base_load.size(); i++) {
        if (base_load[i] + hot_load[i] > max_load) {
            max_load = base_load[i] + hot_load[i];
            max_load_dpu = i;
            max_load_hot = hot_load[i];
            max_load_cold = base_load[i];
        }
    }
    printf("max_load = %ld, max_load_hot = %ld, max_load_cold = %ld, max_load_dpu = %ld\n", max_load, max_load_hot, max_load_cold, max_load_dpu);

    if (!opt.load_output().empty())
        save_load(partitioner, base_load, hot_load, opt.load_output().c_str());

    if (!opt.partition_output().empty())
        save_partition(keys, partitioner, opt.partition_output().c_str());

    delete partitioner;
    delete builder;
}


int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    printf("load init data from %s\n", opt.init_file().c_str());
    std::vector<std::pair<int64_t, int64_t>> kvs = load_init_data<int64_t, int64_t>(opt.init_file());
    std::vector<int64_t> keys;
    for (auto [key, value]: kvs)
        keys.push_back(key);
    if (opt.init_output() != nullptr)
        save_init_data(opt.init_output(), kvs);

    show_load(keys);

    printf("OK\n");

    return 0;
}
