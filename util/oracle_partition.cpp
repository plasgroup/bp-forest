#include <cmdline.h>
#include <random>
#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"
#include "host/inc/partition.hpp"
#include "host/inc/statistics.hpp"
#include "partitioner.hpp"
#include "workload.hpp"
#include "load_simulator.hpp"
#include <queue>

struct Option {
    cmdline::parser a;
    void parse(int argc, char* argv[])
    {       
        // partitioner
        a.add<std::string>("partitioner", 'P', "partitioner type (bpforest, oracle)", false, "bpforest");
        a.add<int>("bpforest-alpha", 'a', "[bpforest] alpha parameter", false, 5);
        a.add<int>("oracle-max-items-per-dpu", 'm', "[oracle] maximum number of items per DPU", false, 40 * 1000);

        // chunk builder
        a.add<std::string>("chunker", 'C', "chunk builder type (bpforest, random, singleton)", false, "bpforest");
        a.add<int>("bpforest-leaf-size", 'L', "[bpforest]number of keys in a leaf node", false, 15);
        a.add<int>("bpforest-node-size", 'N', "[bpforest]number of children in a node", false, 20);
        a.add<int>("random-chunk-max", 0, "[random] max chunk size", false, 16);
        a.add<int>("random-chunk-min", 0, "[random] min chunk size", false, 8);

        // workload
        a.add<double>("zconst", 'z', "zipf constant", false, 0.99);
        a.add<int>("slices", 's', "number of slices", false, 1024 * 10);
        a.add<bool>("zipf-scramble", 0, "scramble zipf", false, true);
        a.add<int>("queries", 'q', "number of queries", false, 1024 * 1024);
        a.add<int>("items-in-range", 'r', "range width of queries (#of items)", false, 100);
        //a.add<std::string>("pimtree-workload-file", 'w', "file path to PIM-Tree workload file", false);
        //a.add<std::string>("pimtree-init-file", 'i', "file path to PIM-Tree init file", false);

        // init data
        a.add<double>("items", 'n', "number of items in millions", false, 500.0);

        a.add<std::string>("ops", 'o', "kind of operation (get, range)", false, "get");
        a.add<int>("dpus", 'p', "number of DPUs", false, 2500);

        a.add<std::string>("load-output", 0, "output file for load (CSV)", false, "");
        a.add<std::string>("partition-output", 0, "output file for partition (binary)", false, "");

        // debug
        a.add("verify-partitoner", 0, "verify-pertitioner");

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

    int oracle_max_items_per_dpu() {
        return a.get<int>("oracle-max-items-per-dpu");
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

    double zconst() {
        return a.get<double>("zconst");
    }

    int num_slices() {
        return a.get<int>("slices");
    }

    bool zipf_scramble() {
        return a.get<bool>("zipf-scramble");
    }

    size_t items() {
        return (size_t)(a.get<double>("items") * 1000 * 1000);
    }

    int items_in_range() {
        return a.get<int>("items-in-range");
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

    const std::string& load_output() {
        return a.get<std::string>("load-output");
    }

    const std::string& partition_output() {
        return a.get<std::string>("partition-output");
    }

    bool verify_partitioner() {
        return a.exist("verify-partitoner");
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
    fprintf(fp, "base_begin_idx, base_end_idx, hot_begin_idx, hot_end_idx\n");
    for (size_t i = 0; i < base_load.size(); i++)
        fprintf(fp, "%ld, %ld, %ld, %d, %d, %d, %d\n",
                i, base_load[i], hot_load[i],
                base[i].begin_idx, base[i].end_idx, hot[i].begin_idx, hot[i].end_idx);
    fclose(fp);
}

static void append_partitions(
    std::vector<partition>& external,
    std::vector<partition_t>& internal,
    std::vector<int64_t>& keys)
{
    for (int i = 0; i < opt.num_dpus(); i++) {
        partition p;
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

    std::vector<partition> partitions;
    // base partition
    std::vector<partition_t>& base = partitioner->ref_partition(0);
    append_partitions(partitions, base, keys);
    // hot partition
    std::vector<partition_t>& hot = partitioner->ref_partition(1);
    append_partitions(partitions, hot, keys);
    store_partition(file_name, partitions);
}

void show_load(std::vector<int64_t>& keys)
{
    ChunkBuilder* builder = nullptr;
    if (opt.chunker() == "bpforest") {
        printf("chunker: bpforest(%d, %d)\n", opt.bpforest_leaf_size(), opt.bpforest_node_size());
        builder = new BPForestChunkBuilder(opt.bpforest_leaf_size(), opt.bpforest_node_size());
    } else if (opt.chunker() == "random") {
        printf("chunker: random(%d, %d)\n", opt.random_chunk_min(), opt.random_chunk_max());
        builder = new RandomChunkBuilder(opt.random_chunk_min(), opt.random_chunk_max(), 0 /* seed */);
    } else if (opt.chunker() == "singleton") {
        printf("chunker: singleton\n");
        builder = new SingletonChunkBuilder();
    } else {
        fprintf(stderr, "invalid chunker type: %s\n", opt.chunker().c_str());
        exit(1);
    }

    Partitioner* partitioner = nullptr;
    if (opt.partitioner() == "bpforest") {
        printf("partitioner: bpforest(%d)\n", opt.bpforest_alpha());
        partitioner = new ChunkedBPForestPartitioner(opt.num_dpus(), builder, opt.bpforest_alpha());
    } else if (opt.partitioner() == "oracle") {
        printf("partitioner: oracle(%d)\n", opt.oracle_max_items_per_dpu());
        partitioner = new ChunkedOraclePartitioner(opt.num_dpus(), builder, opt.oracle_max_items_per_dpu());
    } else {
        fprintf(stderr, "invalid partitioner type: %s\n", opt.partitioner().c_str());
        exit(1);
    }

    std::pair<std::vector<size_t>, std::vector<size_t>> load;
    if (opt.op_type() == get_t) {
        SlicedZipfOverKeyGenerator<int64_t> pgen(keys, opt.zconst(), opt.num_slices(), opt.zipf_scramble(), 0 /* seed */); 
        std::vector<int64_t> workload = pgen.generate(opt.num_queries());

        partitioner->partition_point(keys, workload);
        load = simulate_load_for_point_query(keys, partitioner->ref_partition(0), partitioner->ref_partition(1), workload);
    } else {
        SlicedZipfOverKeyGenerator<int64_t> pgen(keys, opt.zconst(), opt.num_slices(), opt.zipf_scramble(), 0 /* seed */); 
        std::vector<std::pair<int64_t, int64_t>> workload = ConstLengthRangeGenerator<int64_t>(&pgen, keys, opt.items_in_range()).generate(opt.num_queries());

        partitioner->partition_range(keys, workload);
        load = simulate_load_for_range_query(keys, partitioner->ref_partition(0), partitioner->ref_partition(1), workload);
    }

    auto [base_load, hot_load] = load;
    for (size_t i = 0; i < base_load.size(); i++)
        printf("load[%ld] = %ld / %ld\n", i, base_load[i], hot_load[i]);

    if (!opt.load_output().empty())
        save_load(partitioner, base_load, hot_load, opt.load_output().c_str());

    if (!opt.partition_output().empty())
        save_partition(keys, partitioner, opt.partition_output().c_str());

    delete partitioner;
    delete builder;
}


static void sanity_check_compair_BPForestPartitioner_and_ChunkedBPForestPartitioner(std::vector<int64_t> &keys);
static void sanity_check_compair_OraclePartitioner_and_ChunkedOraclePartitioner(std::vector<int64_t>& keys);

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    EvenGenerator<int64_t> init_gen(INT64_MIN, INT64_MAX);
    std::vector<int64_t> keys = init_gen.generate(opt.items());

    if (opt.verify_partitioner()) {
        sanity_check_compair_BPForestPartitioner_and_ChunkedBPForestPartitioner(keys);
        sanity_check_compair_OraclePartitioner_and_ChunkedOraclePartitioner(keys);
    }

    show_load(keys);

    printf("OK\n");

    return 0;
}


static void sanity_check_compair_BPForestPartitioner_and_ChunkedBPForestPartitioner(std::vector<int64_t> &keys)
{
    std::vector<int64_t> workload = SlicedZipfOverKeyGenerator<int64_t>(keys, opt.zconst(), opt.num_slices(), true /* scramble */, 0 /* seed */).generate(opt.num_queries());
    BPForestPartitioner partitioner(opt.num_dpus(), opt.bpforest_alpha());
    SingletonChunkBuilder builder;
    ChunkedBPForestPartitioner chunked_partitioner(opt.num_dpus(), &builder, opt.bpforest_alpha());
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

static void sanity_check_compair_OraclePartitioner_and_ChunkedOraclePartitioner(std::vector<int64_t>& keys)
{
    std::vector<int64_t> workload = SlicedZipfOverKeyGenerator<int64_t>(keys, opt.zconst(), opt.num_slices(), true /* scramble */, 0 /* seed */).generate(opt.num_queries());
    OraclePartitioner partitioner(opt.num_dpus(), opt.oracle_max_items_per_dpu());
    SingletonChunkBuilder builder;
    ChunkedOraclePartitioner chunked_partitioner(opt.num_dpus(), &builder, opt.oracle_max_items_per_dpu());
    auto par1 = partitioner.partition_point(keys, workload);
    auto par2 = chunked_partitioner.partition_point(keys, workload);
    if (par1.size() != par2.size()) {
        printf("par1.size() = %ld, par2.size() = %ld\n", par1.size(), par2.size());
    }
    size_t count = 0;
    for (partition_t p: par1)
        count += p.end_idx - p.begin_idx;
    printf("par1 count = %ld\n", count);

    count = 0;
    for (partition_t p: par2)
        count += p.end_idx - p.begin_idx;
    printf("par2 count = %ld\n", count);
    
    for (size_t i = 0; i < par1.size(); i++) {
        if (par1[i] != par2[i]) {
            printf("par1[%ld] = (%d, %d), par2[%ld] = (%d, %d)\n", i, par1[i].begin_idx, par1[i].end_idx, i, par2[i].begin_idx, par2[i].end_idx);
        }
    }

    for (size_t i = par1.size(); i < par2.size(); i++) {
        printf("par2[%ld] = (%d, %d)\n", i, par2[i].begin_idx, par2[i].end_idx);
    }
}
