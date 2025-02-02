#include <cmdline.h>
#include <random>
#include "oracle_partition/partitioner.hpp"
#include "oracle_partition/workload.hpp"
#include "oracle_partition/load_simulator.hpp"
#include "../external/timer_tree/include/timer.hpp"
#include "parallel.ipp"
#include <queue>
#include <sys/time.h>

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

        a.add<int>("nthreads", 't', "number of threads", false, 0);

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

    int nthreads() {
        return a.get<int>("nthreads");
    }

    bool verify_partitioner() {
        return a.exist("verify-partitoner");
    }
} opt;

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    ParallelManager pm(opt.nthreads());

    init_root_timer();
    timer::active = true;
    timer::default_detail = true;
    //timer::print_when_time = true;
    timer::print_when_time = false;


    //struct timeval start, end;
    //gettimeofday(&start, NULL);

    time_nested_pass("main", [&](timer* timer) {
        std::vector<int64_t> keys;
        {
            EvenGenerator<int64_t> init_gen(INT64_MIN, INT64_MAX);
            keys = init_gen.generate(opt.items());
            printf("number of keys: %d\n", (int) keys.size());
        }

        std::vector<ChunkBuilder::chunk> chunks;
        std::vector<int> chunk_owner;
        {
            RandomChunkBuilder cb(opt.random_chunk_min(), opt.random_chunk_max(), 0);
            cb.build_chunks(chunks, keys, 0, (unsigned int) keys.size());

            /* distribute chunks */
            for (int i = 0; i < (int) chunks.size(); i++)
                chunk_owner.push_back(i % opt.num_dpus());
            /* shuffle */
            std::mt19937_64 mt(0);
            std::shuffle(chunk_owner.begin(), chunk_owner.end(), mt);
            printf("chunk size: %d - %d\n", opt.random_chunk_min(), opt.random_chunk_max());
            printf("number of chunks: %d\n", (int) chunks.size());
        }

        std::vector<std::pair<int64_t, int64_t>> workload;
        {
            SlicedZipfOverKeyGenerator<int64_t> pgen(keys, opt.zconst(), opt.num_slices(), opt.zipf_scramble(), 0);
            ConstLengthRangeGenerator<int64_t> rgen(&pgen, keys, opt.items_in_range());
            workload = rgen.generate(opt.num_queries());
            printf("workload size: %d\n", (int) workload.size());
            printf("  params: zipf_const=%f num_slices=%d scramble=%s len=%d\n",
                    opt.zconst(), opt.num_slices(), opt.zipf_scramble() ? "yes" : "no", opt.items_in_range());
        }

        std::vector<std::vector<std::vector<int>>> sent_query_id; // [thread_id][dpu_id][i] = query_id
        std::vector<std::vector<int>> first_index; // [thread_id][dpu_id] = index of sent_query_id[thread_id][dpu_id]
        for (int i = 0; i < pm.get_parallelism(); i++)
            sent_query_id.push_back(std::vector<std::vector<int>>(opt.num_dpus()));
        {
            pm.run(0, workload.size(), [&](size_t tid, size_t s, size_t e) {
                for (int i = (int) s; i < (int) e; i++) {
                    int64_t left = workload[i].first;
                    int64_t right = workload[i].second;
                    if (left < chunks[0].left_key)
                        continue;
                    auto it = std::upper_bound(chunks.begin(), chunks.end(), left, [](int64_t key, const ChunkBuilder::chunk& c) {
                        return key < c.left_key;
                    });
                    size_t idx_chunk = it - chunks.begin();
                    while (idx_chunk < chunks.size() && chunks[idx_chunk].left_key <= right) {
                        /* send workload[i] to chunk_owner[idx_chunk] */
                        sent_query_id[tid][chunk_owner[idx_chunk]].push_back(i);
                        idx_chunk++;
                    }
                }               
            });
            for (int tid = 0; tid < pm.get_parallelism() + 1; tid++)
                first_index.push_back(std::vector<int>(opt.num_dpus()));
            pm.run(0, opt.num_dpus(), [&](size_t _, size_t s, size_t e) {
                for (size_t dpu_id = s; dpu_id < e; dpu_id++) {
                    int acc = 0;
                    for (int tid = 0; tid < pm.get_parallelism(); tid++) {
                        first_index[tid][dpu_id] = acc;
                        acc += (int) sent_query_id[tid][dpu_id].size();
                    }
                    first_index[pm.get_parallelism()][dpu_id] = acc; // sentinel
                }
            });
            int total = 0;
            for (int dpu_id = 0; dpu_id < opt.num_dpus(); dpu_id++)
                total += first_index[pm.get_parallelism()][dpu_id];
            printf("total duplicated queries: %d\n", total);
        }
        std::vector<int>& num_queries = first_index[pm.get_parallelism()];  // [dpu_id] = num_queries

        struct aggregation_result {
            int query_id;
            int64_t value;
        };

        struct aggregation_result** results; // [dpu_id][j] = result
        {
            results = new struct aggregation_result*[opt.num_dpus()];
            for (int dpu_id = 0; dpu_id < opt.num_dpus(); dpu_id++) {
                results[dpu_id] = new struct aggregation_result[num_queries[dpu_id]];
                
                int result_idx = 0;
                for (int tid = 0; tid < pm.get_parallelism(); tid++) {
                    for (size_t i = 0; i < sent_query_id[tid][dpu_id].size(); i++) {
                        results[dpu_id][result_idx].query_id = sent_query_id[tid][dpu_id][i];
                        results[dpu_id][result_idx].value = 1;  // random value
                        result_idx++;
                    }
                }
                if (result_idx != num_queries[dpu_id])
                    fprintf(stderr, "assertion failed: result_idx=%d num_queries=%d\n", result_idx, num_queries[dpu_id]);
            }
        }

        struct aggregation_result* final_results = new struct aggregation_result[opt.num_queries()];
        for (int round = 0; round < 20; round++) {
            time_nested<true>("aggregation", [&]() {
                pm.run(0, opt.num_queries(), [&](size_t tid, size_t s, size_t e) {
                    for (size_t i = s; i < e; i++) {
                        final_results[i].query_id = (int) i;
                        final_results[i].value = 0;
                    }
                });

                pm.run(0, pm.get_parallelism(), [&](size_t tid, size_t s, size_t e) {
                    assert(s + 1 == e);
                    for (int dpu_id = 0; dpu_id < opt.num_dpus(); dpu_id++) {
                        size_t offset = first_index[tid][dpu_id];
                        for (size_t i = 0; i < sent_query_id[tid][dpu_id].size(); i++)
                            final_results[sent_query_id[tid][dpu_id][i]].value += results[dpu_id][i + offset].value;
                    } 
                });
            }, timer);
        }

        // verify
        {
            int64_t sum = 0;
            for (int i = 0; i < opt.num_queries(); i++)
                sum += final_results[i].value;
            printf("sum = %ld\n", sum);
            if (sum != 8738202)
                fprintf(stderr, "sum is not correct\n");
        }
    });

    //gettimeofday(&end, NULL);
    //printf("elapsed time: %lf sec\n", (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0);


    timer::active = false;
    print_all_timers(print_type::pt_full);

    return 0;
}