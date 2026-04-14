#include "load_dist.hpp"
#include "load_dist_evaluator.hpp"
#include "partition.hpp"
#include "pimtree_query.hpp"

#include <cmdline.h>

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <vector>


namespace cmdline
{
template <>
struct default_reader<std::optional<std::string>> {
    std::optional<std::string> operator()(const std::string& str)
    {
        return str;
    }
};
namespace detail
{
template <>
inline std::string default_value<std::optional<std::string>>(std::optional<std::string> opt_str)
{
    return opt_str.value_or("(nullopt)");
}
template <>
inline std::string readable_typename<std::optional<std::string>>()
{
    return "optional<string>";
}
}  // namespace detail
}  // namespace cmdline

struct CMDOpt {
    std::string partition_file;
    std::string queries_file;
    size_t batch_size;
    bool commutative;
    std::optional<std::string> load_file, cold_load_file, hot_load_file;
    unsigned nthreads;

    CMDOpt(int argc, char* argv[])
    {
        cmdline::parser parser;
        parser.add<std::string>("partition", 'p', "file path to pre-calculated partitioning", true);
        parser.add<std::string>("workload", 'w', "file path to PIM-Tree workload file", true);
        parser.add<size_t>("batch-size", 'b', "number of queries per batch", true);
        parser.add("commutative", 'c', "whether range queries are commutative");
        parser.add<std::optional<std::string>>("load", 0, "File path to output the computational load (optional)", false);
        parser.add<std::optional<std::string>>("cold-load", 0, "File path to output the computational load on cold partitions (optional)", false);
        parser.add<std::optional<std::string>>("hot-load", 0, "File path to output the computational load on hot partitions (optional)", false);
        parser.add<unsigned>("num_threads", 't', "num of threads", false, std::numeric_limits<unsigned>::max());
        parser.parse_check(argc, argv);

        partition_file = parser.get<std::string>("partition");
        queries_file = parser.get<std::string>("workload");
        batch_size = parser.get<size_t>("batch-size");
        commutative = parser.exist("commutative");
        load_file = parser.get<std::optional<std::string>>("load");
        cold_load_file = parser.get<std::optional<std::string>>("cold-load");
        hot_load_file = parser.get<std::optional<std::string>>("hot-load");
        nthreads = parser.get<unsigned>("num_threads");

        if (nthreads == std::numeric_limits<unsigned>::max()) {
            nthreads = std::thread::hardware_concurrency();
        }
        if (nthreads == 0) {
            nthreads = 4;
        }
    }
};


int main(int argc, char* argv[])
{
    const CMDOpt opt{argc, argv};
    LoadDistEvaluator router{load_partition(opt.partition_file), opt.nthreads};
    const pimtree_queries queries = make_pimtree_queries(opt.queries_file);

    std::ofstream load_ofs, cold_load_ofs, hot_load_ofs;
    if (opt.load_file) {
        load_ofs.open(*opt.load_file);
    }
    if (opt.cold_load_file) {
        cold_load_ofs.open(*opt.cold_load_file);
    }
    if (opt.hot_load_file) {
        hot_load_ofs.open(*opt.hot_load_file);
    }

    std::vector<size_t> load;
    LoadDist::PerOperation total_per_operation{};
    for (size_t batch_idx = 0, offset = 0; offset < queries.length; batch_idx++, offset += opt.batch_size) {
        const size_t current_batch_size = std::min(opt.batch_size, queries.length - offset);
        const LoadDist result = router.route_queries(&queries.ops[offset], current_batch_size, opt.commutative);

        const size_t nr_dpus = result.per_partition.size() / 2;

        load.resize(nr_dpus);
        for (size_t dpu_idx = 0; dpu_idx < nr_dpus; ++dpu_idx) {
            load[dpu_idx] = result.per_partition[dpu_idx] + result.per_partition[nr_dpus + dpu_idx];
        }

        const size_t max_load = *std::max_element(load.begin(), load.end()), sum_load = std::accumulate(load.begin(), load.end(), 0u);
        const double avg_load = static_cast<double>(sum_load) / static_cast<double>(nr_dpus),
                     imbalance = static_cast<double>(max_load) / avg_load;
        std::cout << "Batch " << batch_idx << ": "
                  << "imbalance = " << imbalance << ", "
                  << "(max load = " << max_load << ")" << std::endl;

        if (load_ofs.is_open()) {
            if (nr_dpus > 0) {
                load_ofs << load[0];
            }
            for (size_t dpu_idx = 1; dpu_idx < nr_dpus; ++dpu_idx) {
                load_ofs << "," << load[dpu_idx];
            }
            load_ofs << std::endl;
        }
        if (cold_load_ofs.is_open()) {
            if (nr_dpus > 0) {
                cold_load_ofs << result.per_partition[0];
            }
            for (size_t dpu_idx = 1; dpu_idx < nr_dpus; ++dpu_idx) {
                cold_load_ofs << "," << result.per_partition[dpu_idx];
            }
            cold_load_ofs << std::endl;
        }
        if (hot_load_ofs.is_open()) {
            if (nr_dpus > 0) {
                hot_load_ofs << result.per_partition[nr_dpus + 0];
            }
            for (size_t dpu_idx = 1; dpu_idx < nr_dpus; ++dpu_idx) {
                hot_load_ofs << "," << result.per_partition[nr_dpus + dpu_idx];
            }
            hot_load_ofs << std::endl;
        }

        for (size_t i = 0; i < total_per_operation.size(); ++i) {
            total_per_operation[i] += result.per_operation[i];
        }
    }

    std::cout << "#Queries per operation:" << std::endl;
    std::cout << "  empty:       " << total_per_operation[empty_t] << std::endl;
    std::cout << "  get:         " << total_per_operation[get_t] << std::endl;
    std::cout << "  update:      " << total_per_operation[update_t] << std::endl;
    std::cout << "  predecessor: " << total_per_operation[predecessor_t] << std::endl;
    std::cout << "  scan:        " << total_per_operation[scan_t] << std::endl;
    std::cout << "  insert:      " << total_per_operation[insert_t] << std::endl;
    std::cout << "  remove:      " << total_per_operation[remove_t] << std::endl;
    std::cout << "  invalid:     " << total_per_operation.back() << std::endl;
    return 0;
}
