#include "assert.hpp"
#include "parallel.hpp"
#include "partition.hpp"
#include "pimtree_query.hpp"
#include "workload_types.h"

#include <cmdline.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>


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

class Router : public ParallelManager<Router>
{
    std::vector<int64_t> delims;
    std::vector<uint16_t> dests;
    uint16_t nr_dpus;

public:
    explicit Router(const std::vector<Partition>& partitions, unsigned nthreads = 1)
        : ParallelManager{nthreads}, thread_results(nthreads)
    {
        ASSERT(partitions.size() <= std::numeric_limits<uint16_t>::max());
        ASSERT(partitions.size() % 2 == 0);

        const size_t nr_parts = partitions.size();
        nr_dpus = static_cast<uint16_t>(nr_parts / 2);
        const size_t max_nr_delims = nr_dpus * 3;
        delims.emplace_back(max_nr_delims);
        dests.emplace_back(max_nr_delims);

        std::vector<std::pair<Partition, uint16_t>> hot_parts;
        hot_parts.reserve(nr_dpus);
        for (uint16_t i = nr_dpus; i < nr_parts; ++i) {
            if (partitions[i].length > 0) {
                hot_parts.emplace_back(partitions[i], i);
            }
        }
        std::sort(hot_parts.begin(), hot_parts.end(),
            [](const auto& a, const auto& b) { return a.first.left_key < b.first.left_key; });

        auto hot_it = hot_parts.begin();
        ;
        for (uint16_t dpu_id = 0; dpu_id < nr_dpus; ++dpu_id) {
            const Partition& base_part = partitions[dpu_id];
            int64_t base_left = base_part.left_key;
            const int64_t base_right = static_cast<int64_t>(static_cast<uint64_t>(base_left) + base_part.length - 1u);
            while (hot_it != hot_parts.end()) {
                const Partition& hot_part = hot_it->first;
                const int64_t hot_left = hot_part.left_key;
                if (base_right < hot_left) {
                    break;
                }
                const int64_t hot_right = static_cast<int64_t>(static_cast<uint64_t>(hot_left) + hot_part.length - 1u);
                if (base_left < hot_left) {
                    delims.emplace_back(base_left);
                    dests.emplace_back(dpu_id);
                }
                delims.emplace_back(hot_left);
                dests.emplace_back(hot_it->second);
                hot_it++;

                base_left = hot_right + 1;
            }
            if (base_left <= base_right) {
                delims.emplace_back(base_left);
                dests.emplace_back(dpu_id);
            }
        }

        parallel_run(&Router::set_thread_result);
    }

    struct NrQueries {
        using PerPartition = std::vector<size_t>;
        using PerOperation = std::array<size_t, /* valid */ (remove_t + 1) + /* invalid */ 1>;
        PerPartition per_partition;
        PerOperation per_operation{};
    };
    NrQueries route_queries(const operation queries[], size_t batch_size, bool commutative)
    {
        tmp_queries = queries;
        tmp_batch_size = batch_size;

        if (commutative) {
            parallel_run(&Router::route_queries_job<true>);
        } else {
            parallel_run(&Router::route_queries_job<false>);
        }

        NrQueries final_result;
        final_result.per_partition.resize(nr_dpus * 2, 0);
        final_result.per_operation.fill(0);

        for (const NrQueries* thread_result_ptr : thread_results) {
            const NrQueries& thread_result = *thread_result_ptr;
            for (size_t i = 0; i < final_result.per_partition.size(); ++i) {
                final_result.per_partition[i] += thread_result.per_partition[i];
            }
            for (size_t i = 0; i < final_result.per_operation.size(); ++i) {
                final_result.per_operation[i] += thread_result.per_operation[i];
            }
        }
        return final_result;
    }

private:
    static thread_local NrQueries thread_result;
    std::vector<NrQueries*> thread_results;
    const operation* tmp_queries;
    size_t tmp_batch_size;

    void set_thread_result(unsigned tid)
    {
        thread_results[tid] = &thread_result;
    }
    template <bool commutative>
    void route_queries_job(unsigned tid)
    {
        NrQueries& result = thread_result;
        result.per_partition.clear();
        result.per_partition.resize(nr_dpus * 2, 0);
        result.per_operation.fill(0);

        const operation* queries = tmp_queries;
        const size_t batch_size = tmp_batch_size;
        const size_t qry_idx_begin = (batch_size * tid) / get_parallelism(),
                     qry_idx_end = (batch_size * (tid + 1)) / get_parallelism();

        thread_local std::vector<size_t> last_qry_idx;
        if (commutative) {
            last_qry_idx.clear();
            last_qry_idx.resize(nr_dpus * 2, std::numeric_limits<size_t>::max());
        }

        for (size_t qry_idx = qry_idx_begin; qry_idx < qry_idx_end; ++qry_idx) {
            const operation& op = queries[qry_idx];

            if (remove_t < op.type) {
                result.per_operation.back()++;
                continue;
            }

            result.per_operation[op.type]++;

            if (empty_t == op.type) {
                continue;
            }

            int64_t key;
            switch (op.type) {
            case get_t:
                key = op.tsk.g.key;
                break;
            case update_t:
                key = op.tsk.u.key;
                break;
            case predecessor_t:
                key = op.tsk.p.key;
                break;
            case scan_t:
                key = op.tsk.s.lkey;
                break;
            case insert_t:
                key = op.tsk.i.key;
                break;
            case remove_t:
                key = op.tsk.r.key;
                break;
            default:
                __builtin_unreachable();
            }

            std::vector<int64_t>::iterator next_of_part_it;
            if (op.type != predecessor_t) {
                next_of_part_it = std::upper_bound(delims.begin(), delims.end(), key);
            } else {
                next_of_part_it = std::lower_bound(delims.begin(), delims.end(), key);
            }
            const size_t next_of_part_idx = static_cast<size_t>(std::distance(delims.begin(), next_of_part_it));

            if (op.type != scan_t) {
                if (next_of_part_idx == 0) {
                    continue;
                }
                const size_t part_idx = next_of_part_idx - 1u;
                result.per_partition[dests[part_idx]]++;

            } else {  // range query
                const int64_t rkey = op.tsk.s.rkey;
                size_t part_idx;

                if (next_of_part_idx != 0) {
                    part_idx = next_of_part_idx - 1u;
                } else {
                    if (delims.front() <= rkey) {
                        part_idx = 0;
                    } else {
                        continue;
                    }
                }

                do {
                    const auto dest_idx = dests[part_idx];
                    if (commutative) {
                        if (last_qry_idx[dest_idx] != qry_idx) {
                            result.per_partition[dest_idx]++;
                            last_qry_idx[dest_idx] = qry_idx;
                        }
                    } else {
                        result.per_partition[dest_idx]++;
                    }

                    part_idx++;
                } while (part_idx != delims.size() && delims[part_idx] <= rkey);
            }
        }
    }
};
thread_local Router::NrQueries Router::thread_result;


int main(int argc, char* argv[])
{
    const CMDOpt opt{argc, argv};
    Router router{load_partition(opt.partition_file), opt.nthreads};
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
    Router::NrQueries::PerOperation total_per_operation{};
    for (size_t batch_idx = 0, offset = 0; offset < queries.length; batch_idx++, offset += opt.batch_size) {
        const size_t current_batch_size = std::min(opt.batch_size, queries.length - offset);
        const Router::NrQueries result = router.route_queries(&queries.ops[offset], current_batch_size, opt.commutative);

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
