#pragma once

#include "assert.hpp"
#include "load_dist.hpp"
#include "parallel.hpp"
#include "partition.hpp"
#include "pimtree_query.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <utility>
#include <vector>


class LoadDistEvaluator : public ParallelManager<LoadDistEvaluator>
{
    std::vector<int64_t> delims;
    std::vector<uint16_t> dests;
    uint16_t nr_dpus;

public:
    explicit LoadDistEvaluator(const std::vector<Partition>& partitions, unsigned nthreads = 1)
        : ParallelManager{nthreads}, thread_results(nthreads)
    {
        set_partitions(partitions);
        parallel_run(&LoadDistEvaluator::set_thread_result);
    }

    void set_partitions(const std::vector<Partition>& partitions)
    {
        ASSERT(partitions.size() <= std::numeric_limits<uint16_t>::max());
        ASSERT(partitions.size() % 2 == 0);

        delims.clear();
        dests.clear();

        const size_t nr_parts = partitions.size();
        nr_dpus = static_cast<uint16_t>(nr_parts / 2);
        const size_t max_nr_delims = nr_dpus * 3;
        delims.reserve(max_nr_delims);
        dests.reserve(max_nr_delims);

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
    }

    LoadDist route_queries(const operation queries[], size_t batch_size, bool commutative)
    {
        tmp_queries = queries;
        tmp_batch_size = batch_size;

        if (commutative) {
            parallel_run(&LoadDistEvaluator::route_queries_job<true>);
        } else {
            parallel_run(&LoadDistEvaluator::route_queries_job<false>);
        }

        LoadDist final_result;
        final_result.per_partition.resize(nr_dpus * 2, 0);
        final_result.per_operation.fill(0);

        for (const LoadDist* thread_result_ptr : thread_results) {
            const LoadDist& thread_result = *thread_result_ptr;
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
    static thread_local LoadDist thread_result;
    std::vector<LoadDist*> thread_results;
    const operation* tmp_queries;
    size_t tmp_batch_size;

    void set_thread_result(unsigned tid)
    {
        thread_results[tid] = &thread_result;
    }
    template <bool commutative>
    void route_queries_job(unsigned tid)
    {
        LoadDist& result = thread_result;
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
inline thread_local LoadDist LoadDistEvaluator::thread_result;
