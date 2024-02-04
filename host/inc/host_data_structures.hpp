#ifndef __HOST_DATA_STRUCTURES_HPP__
#define __HOST_DATA_STRUCTURES_HPP__

#include "common.h"
#include "host_params.hpp"
#include "piecewise_constant_workload.hpp"
#include "workload_types.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <map>
#include <numeric>

#ifdef PRINT_DEBUG
#include <cstdio>
#endif /* PRINT_DEBUG */

class Migration;

typedef struct seat_addr_t {
    uint32_t dpu;
    seat_id_t seat;
    seat_addr_t() : dpu(-1), seat(INVALID_SEAT_ID) {}
    seat_addr_t(uint32_t d, seat_id_t s) : dpu(d), seat(s) {}
    bool operator==(const struct seat_addr_t& that) const
    {
        return dpu == that.dpu && seat == that.seat;
    }
} seat_addr_t;

/* Data structures in host for managing subtrees in DPUs */
class HostTree
{
public:
    /* host tree: key is the maximum value of the range */
    std::map<key_int64_t, seat_addr_t> key_to_tree_map;

    /* reverse map of host tree */
private:
    key_int64_t tree_to_key_map[NR_DPUS][NR_SEATS_IN_DPU] = {};

public:
    uint64_t tree_bitmap[NR_DPUS];
    int num_seats_used[NR_DPUS];
    int num_kvpairs[NR_DPUS][NR_SEATS_IN_DPU] = {};
    HostTree(int init_trees_per_dpu, int num_init_reqs, PiecewiseConstantWorkloadMetadata& workload_dist)
    {
        assert(KEY_MIN == 0);
        const auto sum_density
            = std::accumulate(workload_dist.densities.cbegin(), workload_dist.densities.cend(),
                decltype(workload_dist.densities)::value_type{0});
        const int nr_init_trees = NR_DPUS * init_trees_per_dpu;
        const auto density_for_each_tree = sum_density / nr_init_trees;

        // the initial keys are key_interval * {0, 1, 2, ..., num_init_reqs - 1}
        const key_int64_t key_interval = KEY_MAX / num_init_reqs;
        key_int64_t last_key = key_int64_t{0} - key_interval;

        size_t idx_dist_piece = 0;
        double left_density_in_the_piece = workload_dist.densities.at(idx_dist_piece);
        for (uint32_t idx_dpu = 0; idx_dpu < NR_DPUS; idx_dpu++) {
            uint64_t tree_bitmap_for_this_dpu = 0;
            int num_seats_used_for_this_dpu = 0;
            for (seat_id_t idx_tree = 0; idx_tree < init_trees_per_dpu; idx_tree++) {
                auto density_left = density_for_each_tree;
                while (density_left >= left_density_in_the_piece) {
                    if (idx_dist_piece == workload_dist.densities.size() - 1) {
                        assert(idx_dpu == NR_DPUS - 1);
                        assert(idx_tree == init_trees_per_dpu - 1);
                        break;
                    }
                    density_left -= left_density_in_the_piece;
                    idx_dist_piece++;
                    left_density_in_the_piece = workload_dist.densities.at(idx_dist_piece);
                }
                left_density_in_the_piece -= density_left;
                const auto max_key = static_cast<key_int64_t>(
                                         workload_dist.intervals.at(idx_dist_piece + 1)
                                         - (workload_dist.intervals.at(idx_dist_piece + 1) - workload_dist.intervals.at(idx_dist_piece))
                                               * left_density_in_the_piece / workload_dist.densities.at(idx_dist_piece))
                                     / key_interval * key_interval;
                if (max_key == last_key) {
                    continue;
                }
                last_key = max_key;

                key_to_tree_map.emplace(max_key, seat_addr_t{idx_dpu, idx_tree});
                tree_to_key_map[idx_dpu][idx_tree] = max_key;
                tree_bitmap_for_this_dpu |= (1 << idx_tree);
            }
            tree_bitmap[idx_dpu] = tree_bitmap_for_this_dpu;
            num_seats_used[idx_dpu] = num_seats_used_for_this_dpu;
        }

        // adjust the number of KV pairs
        const key_int64_t max_key = key_interval * (num_init_reqs - 1);
        auto it = key_to_tree_map.lower_bound(max_key);
        if (it == key_to_tree_map.end()) {
            it--;
            key_to_tree_map.emplace(max_key, it->second);
            tree_to_key_map[it->second.dpu][it->second.seat] = max_key;
            key_to_tree_map.erase(it);
        } else {
            if (it->first != max_key) {
                auto new_it = key_to_tree_map.emplace(max_key, it->second).first;
                tree_to_key_map[it->second.dpu][it->second.seat] = max_key;
                key_to_tree_map.erase(it);
                it = std::move(new_it);
            }
            it++;
            while (it != key_to_tree_map.end()) {
                tree_to_key_map[it->second.dpu][it->second.seat] = 0;  // invalid key?
                tree_bitmap[it->second.dpu] &= ~(1u << it->second.seat);
                num_seats_used[it->second.dpu]--;

                auto next = std::next(it);
                key_to_tree_map.erase(it);
                it = std::move(next);
            }
        }
    }

    key_int64_t inverse(seat_addr_t seat_addr)
    {
        return tree_to_key_map[seat_addr.dpu][seat_addr.seat];
    }

    void inv_map_add(seat_addr_t seat_addr, key_int64_t ub)
    {
        tree_bitmap[seat_addr.dpu] |= 1 << seat_addr.seat;
        num_seats_used[seat_addr.dpu]++;
        tree_to_key_map[seat_addr.dpu][seat_addr.seat] = ub;
    }

    void inv_map_del(seat_addr_t seat_addr)
    {
        tree_bitmap[seat_addr.dpu] &= ~(1 << seat_addr.seat);
        num_seats_used[seat_addr.dpu]--;
        tree_to_key_map[seat_addr.dpu][seat_addr.seat] = 0;  // invalid key?
    }

    seat_set_t get_used_seats(int dpu)
    {
        return tree_bitmap[dpu];
    }

    void apply_migration(Migration* m);

    void remove(uint32_t dpu, seat_id_t seat)
    {
        key_int64_t lb = tree_to_key_map[dpu][seat];
        printf("remove (%d, %d) key = 0x%lx\n", dpu, seat, lb);
        key_to_tree_map.erase(lb);
        tree_to_key_map[dpu][seat] = 0;
        num_seats_used[dpu]--;
        tree_bitmap[dpu] &= ~(1 << seat);
    }

private:
};

/* Data structures in host for managing queries in a batch */
class BatchCtx
{
public:
    /* key_index: i番目のDPUのj番目の木へのクエリの開始インデックス */
    int key_index[NR_DPUS][NR_SEATS_IN_DPU + 1]{};
    int DPU_idx[NR_DPUS]{};
    int num_keys_for_DPU[NR_DPUS]{};
    int num_keys_for_tree[NR_DPUS][NR_SEATS_IN_DPU]{};
    int send_size{};
    BatchCtx()
    {
        for (int i = 0; i < NR_DPUS; i++) {
            DPU_idx[i] = i;
        }
    }

private:
};

#endif /* __HOST_DATA_STRUCTURES_HPP__ */