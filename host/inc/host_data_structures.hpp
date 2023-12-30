#ifndef __HOST_DATA_STRUCTURES_HPP__
#define __HOST_DATA_STRUCTURES_HPP__

#include <cstring>
#include <map>
#include <stdlib.h>
#include "common.h"
#include "migration.hpp"

/* Data structures in host for managing subtrees in DPUs */
class HostTree
{
public:
    uint64_t tree_bitmap[NR_DPUS];
    int num_seats_used[NR_DPUS];
    key_int64_t tree_to_key_map[NR_DPUS][NR_SEATS_IN_DPU];
    std::map<key_int64_t, std::pair<int, int>> key_to_tree_map;
    int num_kvpairs[NR_DPUS][NR_SEATS_IN_DPU]{};
    HostTree(key_int64_t range)
    {
        /* init key_to_tree_map */
        for (int i = 0; i < NR_DPUS * NUM_INIT_TREES_IN_DPU; i++) {

            key_to_tree_map[range * (i + 1)] = std::make_pair(i / NUM_INIT_TREES_IN_DPU, i % NUM_INIT_TREES_IN_DPU);
        }
        /* init tree_to_key_map */
        for (int i = 0; i < NR_DPUS; i++) {
            for (int j = 0; j < NUM_INIT_TREES_IN_DPU; j++) {
                tree_to_key_map[i][j] = range * (i * NUM_INIT_TREES_IN_DPU + j + 1);
            }
        }
        /* init tree_bitmap */
        for (uint64_t i = 0; i < NR_DPUS; i++) {
            tree_bitmap[i] = (1 << NUM_INIT_TREES_IN_DPU) - 1;
            num_seats_used[i] = NUM_INIT_TREES_IN_DPU;
        }

        memset(&num_kvpairs, 0, sizeof(num_kvpairs));
    }

    seat_set_t get_used_seats(int dpu)
    {
        return tree_bitmap[dpu];
    }

    void apply_migration(Migration* m)
    {
        for (auto it = m->begin(); it != m->end(); ++it) {
            Migration::Position from = (*it).first;
            Migration::Position to = (*it).second;
            key_int64_t key = tree_to_key_map[from.first][from.second];
            tree_bitmap[from.first] &= ~(1 << from.second);
            num_seats_used[from.first]--;
            tree_bitmap[to.first] |= 1 << to.second;
            num_seats_used[to.first]++;
            tree_to_key_map[to.first][to.second] = key;
            tree_to_key_map[from.first][from.second] = 0;  // invalid key?
            key_to_tree_map[key] = to;
        }
    }

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