#ifndef __HOST_DATA_STRUCTURES_HPP__
#define __HOST_DATA_STRUCTURES_HPP__

#include <stdlib.h>
#include <assert.h>
#include <cstring>
#include <cstdio>
#include <map>
#include "common.h"

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
    key_int64_t tree_to_key_map[NR_DPUS][NR_SEATS_IN_DPU];

public:
    uint64_t tree_bitmap[NR_DPUS];
    int num_seats_used[NR_DPUS];
    int num_kvpairs[NR_DPUS][NR_SEATS_IN_DPU];
    HostTree(int init_trees_per_dpu)
    {
        assert(KEY_MIN == 0);
        int nr_init_trees = NR_DPUS * init_trees_per_dpu;

        memset(&tree_to_key_map, 0, sizeof(tree_to_key_map));
        memset(&num_kvpairs, 0, sizeof(num_kvpairs));

        key_int64_t q = KEY_MAX / nr_init_trees;
        key_int64_t r = KEY_MAX % nr_init_trees;
        for (int i = 0; i < NR_DPUS; i++) {
            for (int j = 0; j < init_trees_per_dpu; j++) {
                int nth = i * init_trees_per_dpu + j + 1;
                key_int64_t ub = q * nth + (r * nth) / nr_init_trees;
                key_to_tree_map[ub] = seat_addr_t(i, j);
                tree_to_key_map[i][j] = ub;
            }
            tree_bitmap[i] = (1ULL << init_trees_per_dpu) - 1;
            num_seats_used[i] = init_trees_per_dpu;
        }
    }

    key_int64_t inverse(seat_addr_t seat_addr)
    {
        return tree_to_key_map[seat_addr.dpu][seat_addr.seat];
    }

    void inv_map_add(seat_addr_t seat_addr, key_int64_t ub)
    {
        tree_bitmap[seat_addr.dpu] |= 1ULL << seat_addr.seat;
        num_seats_used[seat_addr.dpu]++;
        tree_to_key_map[seat_addr.dpu][seat_addr.seat] = ub;
    }

    void inv_map_del(seat_addr_t seat_addr)
    {
        tree_bitmap[seat_addr.dpu] &= ~(1ULL << seat_addr.seat);
        num_seats_used[seat_addr.dpu]--;
        tree_to_key_map[seat_addr.dpu][seat_addr.seat] = 0; // invalid kye?
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
        tree_bitmap[dpu] &= ~(1ULL << seat);
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