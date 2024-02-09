#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <map>
#include "migration.hpp"
#include "common.h"
#include "upmem.hpp"
#include "host_data_structures.hpp"
#include "node_defs.hpp"

#define min2(a, b) ((a) < (b) ? (a) : (b))
#define min3(a, b, c) ((a) < (b) ? min2(a, c) : min2(b, c))

#define MIN_DIFF_NR_QUERIES_TO_MIGRATE (SPLIT_THRESHOLD / 100)

uint64_t pop_count_64bit(uint64_t x) 
{ 
    x = ((x & 0xaaaaaaaaaaaaaaaaUL) >> 1) 
      +  (x & 0x5555555555555555UL); 
    x = ((x & 0xccccccccccccccccUL) >> 2) 
      +  (x & 0x3333333333333333UL); 
    x = ((x & 0xf0f0f0f0f0f0f0f0UL) >> 4) 
      +  (x & 0x0f0f0f0f0f0f0f0fUL); 
    x = ((x & 0xff00ff00ff00ff00UL) >> 8) 
      +  (x & 0x00ff00ff00ff00ffUL); 
    x = ((x & 0xffff0000ffff0000UL) >> 16) 
      +  (x & 0x0000ffff0000ffffUL); 
    x = ((x & 0xffffffff00000000UL) >> 32) 
      +  (x & 0x00000000ffffffffUL); 
    return x; 
} 

Migration::Migration(HostTree* tree)
{
    for (uint32_t i = 0; i < NR_DPUS; i++) {
        used_seats[i] = tree->get_used_seats(i);
        nr_used_seats[i] = pop_count_64bit(used_seats[i]);
        freeing_seats[i] = 0;
        nr_freeing_seats[i] = 0;
    }
    for (uint32_t i = 0; i < NR_DPUS; i++)
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            plan[i][j] = seat_addr_t(-1, INVALID_SEAT_ID);
}

seat_id_t
Migration::find_available_seat(uint32_t dpu)
{
    seat_set_t unavail = used_seats[dpu] | freeing_seats[dpu];
    seat_id_t seat = 0;
    while ((unavail & (1ULL << seat)))
        seat++;
    return seat;
}

seat_addr_t
Migration::get_source(uint32_t dpu, seat_id_t seat_id)
{
    if (!(used_seats[dpu] & (1ULL << seat_id)))
        /* not used */
        return seat_addr_t(-1, INVALID_SEAT_ID);
    while (plan[dpu][seat_id].dpu != -1) {
        seat_addr_t p = plan[dpu][seat_id];
        dpu = p.dpu;
        seat_id = p.seat;
    }
    return seat_addr_t(dpu, seat_id);
}

int
Migration::get_num_queries_for_source(BatchCtx& batch_ctx, uint32_t dpu, seat_id_t seat_id)
{
    seat_addr_t p = get_source(dpu, seat_id);
    if (p.dpu != -1)
        return batch_ctx.num_keys_for_tree[p.dpu][p.seat];
    return 0;
}

void
Migration::do_migrate_subtree(uint32_t from_dpu, seat_id_t from, uint32_t to_dpu, seat_id_t to)
{
    /* Source tree may not be in (from_dpu, from) because it may be planned to
     * move to there in the same migration plan. In that case, the chain of
     * the migrations are followed when the plan is applied. This is possible
     * because of the invariant that no used seat at the beginning is not
     * chosen as a destination of a migration.
     */
    plan[to_dpu][to] = seat_addr_t(from_dpu, from);
}

void
Migration::migrate_subtree(uint32_t from_dpu, seat_id_t from, uint32_t to_dpu, seat_id_t to)
{
    do_migrate_subtree(from_dpu, from, to_dpu, to);
    used_seats[from_dpu] &= ~(1ULL << from);
    freeing_seats[from_dpu] |= 1ULL << from;
    used_seats[to_dpu] |= 1ULL << to;
    nr_used_seats[from_dpu]--;
    nr_freeing_seats[from_dpu]++;
    nr_used_seats[to_dpu]++;
}

bool
Migration::migrate_subtree_to_balance_load(uint32_t from_dpu, uint32_t to_dpu, int diff, int nkeys_for_trees[NR_DPUS][NR_SEATS_IN_DPU])
{
    seat_id_t candidate = INVALID_SEAT_ID;
    int best = std::numeric_limits<int>::max();
    for (seat_id_t from = 0; from < NR_SEATS_IN_DPU; from++) {
        if (!(used_seats[from_dpu] & (1ULL << from)))
            continue;
        seat_addr_t p = get_source(from_dpu, from);
        int nkeys = nkeys_for_trees[p.dpu][p.seat];
        if (nkeys >= diff)
            continue;
        int score = abs(nkeys * 2 - diff);
        if (score < best) {
            best = score;
            candidate = from;
        }
    }
    if (candidate != INVALID_SEAT_ID) {
        seat_id_t to = find_available_seat(to_dpu);
        assert(to < NR_SEATS_IN_DPU);
        migrate_subtree(from_dpu, candidate, to_dpu, to);
        return true;
    }
    return false;
}

void
Migration::migration_plan_query_balancing(BatchCtx& batch_ctx, int num_migration)
{
    uint32_t dpu_ids[NR_DPUS];
    int nr_keys_for_dpu[NR_DPUS];

    for (uint32_t i = 0; i < NR_DPUS; i++)
        dpu_ids[i] = i;
    for (uint32_t i = 0; i < NR_DPUS; i++) {
        int nkeys = 0;
        for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
            nkeys += get_num_queries_for_source(batch_ctx, i, j);
        }
        nr_keys_for_dpu[i] = nkeys;
    }
    /* sort `dpu_ids` from many queries to few queries */
    std::sort(dpu_ids, dpu_ids + NR_DPUS, [&](uint32_t a, uint32_t b) {
        return nr_keys_for_dpu[a] > nr_keys_for_dpu[b];
    });

    uint32_t l = 0, r = NR_DPUS - 1;
    for (int i = 0; i < num_migration;) {
        if (l >= r)
            break;
        if (nr_used_seats[dpu_ids[l]] <= 1) {
            break;
        }
        if (nr_used_seats[dpu_ids[r]] >= SOFT_LIMIT_NR_TREES_IN_DPU || nr_used_seats[dpu_ids[r]] + nr_freeing_seats[dpu_ids[r]] >= NR_SEATS_IN_DPU) {
            r--;
            continue;
        }
        int diff = nr_keys_for_dpu[dpu_ids[l]] - nr_keys_for_dpu[dpu_ids[r]];
        if (diff < MIN_DIFF_NR_QUERIES_TO_MIGRATE)
            break;
        if (diff < MIN_DIFF_NR_QUERIES_TO_MIGRATE)
            break;
        if (!migrate_subtree_to_balance_load(dpu_ids[l], dpu_ids[r], diff, batch_ctx.num_keys_for_tree)) {
            l++;
            continue;
        }
        l++;
        r--;
        i++;
    }
}

void
Migration::migrate_subtrees(uint32_t from_dpu, uint32_t to_dpu, int n)
{
    seat_set_t from_used = used_seats[from_dpu];
    seat_set_t to_used = used_seats[to_dpu];
    seat_set_t freeing = freeing_seats[to_dpu];
    seat_set_t moving_from = 0;
    seat_set_t moving_to = 0;
    seat_id_t from = 0, to = 0;

    printf("migrate_subtrees %d -> %d n=%d\n", from_dpu, to_dpu, n);

    for (int i = 0; i < n; i++) {
        while (!(from_used & (1ULL << from)))
            from++;
        assert(from < NR_SEATS_IN_DPU);
        while (((to_used | freeing) & (1ULL << to)))
            to++;
        assert(to < NR_SEATS_IN_DPU);

        /* migrate tree (from_dpu, form) -> (to_dpu, to) */
        moving_from |= 1ULL << from;
        moving_to |= 1ULL << to;
        do_migrate_subtree(from_dpu, from, to_dpu, to);
        from++;
        to++;
    }
    used_seats[from_dpu] &= ~moving_from;
    freeing_seats[from_dpu] |= moving_from;
    used_seats[to_dpu] |= moving_to;
    nr_used_seats[from_dpu] -= n;
    nr_freeing_seats[from_dpu] += n;
    nr_used_seats[to_dpu] += n;
}

void
Migration::migration_plan_memory_balancing()
{
    for (uint32_t i = 0; i < NR_DPUS; i++) {
        if (nr_used_seats[i] > SOFT_LIMIT_NR_TREES_IN_DPU) {
            int rem = nr_used_seats[i] - SOFT_LIMIT_NR_TREES_IN_DPU;
            int j = 0;
            while (rem > 0) {
                int room = SOFT_LIMIT_NR_TREES_IN_DPU - nr_used_seats[j];
                int avail = NR_SEATS_IN_DPU - (nr_used_seats[j] + nr_freeing_seats[j]);
                int n = min3(rem, room, avail);
                if (n > 0) {
                    migrate_subtrees(i, j, n);
                    rem -= n;
                }
                j++;
                assert(j < NR_DPUS); // all DPUs are almost full
            }
        }
    }
}

bool
Migration::plan_merge(seat_addr_t left, seat_addr_t right, merge_info_t* merge_info)
{
    uint32_t dpu = right.dpu;
    seat_id_t dest = right.seat;
    seat_id_t src = left.seat;

    if (left.dpu != right.dpu) {
        if (nr_used_seats[dpu] == NR_SEATS_IN_DPU)
            // destination is full
            return false;
        src = find_available_seat(dpu);
        migrate_subtree(left.dpu, left.seat, dpu, src);
    }
    merge_info[dpu].merge_to[src] = dest;
    return true;
}

void Migration::migration_plan_for_merge(HostTree* host_tree, merge_info_t* merge_list)
{
    auto it = host_tree->key_to_tree_map.begin();
    while (it != host_tree->key_to_tree_map.end()) {
        int nkeys = 0;
        auto it_next = it;
        it_next++;
        if (it_next == host_tree->key_to_tree_map.end())
            break;
        seat_addr_t left = it->second;
        seat_addr_t right = it_next->second;
        if (host_tree->num_kvpairs[left.dpu][left.seat] + host_tree->num_kvpairs[right.dpu][right.seat] < MERGE_THRESHOLD) {
            if (plan_merge(left, right, merge_list)) {
                it++;
            }
        }
        it++;
    }
}

void Migration::normalize()
{
    for (uint32_t i = 0; i < NR_DPUS; i++)
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            if (plan[i][j].dpu != -1) {
                seat_addr_t p = plan[i][j];
                while (plan[p.dpu][p.seat].dpu != -1) {
                    seat_addr_t q = plan[p.dpu][p.seat];
                    plan[p.dpu][p.seat] = seat_addr_t(-1, INVALID_SEAT_ID);
                    p = q;
                }
                plan[i][j] = p;
            }
}

/* execute migration according to migration_plan */
void
Migration::execute()
{
    normalize();
    /* apply */
    for (uint32_t i = 0; i < NR_DPUS; i++)
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            if (plan[i][j].dpu != -1) {
                uint32_t from_dpu = plan[i][j].dpu;
                seat_id_t from = plan[i][j].seat;
#ifdef PRINT_DEBUG
                printf("do migration: (%d, %d) -> (%d, %d)\n", from_dpu, from, i, j);
#endif
                upmem_send_nodes_from_dpu_to_dpu(from_dpu, from, i, j);
            }
}


void Migration::print_plan()
{
    printf("====== plan ======\n");
    printf("  :");
    for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
        printf("%5d|", j);
    printf("\n");
    for (uint32_t i = 0; i < NR_DPUS; i++) {
        printf("%2d:", i);
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            printf("%2d,%2d|", plan[i][j].dpu, plan[i][j].seat);
        printf("\n");
    }
}
