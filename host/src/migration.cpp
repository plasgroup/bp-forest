#include "migration.hpp"
#include "common.h"
#include "host_data_structures.hpp"
#include "node_defs.hpp"
#include <algorithm>
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <map>

#define min2(a, b) ((a) < (b) ? (a) : (b))
#define min3(a, b, c) ((a) < (b) ? min2(a, c) : min2(b, c))

Migration::Migration(HostTree* tree)
{
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        used_seats[i] = tree->get_used_seats(i);
        nr_used_seats[i] = __builtin_popcount(used_seats[i]);
        freeing_seats[i] = 0;
        nr_freeing_seats[i] = 0;
    }
    std::fill<Position*, Position>(&plan[0][0], &plan[NR_DPUS][0], {-1, -1});
}

Migration::Position Migration::get_source(dpu_id_t dpu, seat_id_t seat_id)
{
    if (!(used_seats[dpu] & (1 << seat_id)))
        /* not used */
        return {-1, -1};
    while (plan[dpu][seat_id].first != -1) {
        Position p = plan[dpu][seat_id];
        dpu = p.first;
        seat_id = p.second;
    }
    return {dpu, seat_id};
}

int Migration::get_num_queries_for_source(BatchCtx& batch_ctx, dpu_id_t dpu, seat_id_t seat_id)
{
    Position p = get_source(dpu, seat_id);
    if (p.first != -1)
        return batch_ctx.num_keys_for_tree[p.first][p.second];
    return 0;
}

void Migration::do_migrate_subtree(dpu_id_t from_dpu, seat_id_t from, dpu_id_t to_dpu, seat_id_t to)
{
    /* Source tree may not be in (from_dpu, from) because it may be planned to
     * move to there in the same migration plan. In that case, the chain of
     * the migrations are followed when the plan is applied. This is possible
     * because of the invariant that no used seat at the beginning is not
     * chosen as a destination of a migration.
     */
    plan[to_dpu][to] = {from_dpu, from};
}

void Migration::migrate_subtree(dpu_id_t from_dpu, seat_id_t from, dpu_id_t to_dpu, seat_id_t to)
{
    do_migrate_subtree(from_dpu, from, to_dpu, to);
    used_seats[from_dpu] &= ~(1 << from);
    freeing_seats[from_dpu] |= 1 << from;
    used_seats[to_dpu] |= 1 << to;
    nr_used_seats[from_dpu]--;
    nr_freeing_seats[from_dpu]++;
    nr_used_seats[to_dpu]++;
}

bool Migration::migrate_subtree_to_balance_load(dpu_id_t from_dpu, dpu_id_t to_dpu, int diff, int nkeys_for_trees[NR_DPUS][NR_SEATS_IN_DPU])
{
    seat_id_t candidate = INVALID_SEAT_ID;
    int best = std::numeric_limits<int>::max();
    for (seat_id_t from = 0; from < NR_SEATS_IN_DPU; from++) {
        if (!(used_seats[from_dpu] & (1 << from)))
            continue;
        Position p = get_source(from_dpu, from);
        int nkeys = nkeys_for_trees[p.first][p.second];
        if (nkeys >= diff)
            continue;
        int score = abs(nkeys * 2 - diff);
        if (score < best) {
            best = score;
            candidate = from;
        }
    }
    if (candidate != INVALID_SEAT_ID) {
        seat_set_t unavail = used_seats[to_dpu] | freeing_seats[to_dpu];
        seat_id_t to = 0;
        while ((unavail & (1 << to)))
            to++;
        assert(to < NR_SEATS_IN_DPU);
        migrate_subtree(from_dpu, candidate, to_dpu, to);
        return true;
    }
    return false;
}

void Migration::migration_plan_query_balancing(BatchCtx& batch_ctx, int num_migration)
{
    dpu_id_t dpu_ids[NR_DPUS];
    int nr_keys_for_dpu[NR_DPUS];

    for (dpu_id_t i = 0; i < NR_DPUS; i++)
        dpu_ids[i] = i;
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        int nkeys = 0;
        for (int j = 0; j < NR_SEATS_IN_DPU; j++) {
            nkeys += get_num_queries_for_source(batch_ctx, i, j);
        }
        nr_keys_for_dpu[i] = nkeys;
    }
    /* sort `dpu_ids` from many queries to few queries */
    std::sort(dpu_ids, dpu_ids + NR_DPUS, [&](dpu_id_t a, dpu_id_t b) {
        return nr_keys_for_dpu[a] > nr_keys_for_dpu[b];
    });

    dpu_id_t l = 0, r = NR_DPUS - 1;
    for (int i = 0; i < num_migration;) {
        if (l >= r)
            break;
        if (nr_used_seats[dpu_ids[l]] <= 1) {
            l++;
            continue;
        }
        if (nr_used_seats[dpu_ids[r]] >= SOFT_LIMIT_NR_TREES_IN_DPU || nr_used_seats[dpu_ids[r]] + nr_freeing_seats[dpu_ids[r]] >= NR_SEATS_IN_DPU) {
            r--;
            continue;
        }
        int diff = nr_keys_for_dpu[dpu_ids[l]] - nr_keys_for_dpu[dpu_ids[r]];
        if (!migrate_subtree_to_balance_load(dpu_ids[l], dpu_ids[r], diff, batch_ctx.num_keys_for_tree)) {
            l++;
            continue;
        }
        l++;
        r--;
        i++;
    }
}

void Migration::migrate_subtrees(dpu_id_t from_dpu, dpu_id_t to_dpu, int n)
{
    seat_set_t from_used = used_seats[from_dpu];
    seat_set_t to_used = used_seats[to_dpu];
    seat_set_t freeing = freeing_seats[to_dpu];
    seat_set_t moving_from = 0;
    seat_set_t moving_to = 0;
    seat_id_t from = 0, to = 0;

    for (int i = 0; i < n; i++) {
        while (!(from_used & (1 << from)))
            from++;
        assert(from < NR_SEATS_IN_DPU);
        while (((to_used | freeing) & (1 << to)))
            to++;
        assert(to < NR_SEATS_IN_DPU);

        /* migrate tree (from_dpu, form) -> (to_dpu, to) */
        moving_from |= 1 << from;
        moving_to |= 1 << to;
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

void Migration::migration_plan_memory_balancing()
{
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
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
                assert(j < NR_DPUS);
            }
        }
    }
}
bool Migration::plan_merge(Position left, Position right, merge_info_t* merge_info)
{
    if (left.first != right.first && nr_used_seats[left.first] == NR_SEATS_IN_DPU) {
        return false;
    } else if (left.first != right.first) {
        seat_set_t unavail = used_seats[left.first] | freeing_seats[left.first];
        seat_id_t to = 0;
        while ((unavail & (1 << to)))
            to++;
        migrate_subtree(right.first, right.second, left.first, to);
    }
    merge_info[left.first].merge_list[merge_info[left.first].merge_list_size] = left.second;
    merge_info[left.first].merge_list_size++;
    merge_info[left.first].merge_list[merge_info[left.first].merge_list_size] = right.second;
    merge_info[left.first].merge_list_size++;
    merge_info[left.first].tree_nums[merge_info->num_merge] = 2;
    merge_info[left.first].num_merge++;
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
        Position left = it->second;
        Position right = it_next->second;
        if (host_tree->num_kvpairs[left.first][left.second] + host_tree->num_kvpairs[right.first][right.second] < MERGE_THRESHOLD) {
            if (plan_merge(left, right, merge_list)) {
                it++;
            }
        }
        it++;
    }
}

static void send_nodes_from_dpu_to_dpu(dpu_id_t from_DPU, seat_id_t from_tree, dpu_id_t to_DPU, seat_id_t to_tree, dpu_set_t set, dpu_set_t dpu)
{
    uint64_t task;
    /* grobal variables difined in host.cpp */
    extern dpu_id_t each_dpu;
    extern BPTreeNode nodes_buffer[MAX_NUM_NODES_IN_SEAT];
    extern uint64_t nodes_num;
    DPU_FOREACH(set, dpu, each_dpu)
    {
        if (each_dpu == from_DPU) {
            task = (((uint64_t)from_tree) << 32) | TASK_FROM;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            // TODO: nodes_bufferを増やし、並列にmigrationを行う
            DPU_ASSERT(dpu_copy_from(dpu, "tree_transfer_num", 0, &nodes_num, sizeof(uint64_t)));
            DPU_ASSERT(dpu_copy_from(dpu, "tree_transfer_buffer", 0, &nodes_buffer, nodes_num * sizeof(BPTreeNode)));
            break;
        }
    }
    DPU_FOREACH(set, dpu, each_dpu)
    {
        if (each_dpu == to_DPU) {
            task = (((uint64_t)to_tree) << 32) | TASK_TO;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_num));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "tree_transfer_num", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &nodes_buffer));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "tree_transfer_buffer", 0, nodes_num * sizeof(BPTreeNode), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "task_no", 0, sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            break;
        }
    }
}

void Migration::normalize()
{
    for (dpu_id_t i = 0; i < NR_DPUS; i++)
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            if (plan[i][j].first != -1) {
                Position p = plan[i][j];
                while (plan[p.first][p.second].first != -1) {
                    Position q = plan[p.first][p.second];
                    plan[p.first][p.second] = {-1, -1};
                    p = q;
                }
                plan[i][j] = p;
            }
}

/* execute migration according to migration_plan */
void Migration::execute(dpu_set_t set, dpu_set_t dpu)
{
    normalize();
    /* apply */
    for (dpu_id_t i = 0; i < NR_DPUS; i++)
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            if (plan[i][j].first != -1) {
                dpu_id_t from_dpu = plan[i][j].first;
                seat_id_t from = plan[i][j].second;
#ifdef PRINT_DEBUG
                printf("do migration: (%d, %d) -> (%d, %d)\n", from_dpu, from, i, j);
#endif
                send_nodes_from_dpu_to_dpu(from_dpu, from, i, j, set, dpu);
            }
}


void Migration::print_plan()
{
    printf("====== plan ======\n");
    printf("  :");
    for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
        printf("%5d|", j);
    printf("\n");
    for (dpu_id_t i = 0; i < NR_DPUS; i++) {
        printf("%2d:", i);
        for (seat_id_t j = 0; j < NR_SEATS_IN_DPU; j++)
            printf("%2d,%2d|", plan[i][j].first, plan[i][j].second);
        printf("\n");
    }
}
