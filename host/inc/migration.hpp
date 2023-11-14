#pragma once

#include "common.h"
#include "host_data_structures.hpp"
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#include <iostream>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <vector>
class Migration
{
public:
    using Position = std::pair<int, int>;

private:
    Position plan[NR_DPUS][NR_SEATS_IN_DPU];
    seat_set_t used_seats[NR_DPUS];
    seat_set_t freeing_seats[NR_DPUS];
    int nr_used_seats[NR_DPUS];
    int nr_freeing_seats[NR_DPUS];

public:
    Migration(HostTree* tree);
    Position get_source(int dpu, seat_id_t seat_id);
    void migration_plan_query_balancing(BatchCtx& batch_ctx, int num_migration);
    void migration_plan_memory_balancing(void);
    void normalize(void);
    void execute(dpu_set_t set, dpu_set_t dpu);

private:
    void do_migrate_subtree(int from_dpu, seat_id_t from, int to_dpu, seat_id_t to);
    void migrate_subtree(int from_dpu, seat_id_t from, int to_dpu, seat_id_t to);
    bool migrate_subtree_to_balance_load(int from_dpu, int to_dpu, int diff, int nkeys_for_trees[NR_DPUS][NR_SEATS_IN_DPU]);
    void migrate_subtrees(int from_dpu, int to_dpu, int n);
};
