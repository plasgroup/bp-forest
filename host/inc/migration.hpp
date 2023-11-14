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
    using Migration_DPU_t = std::vector<Position>;
    using Migration_plan_t = std::vector<Migration_DPU_t>;

private:
    Migration_plan_t migration_plan;
    std::vector<std::vector<Position>> current_position;

public:
    Migration()
        : migration_plan(NR_DPUS, Migration_DPU_t(NR_SEATS_IN_DPU, {-1, -1})),
          current_position(NR_DPUS, std::vector<Position>(NR_SEATS_IN_DPU))
    {
        /* Initialization */
        for (int dpu = 0; dpu < NR_DPUS; ++dpu) {
            for (int seat = 0; seat < NR_SEATS_IN_DPU; ++seat) {
                current_position[dpu][seat] = {dpu, seat};
            }
        }
    }
    void migration_plan_get(HostTree* tree, BatchCtx& batch_ctx, int num_migration);
    void migration_plan_insert(HostTree* tree, BatchCtx& batch_ctx, int num_migration);
    Position getFinalPosition(int from_DPU, int from_seat);
    void plan_one_migration(int from_DPU, int from_seat, int to_DPU, int to_seat);
    void do_migration(dpu_set_t set, dpu_set_t dpu);

private:
    void migration_plan_query_balancing(HostTree* tree, BatchCtx& batch_ctx, int num_migration, int max_num_seats_in_DPU);
    void migration_plan_memory_balancing(HostTree* tree, BatchCtx& batch_ctx, int max_num_seats_in_DPU);
};

extern void send_nodes_from_dpu_to_dpu(int from_DPU, int from_tree, int to_DPU, int to_tree, dpu_set_t set, dpu_set_t dpu);
