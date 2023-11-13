#pragma once 

#include "common.h"
#include "host_data_structures.hpp"
extern "C" {
    #include <dpu.h>
    #include <dpu_log.h>
}
#include <vector>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <map>

using Migration_DPU_t = std::vector<std::pair<int, int>>;
using Migration_plan_t = std::vector<Migration_DPU_t>;

extern Migration_plan_t migration_plan_new();
extern Migration_plan_t& migration_plan_init(Migration_plan_t& migration_plan);
extern Migration_plan_t& migration_plan_get(Migration_plan_t& migration_plan, HostTree* tree, BatchCtx& batch_ctx, int num_migration);
extern Migration_plan_t& migration_plan_insert(Migration_plan_t& migration_plan, HostTree* tree, BatchCtx& batch_ctx, int num_migration);
extern void do_migration(Migration_plan_t& migration_plan, dpu_set_t set, dpu_set_t dpu);
