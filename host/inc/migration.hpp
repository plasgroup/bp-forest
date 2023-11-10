#include "common.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <map>

using Migration_DPU_t = std::pair<int, int>[NR_SEATS_IN_DPU];
using Migration_plan_ptr = Migration_DPU_t*;

extern Migration_plan_ptr migration_plan_init(Migration_plan_ptr arr);
extern Migration_plan_ptr migration_plan_get(int num_migration, int* idx, Migration_plan_ptr migration_dest, uint64_t* tree_bitmap, int* num_keys_for_DPU, int num_keys_for_tree[NR_DPUS][NR_SEATS_IN_DPU], std::map<key_int64_t, std::pair<int, int>>& key_to_tree_map);
