#include "migration.hpp"
#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"
#include "upmem.hpp"

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <ostream>

static void migration_plan_query_balancing_impl(
    const std::array<unsigned, MAX_NR_DPUS>& load_ary,
    const std::array<unsigned, MAX_NR_DPUS + 1>& load_ary_psum,
    dpu_id_t idx_begin_dpu, dpu_id_t idx_end_dpu,
    double load_from_left, double load_to_right,
    std::array<migration_ratio_param_t, MAX_NR_DPUS>& result_load_flow)
{
    const double ideal_load = (load_ary_psum[idx_end_dpu] - load_ary_psum[idx_begin_dpu] + load_from_left - load_to_right) / (idx_end_dpu - idx_begin_dpu);

    double flow = load_from_left, max_abs_rel_flow = 0.0, clamped_flow = 0.0;
    dpu_id_t idx_sep = 0;
    for (dpu_id_t idx_dpu = idx_begin_dpu; idx_dpu + 1 < idx_end_dpu; idx_dpu++) {
        flow = load_ary[idx_dpu] + flow - ideal_load;
        const unsigned orig_load = load_ary[idx_dpu + (flow < 0)];
        const double rel_flow = flow / orig_load, abs_rel_flow = std::abs(rel_flow);

        if (abs_rel_flow > 1.0) {
            if (abs_rel_flow > max_abs_rel_flow) {
                max_abs_rel_flow = abs_rel_flow;
                idx_sep = idx_dpu + 1;
                clamped_flow = std::copysign(static_cast<double>(orig_load), flow);
            }
        } else if (idx_sep == 0) {
            if (rel_flow > 0.0) {
                result_load_flow[idx_dpu].right_npairs_ratio_x2147483648 = static_cast<uint32_t>(rel_flow * 2147483648);
                result_load_flow[idx_dpu + 1].left_npairs_ratio_x2147483648 = 0;
            } else {
                result_load_flow[idx_dpu].right_npairs_ratio_x2147483648 = 0;
                result_load_flow[idx_dpu + 1].left_npairs_ratio_x2147483648 = static_cast<uint32_t>(-rel_flow * 2147483648);
            }
        }
    }

    if (idx_sep != 0) {
        if (clamped_flow > 0.0) {
            result_load_flow[idx_sep - 1].right_npairs_ratio_x2147483648 = 2147483648u;
            result_load_flow[idx_sep].left_npairs_ratio_x2147483648 = 0;
        } else {
            result_load_flow[idx_sep - 1].right_npairs_ratio_x2147483648 = 0;
            result_load_flow[idx_sep].left_npairs_ratio_x2147483648 = 2147483648u;
        }
        migration_plan_query_balancing_impl(load_ary, load_ary_psum, idx_begin_dpu, idx_sep, load_from_left, clamped_flow, result_load_flow);
        migration_plan_query_balancing_impl(load_ary, load_ary_psum, idx_sep, idx_end_dpu, clamped_flow, load_to_right, result_load_flow);
    }
}

void Migration::migration_plan_query_balancing(const HostTree& host_tree, const size_t /* num_keys_batch */, const BatchCtx& batch_ctx)
{
    std::array<unsigned, MAX_NR_DPUS + 1> load_ary_psum;
    load_ary_psum[0] = 0;
    std::partial_sum(batch_ctx.num_keys_for_DPU.begin(), batch_ctx.num_keys_for_DPU.begin() + host_tree.get_nr_dpus(),
        load_ary_psum.begin() + 1);

    migration_plan_query_balancing_impl(batch_ctx.num_keys_for_DPU, load_ary_psum, 0, host_tree.get_nr_dpus(), 0.0, 0.0, plan);
}

void Migration::migration_plan_memory_balancing()
{
    // TODO
}

/* execute migration according to migration_plan */
bool Migration::execute(HostTree& /* host_tree */)
{
    upmem_migrate_kvpairs(plan);
    return true;
}

void Migration::print(std::ostream &os, const dpu_id_t nr_dpus_to_print)
{
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_dpus_to_print && idx_dpu < plan.size(); idx_dpu++) {
        os << "DPU[" << idx_dpu << "]: "
           << (plan.at(idx_dpu).left_npairs_ratio_x2147483648 / 2147483648.0)
           << " <-  -> "
           << (plan.at(idx_dpu).right_npairs_ratio_x2147483648 / 2147483648.0)
           << std::endl;
    }
}
