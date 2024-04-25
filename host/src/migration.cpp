#include "migration.hpp"
#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"
#include "upmem.hpp"

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <numeric>
#include <ostream>

#ifdef BULK_MIGRATION
#include <limits>
#endif


#ifdef BULK_MIGRATION
void Migration::migration_plan_query_balancing(const HostTree& /* host_tree */, const size_t /* num_keys_batch */, const BatchCtx& batch_ctx)
{
    // p=0.01
    static std::array<double, 64> chi2_table = {
        std::numeric_limits<double>::infinity(), 6.63489660102121514, 9.21034037197618274, 11.3448667301443719, 11.3448667301443719,
        15.0862724693889901, 16.8118938297709311, 18.4753069065823637, 20.0902350296632332, 21.6659943334619258,
        23.2092511589543597, 24.7249703113182827, 26.2169673055358501, 27.6882496104570493, 29.1412377406727969,
        30.5779141668924937, 31.9999269088151812, 33.4086636050046177, 34.8053057347050729, 36.1908691292700527,
        37.5662347866250514, 38.9321726835160676, 40.2893604375938641, 41.6383981188584741, 42.9798201393516363,
        44.3141048962191654, 45.6416826662831503, 46.9629421247514463, 48.2782357703154944, 49.5878844728988306,
        50.8921813115170906, 50.8921813115170906, 53.4857718362353633, 54.7755397601103456, 56.0609087477890793,
        57.3420734338592477, 58.6192145016870519, 59.8925000450868996, 61.162086763689684, 62.4281210161848972,
        63.6907397515644631, 64.9500713352111808, 66.2062362839932533, 67.4593479223258332, 68.7095129693453901,
        69.9568320658382056, 71.2014002483115373, 72.4433073765482471, 73.6826385201057523, 74.9194743084781942,
        76.153891249012717, 77.3859620161372592, 78.6157557150024806, 79.8433381222514683, 81.0687719062971001,
        82.2921168291996714, 83.5134299319894096, 83.5134299319894096, 85.9501762451034685, 87.1657113997875694,
        88.3794189014493228, 89.5913444906870428, 90.8015320308386879, 92.0100236141319913};
    static_assert(MAX_NR_DPUS_IN_RANK <= 64);

    for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
        const std::pair<dpu_id_t, dpu_id_t> dpu_range = upmem_get_dpu_range_in_rank(idx_rank);
        const dpu_id_t idx_dpu_begin = dpu_range.first;
        const dpu_id_t idx_dpu_end = dpu_range.second;
        const dpu_id_t nr_dpus = idx_dpu_end - idx_dpu_begin;

        uint32_t sum_load = 0;
        uint64_t sum_pow2_load = 0;
        for (dpu_id_t idx_dpu = idx_dpu_begin; idx_dpu < idx_dpu_end; idx_dpu++) {
            const uint32_t load = batch_ctx.num_keys_for_DPU[idx_dpu];
            sum_load += load;
            sum_pow2_load += uint64_t{load} * uint64_t{load};
        }

        if (sum_load < nr_dpus * 10) {
            continue;  // too small load to perform chi^2 test
        }
        const double chi2 = static_cast<double>(sum_pow2_load * nr_dpus) / sum_load - sum_load;
        if (chi2 <= chi2_table[nr_dpus - 1]) {
            continue;  // the load distribution can be considered uniform
        }

        std::array<double, MAX_NR_DPUS_IN_RANK - 1> rank_plan;

        const double ideal_load = static_cast<double>(sum_load) / nr_dpus;
        dpu_id_t idx_src = 0;
        double orig_load_in_src = batch_ctx.num_keys_for_DPU[idx_src + idx_dpu_begin], rest_load_in_src = orig_load_in_src;
        for (dpu_id_t idx_dest = 0; idx_dest + 1 < nr_dpus; idx_dest++) {
            double load_left = ideal_load;
            while (load_left >= rest_load_in_src) {
                load_left -= rest_load_in_src;
                idx_src++;
                rest_load_in_src = orig_load_in_src = batch_ctx.num_keys_for_DPU[idx_src + idx_dpu_begin];
            }
            rest_load_in_src -= load_left;

            rank_plan[idx_dest] = idx_src + 1 - rest_load_in_src / orig_load_in_src;
        }

        plan[idx_rank] = rank_plan;
    }
}

#else /* BULK_MIGRATION */
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
                result_load_flow[idx_dpu].right_npairs_ratio_x2147483648 = 0;  // TODO: use below
                // result_load_flow[idx_dpu].right_npairs_ratio_x2147483648 = static_cast<uint32_t>(rel_flow * 2147483648);
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
#endif /* BULK_MIGRATION */

void Migration::migration_plan_memory_balancing()
{
    // TODO
}

/* execute migration according to migration_plan */
bool Migration::execute(HostTree& host_tree)
{
    upmem_migrate_kvpairs(plan, host_tree);
    return true;
}

void Migration::print(std::ostream& os, dpu_id_t nr_dpus_to_print)
{
    std::ios_base::fmtflags f{os.flags()};
    os << std::fixed << std::setprecision(1);

#ifdef BULK_MIGRATION
    for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
        const std::pair<dpu_id_t, dpu_id_t> dpu_range = upmem_get_dpu_range_in_rank(idx_rank);
        const dpu_id_t idx_dpu_begin = dpu_range.first;
        const dpu_id_t idx_dpu_end = dpu_range.second;

        for (dpu_id_t idx_dpu = idx_dpu_begin; idx_dpu < idx_dpu_end; idx_dpu++) {
            if (idx_dpu >= nr_dpus_to_print) {
                os.flags(f);
                return;
            }

            if (plan[idx_rank].has_value()) {
                os << "DPU[" << std::setw(4) << idx_dpu;
                if (idx_dpu == idx_dpu_begin) {
                    os << "]: [  0.0% of DPU[" << std::setw(4) << idx_dpu;
                } else {
                    const double begin_pos = plan[idx_rank].value()[idx_dpu - idx_dpu_begin - 1];
                    const dpu_id_t idx_src_in_rank = static_cast<dpu_id_t>(begin_pos);
                    os << "]: [" << std::setw(5) << (begin_pos - idx_src_in_rank) * 100 << "% "
                       << "of DPU[" << std::setw(4) << idx_src_in_rank + idx_dpu_begin;
                }

                if (idx_dpu + 1 != idx_dpu_end) {
                    const double end_pos = plan[idx_rank].value()[idx_dpu - idx_dpu_begin];
                    const dpu_id_t idx_src_in_rank = static_cast<dpu_id_t>(end_pos);
                    os << "], " << std::setw(5) << (end_pos - idx_src_in_rank) * 100 << "% "
                       << "of DPU[" << std::setw(4) << idx_src_in_rank + idx_dpu_begin << "])" << std::endl;
                } else {
                    os << "],   0.0% of DPU[" << std::setw(4) << (idx_dpu + 1) << "])" << std::endl;
                }
            } else {
                os << "DPU[" << std::setw(4) << idx_dpu << "]: [  0.0% of DPU[" << std::setw(4) << idx_dpu
                   << "],   0.0% of DPU[" << std::setw(4) << (idx_dpu + 1) << "])" << std::endl;
            }
        }
    }

#else /* BULK_MIGRATION */
    nr_dpus_to_print = std::min(nr_dpus_to_print, upmem_get_nr_dpus());
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_dpus_to_print; idx_dpu++) {
        os << "DPU[" << std::setw(4) << idx_dpu << "]: "
           << std::setw(5) << (plan.at(idx_dpu).left_npairs_ratio_x2147483648 / 2147483648.0) * 100
           << "% <- -> "
           << std::setw(5) << (plan.at(idx_dpu).right_npairs_ratio_x2147483648 / 2147483648.0) * 100
           << '%'
           << std::endl;
    }
#endif

    os.flags(f);
}
