#ifndef __HOST_DATA_STRUCTURES_HPP__
#define __HOST_DATA_STRUCTURES_HPP__

#include "common.h"
#include "host_params.hpp"
#include "piecewise_constant_workload.hpp"
#include "workload_types.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iosfwd>
#include <iterator>
#include <limits>
#include <map>
#include <numeric>
#include <optional>


/* Data structures in host for managing subtrees in DPUs */
class HostTree
{
private:
    std::array<key_int64_t, MAX_NR_DPUS> lower_bounds;
    std::array<uint32_t, MAX_NR_DPUS> num_kvpairs;
    dpu_id_t nr_dpus;

    HostTree() {}
    friend HostTree initialize_bpforest(PiecewiseConstantWorkloadMetadata& workload_dist);
    friend void upmem_receive_num_kvpairs(HostTree* host_tree, float* receive_time);
    friend void upmem_migrate_kvpairs(MigrationPlanType&, HostTree&);

public:
    dpu_id_t get_nr_dpus() const { return nr_dpus; }

    key_int64_t get_lower_bound(dpu_id_t idx_dpu) const
    {
        assert(0 <= idx_dpu && idx_dpu < nr_dpus);
        return lower_bounds[idx_dpu];
    }
    uint32_t get_num_kvpairs(dpu_id_t idx_dpu) const
    {
        assert(0 <= idx_dpu && idx_dpu < nr_dpus);
        return num_kvpairs[idx_dpu];
    }

    dpu_id_t dpu_responsible_for_get_query_with(key_int64_t key) const
    {
        // `one_after_the_target` refers to the first lower-bound greater than `key`
        //     ==> it corresponds to the left-most DPU whose KV pairs are all to the right of `key`
        //     ==> `one_after_the_target - 1` corresponds to the DPU whose range includes `key`
        const auto one_after_the_target = std::upper_bound(lower_bounds.cbegin() + 1, lower_bounds.cbegin() + nr_dpus, key);
        return static_cast<dpu_id_t>(one_after_the_target - (lower_bounds.cbegin() + 1));
    }
    dpu_id_t dpu_responsible_for_insert_query_with(key_int64_t key) const
    {
        return dpu_responsible_for_get_query_with(key);
    }

    dpu_id_t dpu_responsible_for_pred_query_with(key_int64_t key) const
    {
        // `one_after_the_target` refers to the first lower-bound not less than `key`
        //     ==> it corresponds to the left-most DPU where even the first KV pair is not to left of `key`
        //     ==> `one_after_the_target - 1` corresponds to the DPU whose range includes `key`
        const auto one_after_the_target = std::lower_bound(lower_bounds.cbegin() + 1, lower_bounds.cbegin() + nr_dpus, key);
        return static_cast<dpu_id_t>(one_after_the_target - (lower_bounds.cbegin() + 1));
    }

    void print(std::ostream& os, dpu_id_t print_size)
    {
        constexpr auto digits = std::numeric_limits<key_int64_t>::digits10 + 1;
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_dpus && idx_dpu < print_size; idx_dpu++) {
            os << "DPU[" << idx_dpu << "]: "
               << num_kvpairs.at(idx_dpu) << " pairs for ["
               << std::setw(digits) << lower_bounds.at(idx_dpu) << ", "
               << std::setw(digits) << (idx_dpu + 1u == nr_dpus ? KEY_MAX : lower_bounds.at(idx_dpu + 1u)) << ")" << std::endl;
        }
    }
};

/* Data structures in host for managing queries in a batch */
struct BatchCtx {
    std::array<unsigned, MAX_NR_DPUS> num_keys_for_DPU{};
    unsigned send_size{};
};

#endif /* __HOST_DATA_STRUCTURES_HPP__ */