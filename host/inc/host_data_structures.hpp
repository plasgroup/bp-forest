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


class Migration;

typedef struct seat_addr_t {
    dpu_id_t dpu;
    seat_id_t seat;
    seat_addr_t() : dpu(INVALID_DPU_ID), seat(INVALID_SEAT_ID) {}
    seat_addr_t(dpu_id_t d, seat_id_t s) : dpu(d), seat(s) {}
    bool operator==(const struct seat_addr_t& that) const
    {
        return dpu == that.dpu && seat == that.seat;
    }
} seat_addr_t;

/* Data structures in host for managing subtrees in DPUs */
class HostTree
{
private:
    std::array<key_int64_t, NR_DPUS> lower_bounds;
    std::array<uint32_t, NR_DPUS> num_kvpairs;

    HostTree() {}
    friend HostTree initialize_bpforest(PiecewiseConstantWorkloadMetadata& workload_dist);
    friend void upmem_receive_num_kvpairs(HostTree* host_tree, float* receive_time);

public:
    key_int64_t get_lower_bound(dpu_id_t idx_dpu)
    {
        assert(0 <= idx_dpu && idx_dpu < lower_bounds.size());
        return lower_bounds[idx_dpu];
    }
    uint32_t get_num_kvpairs(dpu_id_t idx_dpu)
    {
        assert(0 <= idx_dpu && idx_dpu < lower_bounds.size());
        return num_kvpairs[idx_dpu];
    }

    dpu_id_t dpu_responsible_for_get_query_with(key_int64_t key)
    {
        // `one_after_the_target` refers to the first lower-bound greater than `key`
        //     ==> it corresponds to the left-most DPU whose KV pairs are all to the right of `key`
        //     ==> `one_after_the_target - 1` corresponds to the DPU whose range includes `key`
        const auto one_after_the_target = std::upper_bound(lower_bounds.cbegin() + 1, lower_bounds.cend(), key);
        return static_cast<dpu_id_t>(one_after_the_target - (lower_bounds.cbegin() + 1));
    }
    dpu_id_t dpu_responsible_for_insert_query_with(key_int64_t key)
    {
        return dpu_responsible_for_get_query_with(key);
    }

    dpu_id_t dpu_responsible_for_pred_query_with(key_int64_t key)
    {
        // `one_after_the_target` refers to the first lower-bound not less than `key`
        //     ==> it corresponds to the left-most DPU where even the first KV pair is not to left of `key`
        //     ==> `one_after_the_target - 1` corresponds to the DPU whose range includes `key`
        const auto one_after_the_target = std::lower_bound(lower_bounds.cbegin() + 1, lower_bounds.cend(), key);
        return static_cast<dpu_id_t>(one_after_the_target - (lower_bounds.cbegin() + 1));
    }

    friend std::ostream& operator<<(std::ostream& os, HostTree& tree)
    {
        constexpr auto digits = std::numeric_limits<key_int64_t>::digits10 + 1;
        for (dpu_id_t idx_dpu = 0; idx_dpu < NR_DPUS; idx_dpu++) {
            std::cout << "DPU[" << idx_dpu << "]: "
                      << tree.num_kvpairs.at(idx_dpu) << " pairs for ["
                      << std::setw(digits) << tree.lower_bounds.at(idx_dpu) << ", "
                      << std::setw(digits) << (idx_dpu + 1u == NR_DPUS ? KEY_MAX : tree.lower_bounds.at(idx_dpu + 1u)) << ")" << std::endl;
        }

        return os;
    }
};

/* Data structures in host for managing queries in a batch */
class BatchCtx
{
public:
    int DPU_idx[NR_DPUS]{};
    std::array<unsigned, NR_DPUS> num_keys_for_DPU{};
    unsigned send_size{};
    BatchCtx()
    {
        for (int i = 0; i < NR_DPUS; i++) {
            DPU_idx[i] = i;
        }
    }

private:
};

#endif /* __HOST_DATA_STRUCTURES_HPP__ */