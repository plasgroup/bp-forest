#pragma once

#include "workload_types.h"

#include <vector>


struct PiecewiseConstantWorkloadMetadata {
    // same meaning as the constructor parameters of std::piecewise_constant_distribution
    std::vector<key_uint64_t> intervals;
    std::vector<double> densities;

    template <class Archive>
    void serialize(Archive& ar);
};

struct PiecewiseConstantWorkload {
    PiecewiseConstantWorkloadMetadata metadata;
    std::vector<key_uint64_t> data;

    template <class Archive>
    void serialize(Archive& ar);
};


#include "piecewise_constant_workload.ipp"
