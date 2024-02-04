#pragma once

#include "piecewise_constant_workload.hpp"

#include <cereal/types/vector.hpp>

#include <vector>


template <class Archive>
void PiecewiseConstantWorkloadMetadata::serialize(Archive &ar) {
    ar(intervals, densities);
};

template <class Archive>
void PiecewiseConstantWorkload::serialize(Archive &ar) {
    ar(metadata, data);
};
