#pragma once

#include "host_params.hpp"
#include "upmem.hpp"

#include <array>
#include <cstddef>
#include <vector>


template <typename T>
struct EachInArray {
    static constexpr bool IsSizeVarying = false;

    T* array;

    EachInArray(T* array) : array{array} {}

    T* for_dpu(dpu_id_t dpu) const { return &array[dpu]; }
    constexpr size_t bytes_for_dpu(dpu_id_t) const { return sizeof(T); }
};


template <typename T>
struct Single {
    static constexpr bool IsSizeVarying = false;

    T* datum;
    size_t size_in_bytes;

    Single(T& datum, size_t size_in_elems = 1)
        : datum{&datum}, size_in_bytes{sizeof(T) * size_in_elems} {}

    T* for_dpu(dpu_id_t) const { return datum; }
    constexpr size_t bytes_for_dpu(dpu_id_t) const { return size_in_bytes; }
};
