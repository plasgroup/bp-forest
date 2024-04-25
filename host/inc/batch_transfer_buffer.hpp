#pragma once

#include "host_params.hpp"
#include "upmem.hpp"

#include <array>
#include <cstddef>
#include <vector>


template <typename T, size_t N>
struct SameSizeIn2DArray {
    static constexpr bool IsSizeVarying = false;

    using InnerArray = T[N];
    InnerArray* array;
    size_t size_in_bytes;

    SameSizeIn2DArray(T (*array)[N], size_t size_in_elems)
        : array{array}, size_in_bytes{sizeof(T) * size_in_elems} {}

    T* for_dpu(dpu_id_t dpu) const { return array[dpu]; }
    size_t bytes_for_dpu(dpu_id_t) const { return size_in_bytes; }
};

template <typename T, size_t N, typename SizeT>
struct VariousSizeIn2DArray {
    static constexpr bool IsSizeVarying = true;

    using InnerArray = T[N];
    InnerArray* array;
    SizeT* size_in_elems;

    VariousSizeIn2DArray(T (*array)[N], SizeT* size_in_elems)
        : array{array}, size_in_elems{size_in_elems} {}

    T* for_dpu(dpu_id_t dpu) const { return array[dpu]; }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(T) * size_in_elems[dpu]; }
};

template <typename T, class Alloc, size_t N>
struct ArrayOfVector {
    static constexpr bool IsSizeVarying = true;

    std::array<std::vector<T, Alloc>, N>* array;

    ArrayOfVector(std::array<std::vector<T, Alloc>, N>& array) : array{&array} {}

    T* for_dpu(dpu_id_t dpu) const { return (*array)[dpu].data(); }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(T) * (*array)[dpu].size(); }
};

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


namespace RankWise
{

template <class T, typename SizeT>
struct PartitionedArray {
    static constexpr bool IsSizeVarying = true;

    T* array;
    SizeT* size_in_elems;
    SizeT* psum_size_in_elems;
    dpu_id_t idx_rank;

    PartitionedArray(dpu_id_t idx_rank, T* array, SizeT* size_in_elems, SizeT* psum_size_in_elems)
        : array{array}, size_in_elems{size_in_elems}, psum_size_in_elems{psum_size_in_elems}, idx_rank{idx_rank} {}

    T* for_dpu(dpu_id_t dpu) const
    {
        const dpu_id_t idx_dpu_in_rank = dpu - upmem_get_dpu_range_in_rank(idx_rank).first;
        if (idx_dpu_in_rank == 0) {
            return &array[0];
        } else {
            return &array[psum_size_in_elems[idx_dpu_in_rank - 1]];
        }
    }
    constexpr size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        const dpu_id_t idx_dpu_in_rank = dpu - upmem_get_dpu_range_in_rank(idx_rank).first;
        return sizeof(T) * size_in_elems[idx_dpu_in_rank];
    }
};

template <typename T>
struct EachInArray {
    static constexpr bool IsSizeVarying = false;

    T* array;
    dpu_id_t idx_rank;

    EachInArray(dpu_id_t idx_rank, T* array) : array{array}, idx_rank{idx_rank} {}

    T* for_dpu(dpu_id_t dpu) const { return &array[dpu - upmem_get_dpu_range_in_rank(idx_rank).first]; }
    constexpr size_t bytes_for_dpu(dpu_id_t) const { return sizeof(T); }
};

}  // namespace RankWise
