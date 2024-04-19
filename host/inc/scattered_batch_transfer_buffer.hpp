#pragma once

#include "host_params.hpp"
#include "sg_block_info.hpp"
#include "upmem.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>


template <class T, typename SizeT, size_t NBlocks, size_t BlockSize>
struct ArrayOfScatteredArray {
    using InnerArray = SizedBuffer<T, BlockSize>[NBlocks];
    InnerArray* array;
    SizeT* size_in_elems;

    ArrayOfScatteredArray(SizedBuffer<T, BlockSize> (*array)[NBlocks], SizeT* size_in_elems)
        : array{array}, size_in_elems{size_in_elems} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index < NBlocks) {
            SizedBuffer<T, BlockSize>& sized_buf = array[dpu_index][block_index];
            assert(sized_buf.size_in_elems <= BlockSize);
            out->addr = reinterpret_cast<uint8_t*>(sized_buf.buf.data());
            out->length = sized_buf.size_in_elems * sizeof(T);
            return true;
        } else {
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(T) * size_in_elems[dpu]; }
};


namespace RankWise
{

template <class T, typename SizeT>
struct PartitionedArray {
    T* array;
    SizeT* size_in_elems;
    SizeT* psum_size_in_elems;
    dpu_id_t idx_rank;

    PartitionedArray(dpu_id_t idx_rank, T* array, SizeT* size_in_elems, SizeT* psum_size_in_elems)
        : array{array}, size_in_elems{size_in_elems}, psum_size_in_elems{psum_size_in_elems}, idx_rank{idx_rank} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index < 1) {
            const dpu_id_t idx_dpu_in_rank = dpu_index - upmem_get_dpu_range_in_rank(idx_rank).first;
            if (idx_dpu_in_rank == 0) {
                out->addr = reinterpret_cast<uint8_t*>(&array[0]);
            } else {
                out->addr = reinterpret_cast<uint8_t*>(&array[psum_size_in_elems[idx_dpu_in_rank - 1]]);
            }
            out->length = sizeof(T) * size_in_elems[idx_dpu_in_rank];
            return true;
        } else {
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        const dpu_id_t idx_dpu_in_rank = dpu - upmem_get_dpu_range_in_rank(idx_rank).first;
        return sizeof(T) * size_in_elems[idx_dpu_in_rank];
    }
};

}  // namespace RankWise
