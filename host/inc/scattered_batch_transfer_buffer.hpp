#pragma once

#include "host_params.hpp"
#include "sg_block_info.hpp"

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
