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

#ifndef BULK_MIGRATION
template <class T>
struct MigrationToDPU {
    using InnerArray = T[MAX_NUM_NODES_IN_SEAT];
    InnerArray* array;
    migration_pairs_param_t* nr_migrated_pairs;

    MigrationToDPU(T (*array)[MAX_NUM_NODES_IN_SEAT], migration_pairs_param_t* nr_migrated_pairs)
        : array{array}, nr_migrated_pairs{nr_migrated_pairs} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        const dpu_id_t nr_dpus = upmem_get_nr_dpus();
        if (dpu_index == 0) {
            if (block_index == 0) {
                out->addr = reinterpret_cast<uint8_t*>(&array[1][0]);
                out->length = sizeof(T) * nr_migrated_pairs[0].num_right_kvpairs;
                return true;
            }
        } else if (dpu_index == 1) {
            if (block_index == 0) {
                out->addr = reinterpret_cast<uint8_t*>(&array[0][0]);
                out->length = sizeof(T) * nr_migrated_pairs[1].num_left_kvpairs;
                return true;
            } else if (block_index == 1) {
                out->addr = reinterpret_cast<uint8_t*>(&array[2][0]);
                out->length = sizeof(T) * nr_migrated_pairs[1].num_right_kvpairs;
                return true;
            }
        } else if (dpu_index == nr_dpus - 1) {
            if (block_index == 0) {
                out->addr = reinterpret_cast<uint8_t*>(&array[nr_dpus - 2][nr_migrated_pairs[nr_dpus - 3].num_right_kvpairs]);
                out->length = sizeof(T) * nr_migrated_pairs[nr_dpus - 1].num_left_kvpairs;
                return true;
            }
        } else {
            if (block_index == 0) {
                out->addr = reinterpret_cast<uint8_t*>(&array[dpu_index - 1][nr_migrated_pairs[dpu_index - 2].num_right_kvpairs]);
                out->length = sizeof(T) * nr_migrated_pairs[dpu_index].num_left_kvpairs;
                return true;
            } else if (block_index == 1) {
                out->addr = reinterpret_cast<uint8_t*>(&array[dpu_index + 1][0]);
                out->length = sizeof(T) * nr_migrated_pairs[dpu_index].num_right_kvpairs;
                return true;
            }
        }

        return false;
    }
    constexpr size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(T) * (nr_migrated_pairs[dpu].num_left_kvpairs + nr_migrated_pairs[dpu].num_right_kvpairs);
    }
};
#endif /* ifndef BULK_MIGRATION */
