#include "sort.h"

#include "bit_ops.h"
#include "common.h"

#include <barrier.h>
#include <defs.h>

#include <stdbool.h>
#include <stdint.h>
#include <string.h>


extern __mram_ptr each_request_t request_buffer[MAX_REQ_NUM_IN_A_DPU];


void parallel_sort_requests(uint32_t nelems, barrier_t* barrier)
{
    const unsigned tid = (unsigned)me();

    //----- extend to power-of-2 array -----//
    const uint32_t ext_nelems = bit_ceil_uint32(nelems);
    if (nelems != ext_nelems) {
        if (tid == 0u) {
            memset(&request_buffer[nelems], -1, sizeof(each_request_t) * (ext_nelems - nelems));
        }
        barrier_wait(barrier);
        nelems = ext_nelems;
    }

    //----- bitonic sort -----//
    const uint32_t half_nelems = nelems >> 1u;
    const unsigned lg2_half_nelems = floor_log2_uint32(nelems) - 1u;
    const uint32_t idx_swap_begin = ((uint32_t)tid << lg2_half_nelems) / NR_TASKLETS,
                   idx_swap_end = ((uint32_t)(tid + 1) << lg2_half_nelems) / NR_TASKLETS;

    for (uint32_t sorted_chunk_size = 1; sorted_chunk_size < nelems; sorted_chunk_size <<= 1u) {
        const uint32_t descending_bit = sorted_chunk_size;

        for (uint32_t swap_dist = sorted_chunk_size; swap_dist > 0u; swap_dist >>= 1u) {
            for (uint32_t idx = idx_swap_begin; idx < idx_swap_end; idx++) {
                const bool up = (idx & descending_bit) == 0;

                const uint32_t lower_mask = swap_dist - 1u;
                const uint32_t idx_lhs = (idx & lower_mask) | ((idx & ~lower_mask) << 1u),
                               idx_rhs = idx_lhs | swap_dist;
                if ((request_buffer[idx_lhs].key > request_buffer[idx_rhs].key) == up) {
                    each_request_t t = request_buffer[idx_lhs];
                    request_buffer[idx_lhs] = request_buffer[idx_rhs];
                    request_buffer[idx_rhs] = t;
                }
            }

            barrier_wait(barrier);
        }
    }
}
