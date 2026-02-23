#pragma once

#include "compat.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>


typedef struct __dma_aligned InputHeader {
    uint32_t task_no;
    union {
        uint8_t size[12];
        struct {
            uint32_t nr_cold_pairs, nr_hot_pairs;
        } init;
        struct {
            uint32_t nr_pairs;
        } construct_hot;
        struct {
            uint32_t nr_cold_qrys, nr_hot_qrys, result_offset;
        } qrys;
        struct {
            uint16_t nr_cold_lumps, nr_hot_lumps;
        } rmq;
        struct {
            uint32_t nr_ranges;
        } extract, restore;
        struct {
            uint32_t nr_delims, max_nr_delims;
        } serialize;
    };
} InputHeader;
STATIC_ASSERT(sizeof(InputHeader) == 16, "sizeof(InputHeader) == 16");

#ifdef UPMEM
extern InputHeader input_header;
#endif

#ifdef __cplusplus
}  // extern "C"
#endif
