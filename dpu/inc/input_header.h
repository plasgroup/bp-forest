#include <attributes.h>
#include <stdint.h>

typedef struct __dma_aligned InputHeader {
    uint32_t task_no;
    union {
        uint8_t size[4];
        struct {
            uint32_t nr_pairs;
        } init, construct_hot;
        struct {
            uint16_t nr_cold_qrys, nr_hot_qrys;
        } get, rmq;
        struct {
            uint32_t nr_ranges;
        } extract;
        struct {
            uint32_t nr_ranges;
        } restore;
    };
} InputHeader;
extern InputHeader input_header;
_Static_assert(sizeof(input_header) == 8, "sizeof(input_header) == 8");
