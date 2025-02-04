#include <cstdio>
#include <cstdlib>
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#include <sys/time.h>
#include <memory>

#define MAX_DPUS 3000
#define TRANSFER_BUFFER_MAX_SIZE (1024*1024)
#define DPU_BINARY "build/release/util/bandwidth-dpu"

char transfer_buffer[MAX_DPUS][TRANSFER_BUFFER_MAX_SIZE];

/*
WRAM: 0x00000000 - 0x00010000
MRAM: 0x08000000 - 0x0c000000
IRAM: 0x80000000 - 0x80008000
*/
struct dpu_symbol_t dest_buffer = {0x08000000, TRANSFER_BUFFER_MAX_SIZE};

struct timeval start, end;
float time_diff(struct timeval* start, struct timeval* end)
{
    float timediff = (end->tv_sec - start->tv_sec) + 1e-6 * (end->tv_usec - start->tv_usec);
    return timediff;
}

/* dir: DPU_XFER_TO_DPU or DPU_XFER_FROM_DPU */
void do_xfer(dpu_set_t* setp, int transfer_unit, dpu_xfer_t dir)
{
    struct dpu_set_t dpu;
    int each_dpu;

    for (size_t offset = 0; offset < TRANSFER_BUFFER_MAX_SIZE; offset += transfer_unit) {
        DPU_FOREACH(*setp, dpu, each_dpu)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &transfer_buffer[each_dpu][offset]));
        }
        DPU_ASSERT(dpu_push_xfer_symbol(*setp, dir, dest_buffer, 0, transfer_unit, DPU_XFER_DEFAULT));
    }
}

uint32_t test_nr_dpus[] = {1, 4, 16, 64, 256, 1024, 2048, DPU_ALLOCATE_ALL};
int test_transfer_unit[] = {8, 64, 512, 4096, 32768, 262144, 524288, 1048576};

int main(int argc, char* argv[])
{
    struct dpu_set_t set;
    struct timeval start, end;

    for (int i = 0; i < sizeof(test_nr_dpus) / sizeof(int); i++) {
        uint32_t nr_dpus = test_nr_dpus[i];
        DPU_ASSERT(dpu_alloc(nr_dpus, NULL, &set));
        DPU_ASSERT(dpu_get_nr_dpus(set, &nr_dpus));


        for (int round = 0; round < 10; round++) {
            gettimeofday(&start, NULL);
            DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));
            gettimeofday(&end, NULL);
            printf("round %d, load, nr_dpus=%d, time=%f\n", round, nr_dpus, time_diff(&start, &end));

            for (int j = 0; j < sizeof(test_transfer_unit) / sizeof(int); j++) {
                int transfer_unit = test_transfer_unit[j];
                gettimeofday(&start, NULL);
                do_xfer(&set, transfer_unit, DPU_XFER_TO_DPU);
                gettimeofday(&end, NULL);
                printf("round %d, is_to_dpu=%d nr_dpus=%d, size_per_dpu=%d, transfer_unit=%d, time=%f\n", round, 0, nr_dpus, TRANSFER_BUFFER_MAX_SIZE, transfer_unit, time_diff(&start, &end));

                gettimeofday(&start, NULL);
                do_xfer(&set, transfer_unit, DPU_XFER_FROM_DPU);
                gettimeofday(&end, NULL);
                printf("round %d, is_to_dpu=%d nr_dpus=%d, size_per_dpu=%d,  transfer_unit=%d, time=%f\n", round, 1, nr_dpus, TRANSFER_BUFFER_MAX_SIZE, transfer_unit, time_diff(&start, &end));
            }
            gettimeofday(&start, NULL);
            DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
            gettimeofday(&end, NULL);
            printf("round %d, launch, nr_dpus=%d, time=%f\n", round, nr_dpus, time_diff(&start, &end));
        }



        DPU_ASSERT(dpu_free(set));
    }

    return 0;
}