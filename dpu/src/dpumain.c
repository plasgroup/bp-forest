#include "common.h"
#include "input_header.h"
#include "tree.h"

#include <barrier.h>
#include <defs.h>
#include <mram.h>

#include <stdio.h>
#include <stdlib.h>


BARRIER_INIT(my_barrier, NR_TASKLETS);

InputHeader input_header;


#define LAST_TASKLET ((NR_TASKLETS - 1) % NR_TASKLETS)

int main()
{
    if (me() == LAST_TASKLET) {
        mram_read(DPU_MRAM_HEAP_POINTER, &input_header, sizeof(input_header));
#ifdef PRINT_DEBUG
        printf("task_no = %d\n", input_header.task_no);
#endif
    }
    barrier_wait(&my_barrier);

    switch (input_header.task_no) {
    case TASK_INIT:
        task_init();
        break;
#if SUPPORT_GET
    case TASK_GET:
        task_get();
        break;
#endif
#if SUPPORT_INSERT
    case TASK_INSERT:
        task_insert();
        break;
#endif
#if SUPPORT_RANGE_MIN
    case TASK_RANGE_MIN:
        task_range_min();
        break;
#endif
#if SUPPORT_RANGE_COUNT
    case TASK_RANGE_COUNT:
        task_range_count();
        break;
#endif
    case TASK_SUMMARIZE:
        task_summarize();
        break;
    case TASK_EXTRACT:
        task_extract();
        break;
    case TASK_CONSTRUCT_HOT:
        task_construct_hot();
        break;
    case TASK_NONE:
        break;
    default:
        if (me() == 0) {
            printf("no such a task: task %u\n", input_header.task_no);
        }
        abort();
    }
    return 0;
}
