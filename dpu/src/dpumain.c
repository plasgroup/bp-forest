#include "input_header.h"
#include "common.h"
#include "tree.h"

#include <barrier.h>
#include <defs.h>
#include <mram.h>

#include <stdio.h>


BARRIER_INIT(my_barrier, NR_TASKLETS);

InputHeader input_header;


#define LAST_TASKLET ((NR_TASKLETS - 1) % NR_TASKLETS)

int main()
{
    if (me() == LAST_TASKLET) {
        mram_read(DPU_MRAM_HEAP_POINTER, &input_header, sizeof(input_header));
    }
    barrier_wait(&my_barrier);

    switch (input_header.task_no) {
    case TASK_INIT:
        task_init();
    case TASK_NONE:
        break;
    default:
        if (me() == 0) {
            printf("no such a task: task %d\n", input_header.task_no);
        }
        return -1;
    }
    return 0;
}
