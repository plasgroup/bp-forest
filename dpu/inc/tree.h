#pragma once

#include <barrier.h>


//! @brief A barrier over all the tasklets, defined by dpumain.c, which uses it
//! to hand the task number over.
extern barrier_t tasklet_barrier;

void task_init(void);
void task_get(void);
void task_pred(void);
void task_insert(void);
void task_delete(void);
void task_range_min(void);
void task_range_count(void);
void task_range_max(void);
void task_serialize(void);
void task_move_hot(void);
