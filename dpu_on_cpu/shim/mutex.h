#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <mutex.h>.  The mtx_t is initialized
//! lazily on first lock, as with barrier_t (see dpu_cpu_runtime.c).
//!
//! Stricter than the device: the SDK mutex is an ownerless atomic bit, while
//! with mtx_plain unlocking from another tasklet or relocking is undefined
//! behavior.  mutex_trylock is intentionally not provided (unused).

#include "dpu_cpu.h"

#include <stdbool.h>
#include <threads.h>


struct dpu_cpu_mutex {
    _Atomic bool inited; /* whether mtx is initialized yet */
    mtx_t mtx;
};

//! @brief A mutex object reference, as declared by MUTEX_INIT.
typedef struct dpu_cpu_mutex* mutex_id_t;

//! @brief Return the symbol to use when using the mutex associated to the given name.
#define MUTEX_GET(_name) _name

//! @brief Declare and initialize a mutex associated to the given name.
#define MUTEX_INIT(_name)                              \
    static struct dpu_cpu_mutex dpu_cpu_mutex_##_name; \
    const mutex_id_t MUTEX_GET(_name) = &dpu_cpu_mutex_##_name

void mutex_lock(mutex_id_t mutex);
void mutex_unlock(mutex_id_t mutex);
