#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <mutex_pool.h>.  A pool of NB_MUTEXES
//! mutexes indexed by the low bits of the id, as on the device; each entry is
//! a dpu_cpu_mutex (see the <mutex.h> shim).

#include "mutex.h"

#include <stdint.h>


struct mutex_pool {
    struct dpu_cpu_mutex* mutexes;
    uint16_t mask;
};

//! @brief Declare and initialize a pool of NB_MUTEXES (a power of 2) mutexes.
#define MUTEX_POOL_INIT(NAME, NB_MUTEXES)                              \
    static struct dpu_cpu_mutex dpu_cpu_mutex_pool_##NAME[NB_MUTEXES]; \
    static struct mutex_pool NAME = {dpu_cpu_mutex_pool_##NAME, NB_MUTEXES - 1}

static inline void mutex_pool_lock(struct mutex_pool* pool, uint16_t id)
{
    mutex_lock(&pool->mutexes[id & pool->mask]);
}

static inline void mutex_pool_unlock(struct mutex_pool* pool, uint16_t id)
{
    mutex_unlock(&pool->mutexes[id & pool->mask]);
}
