#pragma once

#include <attributes.h>
#include <defs.h>

#include <stdint.h>


extern uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];

#if 0
// AtomicBits[0, NR_TASKLETS - 1)
__attribute__((unused)) static void notify_next_of_readiness(void)
{
    asm volatile("acquire id, %[base], t, .+1\n"
                 "resume id, 1" ::[base] "i"(&AtomicBits)
                 : "memory");
}
__attribute__((unused)) static void wait_for_prev_ready(void)
{
    asm volatile("0:\n"
                 "release id, %[base] - 1, nz, .+2\n"
                 "stop true, 0b" ::[base] "i"(&AtomicBits)
                 : "memory");
}

// AtomicBits[NR_TASKLETS - 1, 2 * NR_TASKLETS - 2)
__attribute__((unused)) static void notify_prev_of_readiness(void)
{
    const unsigned prev_id = me() - 1;
    asm volatile("acquire id, %[base] + %[nr_tasklets] - 2, true, .+1\n"
                 "resume %[prev_id], 0" ::[base] "i"(&AtomicBits),
                 [prev_id] "r"(prev_id),
                 [nr_tasklets] "i"(NR_TASKLETS)
                 : "memory");
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    asm volatile("0:\n"
                 "release id, %[base] + %[nr_tasklets] - 1, nz, .+2\n"
                 "stop true, 0b" ::[base] "i"(&AtomicBits),
                 [nr_tasklets] "i"(NR_TASKLETS)
                 : "memory");
}
#endif

// AtomicBits[2 * NR_TASKLETS - 2]
__attribute__((unused)) static void acquire_lock(void)
{
    asm volatile("acquire zero, %[base] + %[nr_tasklets] + %[nr_tasklets] - 2, nz, .\n" ::[base] "i"(&AtomicBits),
                 [nr_tasklets] "i"(NR_TASKLETS)
                 : "memory");
}
__attribute__((unused)) static void release_lock(void)
{
    asm volatile("release zero, %[base] + %[nr_tasklets] + %[nr_tasklets] - 2, nz, .+1\n" ::[base] "i"(&AtomicBits),
                 [nr_tasklets] "i"(NR_TASKLETS)
                 : "memory");
}


#include <mutex.h>

extern const mutex_id_t tmp_sync_mutex;
extern bool fwd_readiness[NR_TASKLETS];
extern bool bwd_readiness[NR_TASKLETS];

__attribute__((unused)) static void notify_next_of_readiness(void)
{
    mutex_lock(tmp_sync_mutex);
    fwd_readiness[me()] = true;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void wait_for_prev_ready(void)
{
    mutex_lock(tmp_sync_mutex);
    while (!fwd_readiness[me() - 1]) {
        mutex_unlock(tmp_sync_mutex);
        mutex_lock(tmp_sync_mutex);
    }
    fwd_readiness[me() - 1] = false;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void notify_prev_of_readiness(void)
{
    mutex_lock(tmp_sync_mutex);
    bwd_readiness[me()] = true;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    mutex_lock(tmp_sync_mutex);
    while (!bwd_readiness[me() + 1]) {
        mutex_unlock(tmp_sync_mutex);
        mutex_lock(tmp_sync_mutex);
    }
    bwd_readiness[me() + 1] = false;
    mutex_unlock(tmp_sync_mutex);
}
