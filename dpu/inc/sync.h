#pragma once

#include <attributes.h>
#include <defs.h>

#include <stdbool.h>
#include <stdint.h>


extern uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];
extern bool fwd_arrived[NR_TASKLETS - 1];
extern bool bwd_arrived[NR_TASKLETS - 1];

#ifndef DPU_ON_CPU
// AtomicBits[0, NR_TASKLETS - 1)

// `resume` does nothing if the waiter has not reached its `stop` yet, so it is
// retried until it takes.
__attribute__((unused)) static void notify_next_of_readiness(void)
{
    unsigned flipped;
    // `resume id, 1` assembles but wakes nobody: the tasklet to wake has to sit
    // in a general purpose register.
    const unsigned target = me() + 1;
    asm volatile("acquire id, %[atomic], nz, .\n"
                 "lbu %[flipped], id, %[arrived]\n"
                 "xor %[flipped], %[flipped], 1, nz, 1f\n"
                 "resume %[target], 0, nz, .\n"
                 "1:\n"
                 "sb id, %[arrived], %[flipped]\n"
                 "release id, %[atomic], nz, .+1"
                 : [flipped] "=&r"(flipped)
                 : [target] "r"(target), [atomic] "i"(&AtomicBits), [arrived] "i"(&fwd_arrived)
                 : "memory");
}
__attribute__((unused)) static void wait_for_prev_ready(void)
{
    unsigned flipped;
    asm volatile("acquire id, %[atomic] - 1, nz, .\n"
                 "lbu %[flipped], id, %[arrived] - 1\n"
                 "xor %[flipped], %[flipped], 1\n"
                 "sb id, %[arrived] - 1, %[flipped]\n"
                 "release id, %[atomic] - 1, nz, .+1"
                 : [flipped] "=&r"(flipped)
                 : [atomic] "i"(&AtomicBits), [arrived] "i"(&fwd_arrived)
                 : "memory");
    if (flipped) {
        asm volatile("stop" ::
                         : "memory");
    }
}

// AtomicBits[NR_TASKLETS - 1, 2 * NR_TASKLETS - 2)
__attribute__((unused)) static void notify_prev_of_readiness(void)
{
    unsigned flipped;
    const unsigned target = me() - 1;
    asm volatile("acquire id, %[atomic] + %[nr_tasklets] - 2, nz, .\n"
                 "lbu %[flipped], id, %[arrived] - 1\n"
                 "xor %[flipped], %[flipped], 1, nz, 1f\n"
                 "resume %[target], 0, nz, .\n"
                 "1:\n"
                 "sb id, %[arrived] - 1, %[flipped]\n"
                 "release id, %[atomic] + %[nr_tasklets] - 2, nz, .+1"
                 : [flipped] "=&r"(flipped)
                 : [target] "r"(target), [atomic] "i"(&AtomicBits), [nr_tasklets] "i"(NR_TASKLETS), [arrived] "i"(&bwd_arrived)
                 : "memory");
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    unsigned flipped;
    asm volatile("acquire id, %[atomic] + %[nr_tasklets] - 1, nz, .\n"
                 "lbu %[flipped], id, %[arrived]\n"
                 "xor %[flipped], %[flipped], 1\n"
                 "sb id, %[arrived], %[flipped]\n"
                 "release id, %[atomic] + %[nr_tasklets] - 1, nz, .+1"
                 : [flipped] "=&r"(flipped)
                 : [atomic] "i"(&AtomicBits), [nr_tasklets] "i"(NR_TASKLETS), [arrived] "i"(&bwd_arrived)
                 : "memory");
    if (flipped) {
        asm volatile("stop" ::
                         : "memory");
    }
}
#endif

#include <mutex.h>

extern const mutex_id_t tmp_sync_mutex;

#ifdef DPU_ON_CPU

extern const mutex_id_t global_lock_mutex;

__attribute__((unused)) static void acquire_lock(void)
{
    mutex_lock(global_lock_mutex);
}
__attribute__((unused)) static void release_lock(void)
{
    mutex_unlock(global_lock_mutex);
}

#else /* DPU_ON_CPU */

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

#endif /* DPU_ON_CPU */

#ifdef DPU_ON_CPU
__attribute__((unused)) static void notify_next_of_readiness(void)
{
    mutex_lock(tmp_sync_mutex);
    fwd_arrived[me()] = true;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void wait_for_prev_ready(void)
{
    mutex_lock(tmp_sync_mutex);
    while (!fwd_arrived[me() - 1]) {
        mutex_unlock(tmp_sync_mutex);
        mutex_lock(tmp_sync_mutex);
    }
    fwd_arrived[me() - 1] = false;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void notify_prev_of_readiness(void)
{
    mutex_lock(tmp_sync_mutex);
    bwd_arrived[me() - 1] = true;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    mutex_lock(tmp_sync_mutex);
    while (!bwd_arrived[me()]) {
        mutex_unlock(tmp_sync_mutex);
        mutex_lock(tmp_sync_mutex);
    }
    bwd_arrived[me()] = false;
    mutex_unlock(tmp_sync_mutex);
}
#endif
