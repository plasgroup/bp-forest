#pragma once

#include <attributes.h>
#include <defs.h>

#include <stdbool.h>
#include <stdint.h>


extern uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];
extern bool fwd_readiness[NR_TASKLETS - 1];
extern bool bwd_readiness[NR_TASKLETS - 1];

// AtomicBits[0, NR_TASKLETS - 1)
__attribute__((unused)) static void notify_next_of_readiness(void)
{
    asm volatile("acquire id, %[atomic], nz, .\n"
                 "sb id, %[readiness], 1\n"
                 // "resume id, 1\n"
                 "release id, %[atomic], nz, .+1"
                 :
                 : [atomic] "i"(&AtomicBits), [readiness] "i"(&fwd_readiness)
                 : "memory");
}
__attribute__((unused)) static void wait_for_prev_ready(void)
{
    bool ok;
    asm volatile("0:\n"
                 "acquire id, %[base] - 1, nz, .\n"
                 "lbu %[ok], id, %[readiness] - 1\n"
                 "release id, %[base] - 1, nz, .+1\n"
                 "jz %[ok], 0b\n"
                 // "stop true, 0b\n"
                 "sb id, %[readiness] - 1, 0"
                 : [ok] "=r"(ok)
                 : [base] "i"(&AtomicBits), [readiness] "i"(&fwd_readiness)
                 : "memory");
}

// AtomicBits[NR_TASKLETS - 1, 2 * NR_TASKLETS - 2)
__attribute__((unused)) static void notify_prev_of_readiness(void)
{
    // const unsigned prev_id = me() - 1;
    asm volatile("acquire id, %[atomic] + %[nr_tasklets] - 2, nz, .\n"
                 "sb id, %[readiness] - 1, 1\n"
                 // "resume %[prev_id], 0\n"
                 "release id, %[atomic] + %[nr_tasklets] - 2, nz, .+1"
                 :
                 : [atomic] "i"(&AtomicBits), [nr_tasklets] "i"(NR_TASKLETS), [readiness] "i"(&bwd_readiness)  //, [prev_id] "r"(prev_id)
                 : "memory");
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    bool ok;
    asm volatile("0:\n"
                 "acquire id, %[atomic] + %[nr_tasklets] - 1, nz, .\n"
                 "lbu %[ok], id, %[readiness]\n"
                 "release id, %[atomic] + %[nr_tasklets] - 1, nz, .+1\n"
                 "jz %[ok], 0b\n"
                 // "stop true, 0b\n"
                 "sb id, %[readiness], 0"
                 : [ok] "=r"(ok)
                 : [atomic] "i"(&AtomicBits), [nr_tasklets] "i"(NR_TASKLETS), [readiness] "i"(&bwd_readiness)
                 : "memory");
}

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


#if 0
#include <mutex.h>

extern const mutex_id_t tmp_sync_mutex;

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
    bwd_readiness[me() - 1] = true;
    mutex_unlock(tmp_sync_mutex);
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    mutex_lock(tmp_sync_mutex);
    while (!bwd_readiness[me()]) {
        mutex_unlock(tmp_sync_mutex);
        mutex_lock(tmp_sync_mutex);
    }
    bwd_readiness[me()] = false;
    mutex_unlock(tmp_sync_mutex);
}
#endif
