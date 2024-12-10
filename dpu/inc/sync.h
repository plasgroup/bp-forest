#pragma once

#include <attributes.h>

#include <stdint.h>


extern uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];

__attribute__((unused)) static void initialize_readiness_bits(void)
{
    for (unsigned i = 0; i < NR_TASKLETS * 2 - 1; i++) {
        __asm__("acquire %[index], %[base], t, .+1" ::[index] "r"(i),
                [base] "i"(&AtomicBits)
                :);
    }
}

// AtomicBits[0, NR_TASKLETS - 1)
__attribute__((unused)) static void notify_next_of_readiness(void)
{
    // __asm__("acquire id, %[base], t, .+1\n"
    __asm__("release id, %[base], nz, .+1\n"
            // "resume id, 1" ::[base] "i"(&AtomicBits)
            "" ::[base] "i"(&AtomicBits)
            :);
}
__attribute__((unused)) static void wait_for_prev_ready(void)
{
    __asm__("0:\n"
            // "release id, %[base] - 1, nz, .+2\n"
            "acquire id, %[base] - 1, nz, .\n"
            // "stop true, 0b" ::[base] "i"(&AtomicBits)
            // "jump 0b" ::[base] "i"(&AtomicBits)
            "" ::[base] "i"(&AtomicBits)
            :);
}

// AtomicBits[NR_TASKLETS - 1, 2 * NR_TASKLETS - 2)
__attribute__((unused)) static void notify_prev_of_readiness(void)
{
    // __asm__("acquire id, %[base] + %[nr_tasklets] - 2, true, .+1\n"
    __asm__("release id, %[base] + %[nr_tasklets] - 2, nz, .+1\n"
            // "resume id, -1" ::[base] "i"(&AtomicBits),
            "" ::[base] "i"(&AtomicBits),
            [nr_tasklets] "i"(NR_TASKLETS)
            :);
}
__attribute__((unused)) static void wait_for_next_ready(void)
{
    __asm__("0:\n"
            // "release id, %[base] + %[nr_tasklets] - 1, nz, .+2\n"
            "acquire id, %[base] + %[nr_tasklets] - 1, nz, .\n"
            // "stop true, 0b" ::[base] "i"(&AtomicBits),
            // "jump 0b" ::[base] "i"(&AtomicBits),
            "" ::[base] "i"(&AtomicBits),
            [nr_tasklets] "i"(NR_TASKLETS)
            :);
}

// AtomicBits[2 * NR_TASKLETS - 2]
__attribute__((unused)) static void acquire_lock(void)
{
    __asm__("acquire zero, %[base] + %[nr_tasklets] + %[nr_tasklets] - 2, nz, .\n" ::[base] "i"(&AtomicBits),
            [nr_tasklets] "i"(NR_TASKLETS)
            :);
}
__attribute__((unused)) static void release_lock(void)
{
    __asm__("release zero, %[base] + %[nr_tasklets] + %[nr_tasklets] - 2, nz, .+1\n" ::[base] "i"(&AtomicBits),
            [nr_tasklets] "i"(NR_TASKLETS)
            :);
}
