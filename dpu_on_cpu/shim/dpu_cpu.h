#pragma once

//! @file
//! Interface of the dpu_on_cpu runtime.  Each loaded copy of the DPU-program
//! shared object acts as one DPU, owning private instances of the program's
//! globals; the host reaches the symbols below via dlsym.

#include <stdint.h>

/* the real libc stdio; must be parsed before the printf macro below */
#include <stdio.h>


//! Size of the emulated MRAM heap: the whole MRAM of a real DPU.  Laxer than
//! the device, where static __mram variables shrink the heap and overruns
//! collide with them.
#define DPU_CPU_MRAM_HEAP_BYTES (UINT32_C(1) << 26)

//! The emulated MRAM heap: the comm buffer host transfers read/write, and
//! what DPU_MRAM_HEAP_POINTER points to.
extern _Alignas(8) uint8_t dpu_cpu_mram_heap[DPU_CPU_MRAM_HEAP_BYTES];

extern const uint32_t dpu_cpu_mram_heap_bytes;

//! ID of the tasklet the calling thread is emulating; me() returns this.
extern _Thread_local unsigned dpu_cpu_me;

//! ID of the DPU this instance emulates; stored by the host after loading.
extern unsigned dpu_cpu_dpu_id;

extern const unsigned dpu_cpu_nr_tasklets;

//! @brief Perform one DPU launch: run all tasklets and return when they finish.
//!
//! As on real hardware, every tasklet enters main(), globals persist across
//! launches, and the log is reset on entry.
void dpu_cpu_run(void);

//! @brief printf of this DPU: appends to the per-instance log buffer.
//!
//! One call appends one contiguous string (tasklet-atomic, like the DPU-side
//! printf); nothing reaches stdout.  With the environment variable
//! DPU_ON_CPU_TEE_LOG set (non-0), each call is also copied to stderr
//! immediately, prefixed with "[DPU N] " — for crashes and hangs that never
//! reach read_log().
__attribute__((format(printf, 1, 2))) int dpu_cpu_printf(const char* format, ...);

int dpu_cpu_puts(const char* s);
int dpu_cpu_putchar(int c);

//! @brief Copy of the log accumulated since the current launch started.
//!
//! Non-destructive like the SDK's dpu_log_read; discarded at the next launch.
//! @return a malloc'd string the caller must free(), or NULL if empty
char* dpu_cpu_read_log(void);


//! Route the DPU program's stdio calls into the log buffer.  Object-like
//! macros, defined after the real declarations so later #include <stdio.h>
//! are guard-skipped.  abort()/assert() are NOT rerouted (they kill the whole
//! process, unlike the per-DPU fault on the device); the host's SIGABRT
//! handler dumps the pending logs instead.
#define printf dpu_cpu_printf
#define puts dpu_cpu_puts
#define putchar dpu_cpu_putchar
