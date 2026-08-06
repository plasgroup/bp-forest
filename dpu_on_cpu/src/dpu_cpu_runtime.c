//! @file
//! Per-instance runtime of the dpu_on_cpu build (see dpu_cpu.h for the
//! interface).  Compiled into the DPU-program shared object, so every loaded
//! copy has its own MRAM heap, log buffer and synchronization objects.
//! ISO C11 only.

#include "barrier.h"
#include "dpu_cpu.h"
#include "mutex.h"

#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>


/* the v1A DPU runs at most 24 tasklets; reject configurations the device
 * could not run */
_Static_assert(NR_TASKLETS <= 24, "NR_TASKLETS must be at most 24");

_Alignas(8) uint8_t dpu_cpu_mram_heap[DPU_CPU_MRAM_HEAP_BYTES];

const uint32_t dpu_cpu_mram_heap_bytes = DPU_CPU_MRAM_HEAP_BYTES;

_Thread_local unsigned dpu_cpu_me;

unsigned dpu_cpu_dpu_id;

const unsigned dpu_cpu_nr_tasklets = NR_TASKLETS;


//
//  lazy initialization of C11 mutexes/condvars, which ISO C cannot
//  statically initialize; the bootstrap mutex serializing it is itself
//  created by call_once on the statically initializable once_flag
//

static once_flag dpu_cpu_bootstrap_once = ONCE_FLAG_INIT;
static mtx_t dpu_cpu_bootstrap_mtx;

static void dpu_cpu_bootstrap(void)
{
    if (mtx_init(&dpu_cpu_bootstrap_mtx, mtx_plain) != thrd_success) {
        fprintf(stderr, "dpu_on_cpu runtime (DPU %u): mtx_init failed\n", dpu_cpu_dpu_id);
        abort();
    }
}

//! Run init() exactly once for the object *inited belongs to.
static void dpu_cpu_ensure_inited(_Atomic bool* inited, void (*init)(void* obj), void* obj)
{
    if (atomic_load_explicit(inited, memory_order_acquire)) {
        return;
    }
    call_once(&dpu_cpu_bootstrap_once, dpu_cpu_bootstrap);
    mtx_lock(&dpu_cpu_bootstrap_mtx);
    if (!atomic_load_explicit(inited, memory_order_relaxed)) {
        init(obj);
        atomic_store_explicit(inited, true, memory_order_release);
    }
    mtx_unlock(&dpu_cpu_bootstrap_mtx);
}

static void dpu_cpu_init_mutex(void* obj)
{
    struct dpu_cpu_mutex* const mutex = obj;
    if (mtx_init(&mutex->mtx, mtx_plain) != thrd_success) {
        fprintf(stderr, "dpu_on_cpu runtime (DPU %u): mtx_init failed\n", dpu_cpu_dpu_id);
        abort();
    }
}

static void dpu_cpu_init_barrier(void* obj)
{
    barrier_t* const barrier = obj;
    if (mtx_init(&barrier->mtx, mtx_plain) != thrd_success || cnd_init(&barrier->cond) != thrd_success) {
        fprintf(stderr, "dpu_on_cpu runtime (DPU %u): barrier init failed\n", dpu_cpu_dpu_id);
        abort();
    }
}


//
//  <mutex.h> shim implementation
//

void mutex_lock(mutex_id_t mutex)
{
    dpu_cpu_ensure_inited(&mutex->inited, dpu_cpu_init_mutex, mutex);
    mtx_lock(&mutex->mtx);
}

void mutex_unlock(mutex_id_t mutex)
{
    mtx_unlock(&mutex->mtx);
}


//
//  <barrier.h> shim implementation
//

void barrier_wait(barrier_t* barrier)
{
    dpu_cpu_ensure_inited(&barrier->inited, dpu_cpu_init_barrier, barrier);

    mtx_lock(&barrier->mtx);
    const unsigned generation = barrier->generation;
    barrier->nr_waiting++;
    if (barrier->nr_waiting == barrier->nr_expected) {
        barrier->nr_waiting = 0;
        barrier->generation++;
        cnd_broadcast(&barrier->cond);
    } else {
        while (barrier->generation == generation) {
            cnd_wait(&barrier->cond, &barrier->mtx);
        }
    }
    mtx_unlock(&barrier->mtx);
}


//
//  per-instance log, fed by the printf of this DPU
//

static _Atomic bool log_mtx_inited;
static mtx_t log_mtx;
static char* log_buf;
static size_t log_len, log_cap;
static bool log_tee; /* whether to copy every append to stderr immediately */

static void dpu_cpu_init_log_mtx(void* obj)
{
    (void)obj;
    if (mtx_init(&log_mtx, mtx_plain) != thrd_success) {
        fprintf(stderr, "dpu_on_cpu runtime (DPU %u): mtx_init failed\n", dpu_cpu_dpu_id);
        abort();
    }
    const char* const tee = getenv("DPU_ON_CPU_TEE_LOG");
    log_tee = tee != NULL && tee[0] != '0';
}

int dpu_cpu_printf(const char* format, ...)
{
    va_list for_sizing, for_writing;
    va_start(for_sizing, format);
    va_copy(for_writing, for_sizing);
    const int nr_chars = vsnprintf(NULL, 0, format, for_sizing);
    va_end(for_sizing);
    if (nr_chars < 0) {
        va_end(for_writing);
        return nr_chars;
    }

    dpu_cpu_ensure_inited(&log_mtx_inited, dpu_cpu_init_log_mtx, NULL);
    mtx_lock(&log_mtx);
    if (log_len + (size_t)nr_chars + 1 > log_cap) {
        size_t new_cap = log_cap != 0 ? log_cap : 4096;
        while (log_len + (size_t)nr_chars + 1 > new_cap) {
            new_cap *= 2;
        }
        char* const new_buf = realloc(log_buf, new_cap);
        if (new_buf == NULL) {
            mtx_unlock(&log_mtx);
            va_end(for_writing);
            fprintf(stderr, "dpu_on_cpu runtime (DPU %u): log buffer allocation failed\n", dpu_cpu_dpu_id);
            abort();
        }
        log_buf = new_buf;
        log_cap = new_cap;
    }
    vsnprintf(log_buf + log_len, (size_t)nr_chars + 1, format, for_writing);
    if (log_tee) {
        fprintf(stderr, "[DPU %u] %s", dpu_cpu_dpu_id, log_buf + log_len);
    }
    log_len += (size_t)nr_chars;
    mtx_unlock(&log_mtx);

    va_end(for_writing);
    return nr_chars;
}

int dpu_cpu_puts(const char* s)
{
    return dpu_cpu_printf("%s\n", s);
}

int dpu_cpu_putchar(int c)
{
    const int result = dpu_cpu_printf("%c", (char)c);
    return result < 0 ? result : c;
}

char* dpu_cpu_read_log(void)
{
    dpu_cpu_ensure_inited(&log_mtx_inited, dpu_cpu_init_log_mtx, NULL);
    mtx_lock(&log_mtx);
    char* copy = NULL;
    if (log_len != 0) {
        copy = malloc(log_len + 1);
        if (copy == NULL) {
            mtx_unlock(&log_mtx);
            fprintf(stderr, "dpu_on_cpu runtime (DPU %u): log copy allocation failed\n", dpu_cpu_dpu_id);
            abort();
        }
        memcpy(copy, log_buf, log_len);
        copy[log_len] = '\0';
    }
    mtx_unlock(&log_mtx);
    return copy;
}

//! Discard the log, as the device does when a new launch boots.
static void dpu_cpu_reset_log(void)
{
    dpu_cpu_ensure_inited(&log_mtx_inited, dpu_cpu_init_log_mtx, NULL);
    mtx_lock(&log_mtx);
    log_len = 0;
    mtx_unlock(&log_mtx);
}


//
//  launching the DPU program
//

//! the DPU program's main(), renamed by compiling with -Dmain=dpu_task_main
int dpu_task_main(void);

static int dpu_cpu_tasklet(void* arg)
{
    dpu_cpu_me = (unsigned)(uintptr_t)arg;
    return dpu_task_main();
}

void dpu_cpu_run(void)
{
    dpu_cpu_reset_log();

    thrd_t tasklets[NR_TASKLETS];
    for (unsigned i = 0; i < NR_TASKLETS; i++) {
        if (thrd_create(&tasklets[i], dpu_cpu_tasklet, (void*)(uintptr_t)i) != thrd_success) {
            fprintf(stderr, "dpu_on_cpu runtime (DPU %u): thrd_create failed\n", dpu_cpu_dpu_id);
            abort();
        }
    }
    for (unsigned i = 0; i < NR_TASKLETS; i++) {
        thrd_join(tasklets[i], NULL);
    }
}
