#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <barrier.h>.
//!
//! ISO C has no static initializer for mtx_t/cnd_t, so BARRIER_INIT only
//! zero-initializes and the runtime initializes them lazily on first wait
//! (see dpu_cpu_runtime.c).

#include "dpu_cpu.h"

#include <stdbool.h>
#include <threads.h>


typedef struct barrier_t {
    _Atomic bool inited; /* whether mtx and cond are initialized yet */
    mtx_t mtx;
    cnd_t cond;
    unsigned nr_expected; /* number of tasklets that must reach the barrier */
    unsigned nr_waiting;
    unsigned generation; /* incremented each time the barrier opens */
} barrier_t;

//! @brief Define and initialize a barrier released when _counter tasklets reach it.
//! The counter limit comes from the SDK, which encodes it in a byte.
#define BARRIER_INIT(_name, _counter)                                                          \
    _Static_assert((_counter) < 128 && (_counter) >= -127, "barrier counter must fit a byte"); \
    barrier_t _name = {.nr_expected = (_counter)}

//! @brief Block until nr_expected tasklets (including this one) have arrived.
void barrier_wait(barrier_t* barrier);
