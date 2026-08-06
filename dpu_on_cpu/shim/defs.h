#pragma once

//! @file
//! dpu_on_cpu shim for the UPMEM SDK <defs.h>.

#include "dpu_cpu.h"

typedef unsigned int sysname_t;

static inline sysname_t me(void)
{
    return dpu_cpu_me;
}
