#pragma once

#include <barrier.h>

#include <stdint.h>


void parallel_sort_requests(uint32_t nelems, barrier_t* barrier);
