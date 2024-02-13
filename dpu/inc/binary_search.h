#pragma once

#include "workload_types.h"


// @return index of the appropriate tree if exists, UINT_MAX otherwise
unsigned tree_responsible_for_get_request_with(key_int64_t key);
