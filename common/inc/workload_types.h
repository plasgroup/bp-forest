#pragma once

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#include "bit_ops_macro.h"

#include <stdint.h>


/*
 * Database definition
 */
typedef uint64_t key_uint64_t;
#define KEY_MIN (UINT64_C(0))
#define KEY_MAX (UINT64_MAX)
#define KEY_WIDTH (BITWIDTH_UINT64(KEY_MAX))

// Values a user may store: the 63-bit signed range.  Everything below it is
// reserved for the tombstone and for TASK_DELETE's claims (docs/parallel_delete.md).
typedef int64_t value_int64_t;
#define VALUE_MIN (-(INT64_C(1) << 62))
#define VALUE_MAX ((INT64_C(1) << 62) - 1)
#define NOT_FOUND_VALUE (INT64_MIN)

// shift [-2^63, 2^63-1] to [0, 2^64-1]
inline key_uint64_t key_int64_to_uint64(int64_t key)
{
    return ((uint64_t)key) ^ (UINT64_C(1) << 63);
}
// shift [0, 2^64-1] [-2^63, 2^63-1]
inline int64_t key_uint64_to_int64(key_uint64_t key)
{
    return (int64_t)(key ^ (UINT64_C(1) << 63));
}

#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus
