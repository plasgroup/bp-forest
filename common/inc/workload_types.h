#pragma once

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#include <stdint.h>


/*
 * Database definition
 */
typedef uint64_t key_uint64_t;
#define KEY_MIN (UINT64_C(0))
#define KEY_MAX (UINT64_MAX)

typedef uint64_t value_uint64_t;
#define VALUE_MAX (UINT64_MAX)
#define NOT_FOUND_VALUE (UINT64_C(0))

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

inline key_uint64_t value_int64_to_uint64(int64_t value)
{
    return ((uint64_t)value) ^ (UINT64_C(1) << 63);
}

#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus
