#pragma once

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#include <stdint.h>


/*
 * Database definition
 */
typedef uint64_t key_int64_t;
#define KEY_MIN (UINT64_C(0))
#define KEY_MAX (UINT64_MAX)

typedef uint64_t value_ptr_t;


#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus
