#pragma once
#include <stdint.h>

/* Structure used by both the host and the dpu to communicate information */
typedef uint64_t key_int64_t;
#define KEY_MIN (0)
#define KEY_MAX (-1ULL)
typedef uint64_t value_ptr_t;

typedef struct KVPair {
    key_int64_t key;
    value_ptr_t value;
} KVPair;
