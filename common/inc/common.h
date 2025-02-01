#ifndef __COMMON_H__
#define __COMMON_H__

#ifdef __cplusplus
extern "C" {
#endif  // ifdef __cplusplus


#include "workload_types.h"

#include <stdint.h>


#ifdef UPMEM_SIMULATOR
#define MAX_NR_DPUS_IN_RANK 1
#else
#define MAX_NR_DPUS_IN_RANK 64
#endif


/*
 * Shared Data Structures
 */
typedef struct {
    key_uint64_t key;
    value_uint64_t value;
} KVPair;
typedef struct {
    key_uint64_t begin, end;
} KeyRange;
typedef struct {
    uint32_t begin, end;
} IndexRange;
typedef struct {
    KeyRange range;
    value_uint64_t needle;
} RangeCountQuery;
typedef struct {
    KeyRange range;
    char prefix[8];
} RangeCountPrefixQuery;
typedef struct {
    uint16_t nr_keys[4];
    key_uint64_t head_keys[4];
} SummaryBlock;


/* Tasks */
enum TaskID : uint32_t {
    TASK_INIT,
    TASK_GET,
    TASK_PRED,
    TASK_SCAN,
    TASK_INSERT,
    TASK_DELETE,
    TASK_RANGE_MIN,
    TASK_RANGE_SUM,
    TASK_RANGE_COUNT,
    TASK_RANGE_COUNT_PREFIX,
    TASK_SUMMARIZE,
    TASK_EXTRACT,
    TASK_CONSTRUCT_HOT,
    TASK_FLATTEN_HOT,
    TASK_RESTORE,
    TASK_NONE
};


#define PRINT_POSITION_AND_VARIABLE(NAME, FORMAT) \
    printf("[Debug at %s:%d] " #NAME " = " #FORMAT "\n", __FILE__, __LINE__, NAME);
#define PRINT_POSITION_AND_MESSAGE(MESSAGE) \
    printf("[Debug at %s:%d] " #MESSAGE "\n", __FILE__, __LINE__);


#ifdef __cplusplus
}  // extern "C"
#endif  // ifdef __cplusplus

#endif /* __COMMON_H__ */
