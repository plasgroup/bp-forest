#include "dpu_params.h"
#include "tree_impl.h"

#include <stdint.h>


// clang-format off
#define STRINGIFY(x) #x
#define EXPAND_STRINGIFY(x) STRINGIFY(x)
__mram_keep const char ParamDump[] = "NR_RANKS: " EXPAND_STRINGIFY(NR_RANKS) "\n"
#ifdef UPMEM_SIMULATOR
                                     "UPMEM_SIMULATOR: 1\n"
#else
                                     "UPMEM_SIMULATOR: 0\n"
#endif
                                     "MAX_NR_SUMMARY_CHUNKS: " EXPAND_STRINGIFY(MAX_NR_SUMMARY_CHUNKS) "\n"
                                     "MAX_NR_RMQ_LUMPS: " EXPAND_STRINGIFY(MAX_NR_RMQ_LUMPS) "\n"
                                     "NR_TASKLETS: " EXPAND_STRINGIFY(NR_TASKLETS) "\n"
#ifdef PRINT_DEBUG
                                     "PRINT_DEBUG: 1\n"
#else
                                     "PRINT_DEBUG: 0\n"
#endif
#ifdef DEBUG_OCCUPANCY
                                     "DEBUG_OCCUPANCY: 1\n"
#else
                                     "DEBUG_OCCUPANCY: 0\n"
#endif
                                     "MRAM_FOR_TREE: " EXPAND_STRINGIFY(MRAM_FOR_TREE) "\n"
#ifdef BITMAP_IN_MRAM
                                     "BITMAP_IN_MRAM: 1\n"
#else
                                     "BITMAP_IN_MRAM: 0\n"
#endif
                                     "SIZEOF_NODE: " EXPAND_STRINGIFY(SIZEOF_NODE) "\n"
                                     "TREE_CONSTRUCT_NR_TASKLETS: " EXPAND_STRINGIFY(TREE_CONSTRUCT_NR_TASKLETS) "\n"
                                     "TREE_CONSTRUCT_NR_CACHED_KVPAIRS: " EXPAND_STRINGIFY(TREE_CONSTRUCT_NR_CACHED_KVPAIRS) "\n"
                                     "TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT: " EXPAND_STRINGIFY(TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT) "\n"
                                     "TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT: " EXPAND_STRINGIFY(TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT) "\n"
#ifdef TASK_INIT_CHECK
                                     "TASK_INIT_CHECK: 1\n"
#else
                                     "TASK_INIT_CHECK: 0\n"
#endif
                                     "TASK_SUMMARIZE_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_SUMMARIZE_NR_TASKLETS) "\n"
#ifdef TASK_EXTRACT_CHECK
                                     "TASK_EXTRACT_CHECK: 1\n"
#else
                                     "TASK_EXTRACT_CHECK: 0\n"
#endif
                                     "TASK_SERIALIZE_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_SERIALIZE_NR_TASKLETS) "\n"
                                     "TASK_SERIALIZE_NR_CACHED_KVPAIRS: " EXPAND_STRINGIFY(TASK_SERIALIZE_NR_CACHED_KVPAIRS) "\n"
                                     "TASK_SERIALIZE_NR_CACHED_DELIMS: " EXPAND_STRINGIFY(TASK_SERIALIZE_NR_CACHED_DELIMS) "\n"
                                     "TREE_CLEAR_NR_TASKLETS: " EXPAND_STRINGIFY(TREE_CLEAR_NR_TASKLETS) "\n"
#ifdef TASK_MOVE_HOT_CHECK
                                     "TASK_MOVE_HOT_CHECK: 1\n"
#else
                                     "TASK_MOVE_HOT_CHECK: 0\n"
#endif
                                     "TASK_GET_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_GET_NR_TASKLETS) "\n"
                                     "TASK_GET_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_GET_NR_CACHED_QRYS) "\n"
                                     "TASK_PRED_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_PRED_NR_TASKLETS) "\n"
                                     "TASK_PRED_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_PRED_NR_CACHED_QRYS) "\n"
                                     "TASK_DELETE_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_DELETE_NR_TASKLETS) "\n"
                                     "TASK_DELETE_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_DELETE_NR_CACHED_QRYS) "\n"
                                     "TASK_INSERT_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_INSERT_NR_TASKLETS) "\n"
                                     "TASK_INSERT_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_INSERT_NR_CACHED_QRYS) "\n"
#ifdef TASK_INSERT_CHECK
                                     "TASK_INSERT_CHECK: 1\n"
#else
                                     "TASK_INSERT_CHECK: 0\n"
#endif
#ifdef TASK_INSERT_SORT_CHECK
                                     "TASK_INSERT_SORT_CHECK: 1\n"
#else
                                     "TASK_INSERT_SORT_CHECK: 0\n"
#endif
                                     "TASK_INSERT_SORT_RADIX_BITS: " EXPAND_STRINGIFY(TASK_INSERT_SORT_RADIX_BITS) "\n"
                                     "TASK_INSERT_SORT_RUN: " EXPAND_STRINGIFY(TASK_INSERT_SORT_RUN) "\n"
                                     "TASK_INSERT_SORT_DIGIT_BUF: " EXPAND_STRINGIFY(TASK_INSERT_SORT_DIGIT_BUF) "\n"
                                     "TASK_RANGE_MIN_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_RANGE_MIN_NR_TASKLETS) "\n"
                                     "TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES: " EXPAND_STRINGIFY(TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES) "\n"
                                     "TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS: " EXPAND_STRINGIFY(TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS) "\n"
                                     "TASK_RANGE_MIN_NR_CACHED_RESULTS: " EXPAND_STRINGIFY(TASK_RANGE_MIN_NR_CACHED_RESULTS) "\n"
                                     "TASK_RANGE_COUNT_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_RANGE_COUNT_NR_TASKLETS) "\n"
                                     "TASK_RANGE_COUNT_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_RANGE_COUNT_NR_CACHED_QRYS) "\n"
                                     "TASK_RANGE_COUNT_NR_CACHED_RESULTS: " EXPAND_STRINGIFY(TASK_RANGE_COUNT_NR_CACHED_RESULTS) "\n"
                                     "TASK_RANGE_MAX_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_RANGE_MAX_NR_TASKLETS) "\n"
                                     "TASK_RANGE_MAX_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_RANGE_MAX_NR_CACHED_QRYS) "\n"
                                     "TASK_RANGE_MAX_NR_CACHED_RESULTS: " EXPAND_STRINGIFY(TASK_RANGE_MAX_NR_CACHED_RESULTS) "\n"
                                     "TASK_RANGE_COUNT_PREFIX_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_RANGE_COUNT_PREFIX_NR_TASKLETS) "\n"
                                     "TASK_RANGE_COUNT_PREFIX_NR_CACHED_QRYS: " EXPAND_STRINGIFY(TASK_RANGE_COUNT_PREFIX_NR_CACHED_QRYS) "\n"
                                     "TASK_RANGE_COUNT_PREFIX_NR_CACHED_RESULTS: " EXPAND_STRINGIFY(TASK_RANGE_COUNT_PREFIX_NR_CACHED_RESULTS) "\n"
                                     "TASK_INIT_BITMAP_NR_TASKLETS: " EXPAND_STRINGIFY(TASK_INIT_BITMAP_NR_TASKLETS) "\n"
                                     "TASK_INIT_NR_CACHED_WORDS: " EXPAND_STRINGIFY(TASK_INIT_NR_CACHED_WORDS) "\n"
#undef STRINGIFY
#undef EXPAND_STRINGIFY
                                     "\0\0\0\0\0\0\0\0";
// clang-format on

__mram_keep const uint64_t ParamDumpSize = sizeof(ParamDump) / 8 * 8;
