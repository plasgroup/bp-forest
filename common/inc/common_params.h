#pragma once
/* Listing parameters and setting default values */

/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() AND param_dump.c TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() AND param_dump.c TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() AND param_dump.c TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() AND param_dump.c TOGETHER !!!!!!!!!!!!!! */
/* !!!!!!!!!!!!!!! UPDATE BPForest::print_params() AND param_dump.c TOGETHER !!!!!!!!!!!!!! */


#ifndef NR_RANKS
#error NR_RANKS should always be defined
#endif

// #define UPMEM_SIMULATOR


#ifndef MAX_NR_SUMMARY_CHUNKS
#define MAX_NR_SUMMARY_CHUNKS 635
#endif


#ifndef MAX_NR_RMQ_LUMPS
#define MAX_NR_RMQ_LUMPS 20000
#endif
