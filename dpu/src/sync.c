#include <attributes.h>

#include <stdint.h>


uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];


#include <mutex.h>

MUTEX_INIT(tmp_sync_mutex);
bool fwd_readiness[NR_TASKLETS] = {};
bool bwd_readiness[NR_TASKLETS] = {};
