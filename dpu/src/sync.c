#include <attributes.h>

#include <stdbool.h>
#include <stdint.h>


uint8_t __atomic_bit AtomicBits[NR_TASKLETS * 2];
bool fwd_readiness[NR_TASKLETS - 1] = {};
bool bwd_readiness[NR_TASKLETS - 1] = {};


#include <mutex.h>

MUTEX_INIT(tmp_sync_mutex);

#ifdef DPU_ON_CPU
MUTEX_INIT(global_lock_mutex);
#endif
