#pragma once

#include "upmem.hpp"

#include "dpu_set.hpp"
#include "host_params.hpp"

#ifndef HOST_ONLY
#include <array>
#endif

#ifdef PRINT_DEBUG
#include <cstdio>
#endif /* PRINT_DEBUG */


struct UPMEM_AsyncDuration {
    ~UPMEM_AsyncDuration();

#ifndef HOST_ONLY
    bool all{};
    std::array<bool, NR_RANKS> rank{};
#endif
};

inline void upmem_init_impl();
inline void upmem_release_impl();


#ifdef HOST_ONLY
#include "upmem_impl_host_only.ipp"
#else
#include "upmem_impl_dpu.ipp"
#endif


//
//  UPMEM module interface
//
inline void upmem_init()
{
    upmem_init_impl();

#ifdef PRINT_DEBUG
    const dpu_id_t nr_dpus = upmem_get_nr_dpus();
    std::printf("Allocated %d DPU(s)\n", nr_dpus);
#endif /* PRINT_DEBUG */
}

inline void upmem_release()
{
    upmem_release_impl();
}
