#pragma once


#ifdef UPMEM

#define STATIC_ASSERT(...) _Static_assert(__VA_ARGS__)
#define ALIGNOF(...) _Alignof(__VA_ARGS__)
#define ALIGNAS(...) _Alignas(__VA_ARGS__)

#define CONSTEXPR const

#include <attributes.h>


#else /* ifdef UPMEM */

#define STATIC_ASSERT(...) static_assert(__VA_ARGS__)
#define ALIGNOF(...) alignof(__VA_ARGS__)
#define ALIGNAS(...) alignas(__VA_ARGS__)

#define CONSTEXPR constexpr

#define __dma_aligned alignas(8)


#endif /* ifdef UPMEM */
