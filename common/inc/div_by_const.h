#pragma once

#include <bit_ops_macro.h>

#include <built_ins.h>

#include <stdbool.h>
#include <stdint.h>


inline __attribute__((always_inline)) void choose_multiplier_for_div_by_const(
    uint32_t divisor, int nbits_dividend, uint64_t* p_m, int* p_sh_post)
{
    int l = CEIL_LOG2_UINT32(divisor), sh_post = l;
    const uint64_t m_low = ((UINT64_C(1) << (nbits_dividend + l)) - 1u) / divisor,
                   m_high = ((UINT64_C(1) << (nbits_dividend + l)) + (UINT64_C(1) << l)) / divisor;
    uint64_t bit_diff = m_high ^ m_low;
    const int prec_margin = FLOOR_LOG2_UINT64(bit_diff);
    const int shift = (sh_post < prec_margin ? sh_post : prec_margin);
    *p_m = m_high >> shift;
    *p_sh_post = nbits_dividend + sh_post - shift;
}

//! @pre constant < (UINT64_C(1) << 33)
//! @pre variable < (UINT64_C(1) << 31)
//! @return (constant * variable) >> sh_post
inline __attribute__((always_inline)) uint32_t lsr_mul_lsr_impl(
    uint64_t constant, uint32_t variable, int sh_post)
{
    const int nr_trailing_0s = FLOOR_LOG2_UINT64(constant & (~constant + 1));
    int last_shift = nr_trailing_0s;
    uint32_t result = variable;

#define lsr_mul_lsr_impl_step(shift)                                           \
    if ((constant & (UINT64_C(1) << shift)) != 0 && nr_trailing_0s != shift) { \
        if (shift <= sh_post) {                                                \
            result = (result >> (shift - last_shift)) + variable;              \
            last_shift = shift;                                                \
        } else {                                                               \
            if (last_shift < sh_post) {                                        \
                result = result >> (sh_post - last_shift);                     \
                last_shift = sh_post;                                          \
            }                                                                  \
            result = result + (variable << (shift - sh_post));                 \
        }                                                                      \
    }

    lsr_mul_lsr_impl_step(1);
    lsr_mul_lsr_impl_step(2);
    lsr_mul_lsr_impl_step(3);
    lsr_mul_lsr_impl_step(4);
    lsr_mul_lsr_impl_step(5);
    lsr_mul_lsr_impl_step(6);
    lsr_mul_lsr_impl_step(7);
    lsr_mul_lsr_impl_step(8);
    lsr_mul_lsr_impl_step(9);
    lsr_mul_lsr_impl_step(10);
    lsr_mul_lsr_impl_step(11);
    lsr_mul_lsr_impl_step(12);
    lsr_mul_lsr_impl_step(13);
    lsr_mul_lsr_impl_step(14);
    lsr_mul_lsr_impl_step(15);
    lsr_mul_lsr_impl_step(16);
    lsr_mul_lsr_impl_step(17);
    lsr_mul_lsr_impl_step(18);
    lsr_mul_lsr_impl_step(19);
    lsr_mul_lsr_impl_step(20);
    lsr_mul_lsr_impl_step(21);
    lsr_mul_lsr_impl_step(22);
    lsr_mul_lsr_impl_step(23);
    lsr_mul_lsr_impl_step(24);
    lsr_mul_lsr_impl_step(25);
    lsr_mul_lsr_impl_step(26);
    lsr_mul_lsr_impl_step(27);
    lsr_mul_lsr_impl_step(28);
    lsr_mul_lsr_impl_step(29);
    lsr_mul_lsr_impl_step(30);
    lsr_mul_lsr_impl_step(31);
    lsr_mul_lsr_impl_step(32);
#undef lsr_mul_lsr_impl_step

    return result >> (sh_post - last_shift);
}

inline __attribute__((always_inline)) uint32_t lsr_imm(uint32_t variable, int constant_shift)
{
    // clang-format off
    switch (constant_shift) {
    case 0: break;
    case 1: __builtin_lsr_rri(variable, variable, "1"); break;
    case 2: __builtin_lsr_rri(variable, variable, "2"); break;
    case 3: __builtin_lsr_rri(variable, variable, "3"); break;
    case 4: __builtin_lsr_rri(variable, variable, "4"); break;
    case 5: __builtin_lsr_rri(variable, variable, "5"); break;
    case 6: __builtin_lsr_rri(variable, variable, "6"); break;
    case 7: __builtin_lsr_rri(variable, variable, "7"); break;
    case 8: __builtin_lsr_rri(variable, variable, "8"); break;
    case 9: __builtin_lsr_rri(variable, variable, "9"); break;
    case 10: __builtin_lsr_rri(variable, variable, "10"); break;
    case 11: __builtin_lsr_rri(variable, variable, "11"); break;
    case 12: __builtin_lsr_rri(variable, variable, "12"); break;
    case 13: __builtin_lsr_rri(variable, variable, "13"); break;
    case 14: __builtin_lsr_rri(variable, variable, "14"); break;
    case 15: __builtin_lsr_rri(variable, variable, "15"); break;
    case 16: __builtin_lsr_rri(variable, variable, "16"); break;
    case 17: __builtin_lsr_rri(variable, variable, "17"); break;
    case 18: __builtin_lsr_rri(variable, variable, "18"); break;
    case 19: __builtin_lsr_rri(variable, variable, "19"); break;
    case 20: __builtin_lsr_rri(variable, variable, "20"); break;
    case 21: __builtin_lsr_rri(variable, variable, "21"); break;
    case 22: __builtin_lsr_rri(variable, variable, "22"); break;
    case 23: __builtin_lsr_rri(variable, variable, "23"); break;
    case 24: __builtin_lsr_rri(variable, variable, "24"); break;
    case 25: __builtin_lsr_rri(variable, variable, "25"); break;
    case 26: __builtin_lsr_rri(variable, variable, "26"); break;
    case 27: __builtin_lsr_rri(variable, variable, "27"); break;
    case 28: __builtin_lsr_rri(variable, variable, "28"); break;
    case 29: __builtin_lsr_rri(variable, variable, "29"); break;
    case 30: __builtin_lsr_rri(variable, variable, "30"); break;
    case 31: __builtin_lsr_rri(variable, variable, "31"); break;
    }
    // clang-format on
    return variable;
}

//! @pre constant < (UINT64_C(1) << 33)
//! @return (constant * (variable >> sh_pre)) >> sh_post
inline __attribute__((always_inline)) uint32_t lsr_mul_lsr(
    uint64_t constant, uint32_t variable, int nbits_variable, int sh_pre, int sh_post)
{
    const bool big_variable = (sh_pre == 0 && nbits_variable >= 32 && variable >= (1u << 31));
    if (big_variable) {
        // (constant * (variable - 2^31)) >> sh_post + constant * 2^(31 - sh_post)
        variable = variable % (1u << 31);
    } else {
        variable = lsr_imm(variable, sh_pre);
    }

    const uint32_t tmp = lsr_mul_lsr_impl(constant, variable, sh_post);

    if (big_variable) {
        if (sh_post >= 31) {
            return tmp + (uint32_t)(constant >> (sh_post - 31));
        } else {
            return tmp + (uint32_t)(constant << (31 - sh_post));
        }
    } else {
        return tmp;
    }
}

#define DECLARE_DIV_BY(divisor, tag) \
    uint32_t DIV##tag##_BY_##divisor(uint32_t);

#define DEFINE_DIV_BY(divisor, nbits_dividend, tag)                              \
    uint32_t DIV##tag##_BY_##divisor(uint32_t n)                                 \
    {                                                                            \
        const uint32_t d = (divisor);                                            \
        const int nbits = (nbits_dividend);                                      \
                                                                                 \
        /* n / divisor == (n >> sh_pre) / new_d */                               \
        const int sh_pre = FLOOR_LOG2_UINT32(d & (~d + 1));                      \
        const uint32_t new_d = d >> sh_pre;                                      \
                                                                                 \
        /* (n >> sh_pre) / new_d == (m * (n >> sh_pre)) >> sh_post */            \
        uint64_t m;                                                              \
        int sh_post;                                                             \
        choose_multiplier_for_div_by_const(new_d, nbits - sh_pre, &m, &sh_post); \
                                                                                 \
        if (new_d == 1) {                                                        \
            return n >> sh_pre;                                                  \
        } else {                                                                 \
            /* #define lsr_mul_lsr(m, n, _, sh_pre, sh_post)                     \
                   ((uint32_t)(((m) * ((n) >> sh_pre)) >> sh_post)) */           \
            return lsr_mul_lsr(m, n, nbits, sh_pre, sh_post);                    \
        }                                                                        \
    }
