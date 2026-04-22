#include "noise_params.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>

namespace
{
bool approx(double a, double b, double eps)
{
    return std::fabs(a - b) <= eps * std::max(1.0, std::fabs(b));
}

void test_compute_constants()
{
    // D=1000, alpha=0.001, a=10 -> p = 11/3000, L = ln(1e6)
    const auto np = NoiseParams::compute(0.001, 1000, 10);
    assert(approx(np.p, 11.0 / 3000.0, 1e-12));
    assert(approx(np.one_minus_p, 1.0 - 11.0 / 3000.0, 1e-12));
    assert(approx(np.L, std::log(1e6), 1e-12));
    assert(approx(np.K1, np.L / 3.0, 1e-12));
    assert(approx(np.K2, np.L * np.L / 9.0, 1e-12));
}

void test_balancing_floor()
{
    // balancing=1 -> max(3, 2) = 3 -> p = 1/D
    const auto np = NoiseParams::compute(0.01, 100, 1);
    assert(approx(np.p, 1.0 / 100.0, 1e-12));
    // balancing=2 -> max(3, 3) = 3 -> p = 1/D
    const auto np2 = NoiseParams::compute(0.01, 100, 2);
    assert(approx(np2.p, 1.0 / 100.0, 1e-12));
    // balancing=3 -> max(3, 4) = 4 -> p = 4/(3D)
    const auto np3 = NoiseParams::compute(0.01, 100, 3);
    assert(approx(np3.p, 4.0 / 300.0, 1e-12));
}

void test_threshold_formula_manual()
{
    // D=1000, alpha=0.001, a=10, B=1e6
    const auto np = NoiseParams::compute(0.001, 1000, 10);
    const double B = 1e6;
    const double lam = B * np.p;                  // ~= 3666.67
    const double sigma2 = lam * np.one_minus_p;   // ~= 3653.22
    const double t = np.K1 + std::sqrt(2.0 * sigma2 * np.L + np.K2);
    const uint32_t tc_expected = static_cast<uint32_t>(std::ceil(lam + t));
    const uint32_t tc = np.threshold_count(static_cast<size_t>(B));
    assert(tc == tc_expected);
    // stored = tc - 1 (because main.cpp fires on count > threshold)
    assert(np.stored_threshold(static_cast<size_t>(B)) == tc - 1);
    // sanity range (spot-checked against Python eval_threshold_ideas.py)
    assert(tc >= 3980 && tc <= 4010);
}

void test_threshold_small_batch()
{
    // Small B: lam is tiny but threshold must still be at least ceil(K1 + sqrt(K2))
    const auto np = NoiseParams::compute(0.001, 1000, 10);
    const uint32_t tc0 = np.threshold_count(0);
    // B=0 -> lam=0, sigma2=0, t=K1+sqrt(K2)=2*K1
    const double expected_t = 2.0 * np.K1;
    assert(tc0 == static_cast<uint32_t>(std::ceil(expected_t)));

    // B=100: lam=0.367, threshold should stay >= tc0 since t is larger with positive sigma2
    const uint32_t tc100 = np.threshold_count(100);
    assert(tc100 >= tc0);
}

void test_threshold_monotone_in_B()
{
    const auto np = NoiseParams::compute(0.001, 1000, 10);
    uint32_t prev = 0;
    for (size_t B : {size_t{1000}, size_t{10000}, size_t{100000}, size_t{1000000}, size_t{10000000}, size_t{100000000}}) {
        const uint32_t tc = np.threshold_count(B);
        assert(tc >= prev);
        prev = tc;
    }
}

void test_stored_vs_count()
{
    // stored_threshold + 1 == threshold_count (when count > 0) so the fire rule
    // "count > stored" matches "count >= threshold_count"
    const auto np = NoiseParams::compute(0.01, 100, 10);
    for (size_t B : {size_t{100}, size_t{1000}, size_t{10000}, size_t{100000}}) {
        const uint32_t tc = np.threshold_count(B);
        const uint32_t stored = np.stored_threshold(B);
        if (tc > 0) {
            assert(stored + 1 == tc);
        } else {
            assert(stored == 0);
        }
    }
}

void test_upper_bound_vs_legacy_extremes()
{
    // At extreme B=1e8 and D=1000, alpha=0.001, a=10:
    //   goal = B*p = 366667, sigma = sqrt(lam*(1-p)) ~= 605.3
    //   legacy hwm=1.05: threshold_count = ceil(goal*0.05) + goal ~= 385001 (miss by 30+ sigma)
    //   A4: threshold_count ~= goal + 5.26*sigma ~= 369850
    // Check A4 gives a much tighter threshold than legacy at huge B.
    const auto np = NoiseParams::compute(0.001, 1000, 10);
    const size_t B = 100'000'000;
    const uint32_t tc_a4 = np.threshold_count(B);
    const uint64_t goal = static_cast<uint64_t>(B) * 11 / 3000;
    const uint32_t tc_legacy = static_cast<uint32_t>(goal * 1.05) + 1;
    assert(tc_a4 < tc_legacy);
    assert(tc_a4 > goal);  // still an upper threshold above goal
    assert(tc_a4 - goal >= 1000);  // at least several sigma above goal
}
}  // namespace

int main()
{
    test_compute_constants();
    test_balancing_floor();
    test_threshold_formula_manual();
    test_threshold_small_batch();
    test_threshold_monotone_in_B();
    test_stored_vs_count();
    test_upper_bound_vs_legacy_extremes();
    std::puts("noise_params_test passed");
    return 0;
}
