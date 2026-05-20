#include "noise_params.hpp"
#include "overload_threshold.hpp"

#include <algorithm>
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

// Reference Bernstein tail (same closed form as NoiseParams::threshold_count).
uint32_t ref_tc(double B, double p, double L)
{
    const double M = 1.0 - p;
    const double lam = B * p;
    const double sigma2 = lam * M;
    const double K1 = M * L / 3.0;
    const double t = K1 + std::sqrt(2.0 * sigma2 * L + K1 * K1);
    return static_cast<uint32_t>(std::ceil(lam + t));
}

OverloadThreshold fpr_threshold(double fp_rate)
{
    return OverloadThreshold{OverloadThresholdSpec{FalsePositiveRate{fp_rate}}};
}
OverloadThreshold hwm_threshold(double r)
{
    return OverloadThreshold{OverloadThresholdSpec{HighWatermarkRatio{r}}};
}

// ---- NoiseParams: policy-agnostic Bernstein calculator ----

void test_noise_params_retains_only_fp_rate()
{
    // compute() takes fp_rate alone; no ndpus/balancing leak in.
    const auto np = NoiseParams::compute(0.001);
    assert(approx(np.fp_rate, 0.001, 1e-12));
    assert(approx(np.L_for(1000), std::log(1e6), 1e-12));
    assert(approx(np.L_for(2000), std::log(2e6), 1e-12));
}

void test_threshold_count_matches_closed_form()
{
    const auto np = NoiseParams::compute(0.001);
    const unsigned family = 1000;
    const double L = np.L_for(family);
    const double B = 1e6;
    const double p = 11.0 / 3000.0;  // a caller-supplied rate
    assert(np.threshold_count(static_cast<size_t>(B), p, family) == ref_tc(B, p, L));
}

void test_threshold_count_small_batch()
{
    const auto np = NoiseParams::compute(0.001);
    const unsigned family = 1000;
    const double p = 11.0 / 3000.0;
    const double K1 = (1.0 - p) * np.L_for(family) / 3.0;
    // B=0 -> lam=0, sigma2=0, t=K1+sqrt(K1^2)=2*K1
    const uint32_t tc0 = np.threshold_count(0, p, family);
    assert(tc0 == static_cast<uint32_t>(std::ceil(2.0 * K1)));
    // B=100: positive sigma2 only grows t
    assert(np.threshold_count(100, p, family) >= tc0);
}

void test_threshold_count_monotone_in_B()
{
    const auto np = NoiseParams::compute(0.001);
    const double p = 11.0 / 3000.0;
    uint32_t prev = 0;
    for (size_t B : {size_t{1000}, size_t{10000}, size_t{100000}, size_t{1000000}, size_t{10000000}, size_t{100000000}}) {
        const uint32_t tc = np.threshold_count(B, p, 1000);
        assert(tc >= prev);
        prev = tc;
    }
}

void test_threshold_count_monotone_in_family()
{
    // Larger family => larger L => higher (more conservative) threshold.
    const auto np = NoiseParams::compute(0.001);
    const double p = 11.0 / 3000.0;
    assert(np.threshold_count(1'000'000, p, 5000) >= np.threshold_count(1'000'000, p, 500));
}

// ---- OverloadThreshold: one threshold_for, goal is the null mean ----

void test_threshold_for_uses_goal_as_null_mean()
{
    const auto ot = fpr_threshold(0.001);
    const unsigned family = 1000;
    const size_t B = 1'000'000;
    const uint32_t g = 4000;

    const uint32_t thr = ot.threshold_for(B, g, family);
    assert(thr > g);  // strictly positive Bernstein tail

    const auto np = NoiseParams::compute(0.001);
    const double p = static_cast<double>(g) / static_cast<double>(B);
    const uint32_t tc = np.threshold_count(B, p, family);
    assert(thr == tc - 1);  // "-1" applied by OverloadThreshold

    // goal == B  -> p=1, M=0, t=0 -> tc=B -> stored B-1 (<= Bcap).
    assert(ot.threshold_for(B, static_cast<uint32_t>(B), family) == static_cast<uint32_t>(B) - 1);
    // goal  > B  -> saturate to B (never fires, since count <= B).
    assert(ot.threshold_for(B, static_cast<uint32_t>(B) + 1, family) == static_cast<uint32_t>(B));
}

void test_threshold_for_high_watermark()
{
    const auto ot = hwm_threshold(1.05);
    const size_t B = 1'000'000;
    const uint32_t g = 4000;
    assert(ot.threshold_for(B, g, 1) == static_cast<uint32_t>(std::ceil(g * 1.05)));
    assert(ot.threshold_for(B, g, 999) == static_cast<uint32_t>(std::ceil(g * 1.05)));  // family ignored
    // ceil(goal*r) exceeds B -> clamp to B.
    assert(ot.threshold_for(B, static_cast<uint32_t>(B), 1) == static_cast<uint32_t>(B));
    // goal > B -> saturate to B.
    assert(ot.threshold_for(B, static_cast<uint32_t>(B) + 1, 1) == static_cast<uint32_t>(B));
}

// ---- Cold path: caller passes goal = B * max(3,bal+1)/(3D) ----

void test_cold_goal_sanity()
{
    // D=1000, a=10 -> cold rate 11/3000; cold passes goal = B * 11/3000.
    const auto ot = fpr_threshold(0.001);
    const unsigned family = 1000;
    const size_t B = 1'000'000;
    const uint32_t goal = static_cast<uint32_t>(static_cast<double>(B) * 11.0 / 3000.0);
    const auto np = NoiseParams::compute(0.001);
    const uint32_t tc = np.threshold_count(B, static_cast<double>(goal) / static_cast<double>(B), family);
    assert(ot.threshold_for(B, goal, family) == tc - 1);
    // Sanity range (spot-checked against Python eval_threshold_ideas.py).
    assert(tc >= 3980 && tc <= 4010);
}

void test_cold_goal_below_legacy_high_watermark()
{
    const auto ot = fpr_threshold(0.001);
    const size_t B = 100'000'000;
    const uint32_t goal = static_cast<uint32_t>(static_cast<uint64_t>(B) * 11 / 3000);
    const uint32_t thr = ot.threshold_for(B, goal, 1000);
    const uint32_t legacy = static_cast<uint32_t>(static_cast<uint64_t>(goal) * 1.05) + 1;
    assert(thr < legacy);
    assert(thr > goal);
    assert(thr - goal >= 1000);
}
}  // namespace

int main()
{
    test_noise_params_retains_only_fp_rate();
    test_threshold_count_matches_closed_form();
    test_threshold_count_small_batch();
    test_threshold_count_monotone_in_B();
    test_threshold_count_monotone_in_family();
    test_threshold_for_uses_goal_as_null_mean();
    test_threshold_for_high_watermark();
    test_cold_goal_sanity();
    test_cold_goal_below_legacy_high_watermark();
    std::puts("noise_params_test passed");
    return 0;
}
