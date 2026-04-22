#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

// Noise-aware overload threshold (Bernstein closed form).
//
// Per-DPU cold query count under the null (goal-loaded) hypothesis is
//   X ~ Binomial(B, p),  p = max(3, a+1) / (3*D)
// with mean lambda = B*p, variance sigma2 = lambda*(1-p).
// For a target per-batch false-positive rate fp_rate, Bonferroni assigns
// each DPU the budget q = fp_rate / D, i.e. L = ln(D / fp_rate).
//
// Bernstein's inequality (with the safe bound M <= 1 for Bernoulli) gives
//   P(X - lambda >= t)  <=  exp( -t^2 / (2*sigma2 + 2*t/3) )  <=  1/e^L.
// Solving the quadratic yields the closed form
//   t  =  L/3  +  sqrt(L^2/9 + 2*sigma2*L).
// T_count = ceil(lambda + t) is the smallest count treated as overload.
// Callers fire on "count > threshold", so the stored threshold is
//   threshold = T_count - 1.
struct NoiseParams {
    double p;
    double one_minus_p;
    double L;   // ln(D / fp_rate)
    double K1;  // L/3
    double K2;  // L^2/9

    static NoiseParams compute(double fp_rate, unsigned ndpus, unsigned balancing)
    {
        NoiseParams np;
        np.p = static_cast<double>(std::max(3u, balancing + 1u)) / (3.0 * static_cast<double>(ndpus));
        np.one_minus_p = 1.0 - np.p;
        np.L = std::log(static_cast<double>(ndpus) / fp_rate);
        np.K1 = np.L / 3.0;
        np.K2 = np.L * np.L / 9.0;
        return np;
    }

    // Smallest count treated as overload. Fires iff observed count >= this.
    uint32_t threshold_count(std::size_t batch_size) const
    {
        const double B = static_cast<double>(batch_size);
        const double lam = B * p;
        const double sigma2 = lam * one_minus_p;
        const double t = K1 + std::sqrt(2.0 * sigma2 * L + K2);
        const double raw = std::ceil(lam + t);
        if (raw >= static_cast<double>(std::numeric_limits<uint32_t>::max())) {
            return std::numeric_limits<uint32_t>::max();
        }
        if (raw < 0.0) {
            return 0;
        }
        return static_cast<uint32_t>(raw);
    }

    // Threshold under the "count > threshold" fire rule.
    uint32_t stored_threshold(std::size_t batch_size) const
    {
        const uint32_t tc = threshold_count(batch_size);
        return tc == 0 ? 0 : tc - 1;
    }
};
