#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

// Bernstein closed-form overload threshold.
//
// A count under the null hypothesis is modelled as
//   X ~ Binomial(B, p),  lambda = B*p,  M = 1-p,  sigma2 = lambda*M
// where B and the per-trial rate `p` are both supplied by the caller; how `p`
// is chosen is not this struct's concern.
//
// For a target false-positive rate fp_rate split across `family` tests by
// Bonferroni, each test gets budget q = fp_rate / family, i.e.
// L = ln(family / fp_rate).  `family` is passed per call (it may vary), so
// NoiseParams retains only `fp_rate` and recomputes L every call.
//
// Bernstein's inequality with M = 1-p (tight for centered Bernoulli when
// p < 1/2) gives
//   P(X - lambda >= t)  <=  exp( -t^2 / (2*sigma2 + 2*M*t/3) )  <=  1/e^L,
// whose quadratic solution is
//   t  =  M*L/3  +  sqrt(M^2*L^2/9 + 2*sigma2*L).
// threshold_count = ceil(lambda + t) is the smallest count treated as
// overload.  The "-1" for a "count > threshold" fire rule and any
// saturation/clamping are left to the caller.
struct NoiseParams {
    double fp_rate;  // retained; L = ln(family / fp_rate) computed per call

    static NoiseParams compute(double fp_rate)
    {
        NoiseParams np;
        np.fp_rate = fp_rate;
        return np;
    }

    // Bonferroni-corrected log budget for the given per-batch test count.
    double L_for(unsigned family) const
    {
        return std::log(static_cast<double>(family) / fp_rate);
    }

    // Smallest count treated as overload for X ~ Binomial(batch_size, p).
    uint32_t threshold_count(std::size_t batch_size, double p, unsigned family) const
    {
        const double B = static_cast<double>(batch_size);
        const double M = 1.0 - p;
        const double lam = B * p;
        const double sigma2 = lam * M;
        const double L = L_for(family);
        const double K1 = M * L / 3.0;
        const double t = K1 + std::sqrt(2.0 * sigma2 * L + K1 * K1);
        return clamp_u32(std::ceil(lam + t));
    }

private:
    static uint32_t clamp_u32(double raw)
    {
        if (raw >= static_cast<double>(std::numeric_limits<uint32_t>::max())) {
            return std::numeric_limits<uint32_t>::max();
        }
        if (raw < 0.0) {
            return 0;
        }
        return static_cast<uint32_t>(raw);
    }
};
