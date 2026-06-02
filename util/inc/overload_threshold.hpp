#pragma once

#include "noise_params.hpp"
#include "overload.hpp"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <ostream>
#include <variant>

// Rebalance-trigger policy for an observed per-DPU query count.  Callers fire
// rebalance when the count exceeds the threshold produced by
// OverloadThreshold::threshold_for, which takes the per-batch fair-share goal
// as the null mean.
//
// Spec is the user-facing choice:
//   HighWatermarkRatio : threshold = goal * value.
//   FalsePositiveRate  : target per-batch false-positive rate; the threshold
//                        is the Bernstein closed form in NoiseParams.
struct HighWatermarkRatio {
    double value;
};
struct FalsePositiveRate {
    double value;
};
using OverloadThresholdSpec = std::variant<HighWatermarkRatio, FalsePositiveRate>;

struct OverloadThreshold {
    std::variant<HighWatermarkRatio, NoiseParams> policy;

    explicit OverloadThreshold(const OverloadThresholdSpec& spec)
        : policy(std::visit(
              overload(
                  [](const HighWatermarkRatio& r) -> std::variant<HighWatermarkRatio, NoiseParams> { return r; },
                  [&](const FalsePositiveRate& fpr) -> std::variant<HighWatermarkRatio, NoiseParams> {
                      return NoiseParams::compute(fpr.value);
                  }),
              spec))
    {
    }

    // Threshold to compare an observed per-DPU query count against (fire when
    // count > threshold).  `goal` is the per-batch fair-share count used as
    // the null mean: X ~ Binomial(B, goal/B) (cold passes its cold-query goal,
    // hot its split floor).  `family` is the per-batch total number of
    // statistical tests for the Bonferroni budget (cold nr_base_parts + hot
    // pre-filter nr_existing_hots); ignored by HighWatermark (no statistics).
    //   goal > B         -> saturate to B (count <= B so it cannot fire); the
    //                       structural floor exceeds the observable max.
    //   HighWatermark    -> min(B, ceil(goal * r))        (family ignored)
    //   NoiseParams      -> min(B, Bernstein(B, goal, family) - 1)
    uint32_t threshold_for(std::size_t batch_size, uint32_t goal, unsigned family) const
    {
        const std::size_t B = batch_size;
        const uint32_t Bcap = (B > std::numeric_limits<uint32_t>::max())
                                  ? std::numeric_limits<uint32_t>::max()
                                  : static_cast<uint32_t>(B);
        if (static_cast<std::size_t>(goal) > B) {
            return Bcap;  // saturate: non-firing
        }
        return std::visit(
            overload(
                [&](const HighWatermarkRatio& r) -> uint32_t {
                    const double thr = std::ceil(static_cast<double>(goal) * r.value);
                    return thr >= static_cast<double>(Bcap) ? Bcap : static_cast<uint32_t>(thr);
                },
                [&](const NoiseParams& np) -> uint32_t {
                    const double p = (B > 0) ? static_cast<double>(goal) / static_cast<double>(B) : 0.0;
                    const uint32_t tc = np.threshold_count(B, p, family);
                    const uint32_t stored = tc == 0 ? 0u : tc - 1u;
                    return stored > Bcap ? Bcap : stored;
                }),
            policy);
    }
};

inline void print_overload_threshold_spec(std::ostream& ostr, const OverloadThresholdSpec& spec)
{
    std::visit(
        overload(
            [&](const HighWatermarkRatio& r) { ostr << "overload_threshold.high_watermark: " << r.value << "\n"; },
            [&](const FalsePositiveRate& fpr) { ostr << "overload_threshold.fp_rate: " << fpr.value << "\n"; }),
        spec);
}

// Register --high-watermark and --fp-rate on a cmdline::parser-like object.
// Callers decide the default when neither is set (see parse_overload_threshold_spec).
template <class Parser>
inline void add_overload_threshold_options(Parser& a)
{
    a.template add<std::optional<double>>(
        "high-watermark", 0,
        "overload threshold = per-DPU goal * r. Mutually exclusive with --fp-rate.",
        false);
    a.template add<std::optional<double>>(
        "fp-rate", 0,
        "target per-batch false-positive rate for overload detection: the probability of "
        "firing rebalance when the load is actually balanced. Mutually exclusive with --high-watermark.",
        false);
}

// Parse --high-watermark / --fp-rate with mutual-exclusion and range
// validation.  Returns std::nullopt if neither flag is set so the caller can
// apply its own default.
template <class Parser>
inline std::optional<OverloadThresholdSpec> parse_overload_threshold_spec(Parser& a)
{
    const auto hwm = a.template get<std::optional<double>>("high-watermark");
    const auto fpr = a.template get<std::optional<double>>("fp-rate");
    if (hwm && fpr) {
        std::cerr << "must not set both --high-watermark and --fp-rate" << std::endl;
        std::exit(1);
    }
    if (hwm) {
        if (!(*hwm > 1.0)) {
            std::cerr << "--high-watermark must be > 1.0" << std::endl;
            std::exit(1);
        }
        return OverloadThresholdSpec{HighWatermarkRatio{*hwm}};
    }
    if (fpr) {
        if (!(*fpr > 0.0 && *fpr < 1.0)) {
            std::cerr << "--fp-rate must be in (0, 1)" << std::endl;
            std::exit(1);
        }
        return OverloadThresholdSpec{FalsePositiveRate{*fpr}};
    }
    return std::nullopt;
}
