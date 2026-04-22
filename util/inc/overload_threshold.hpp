#pragma once

#include "noise_params.hpp"
#include "overload.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <ostream>
#include <variant>

// Rebalance-trigger policy for per-DPU cold-query count.  Callers fire
// rebalance when observed count exceeds the threshold produced by
// OverloadThreshold::threshold_for.
//
// Spec is the user-facing choice:
//   HighWatermarkRatio : threshold = per-DPU goal * value.
//   FalsePositiveRate  : target per-batch false-positive rate; threshold is
//                        the Bernstein closed form in NoiseParams, resolved
//                        once ndpus and balancing are known.
struct HighWatermarkRatio {
    double value;
};
struct FalsePositiveRate {
    double value;
};
using OverloadThresholdSpec = std::variant<HighWatermarkRatio, FalsePositiveRate>;

// A spec resolved against a concrete DPU count and balancing factor.
struct OverloadThreshold {
    std::variant<HighWatermarkRatio, NoiseParams> policy;

    OverloadThreshold(const OverloadThresholdSpec& spec, unsigned ndpus, unsigned balancing)
        : policy(std::visit(
              overload(
                  [](const HighWatermarkRatio& r) -> std::variant<HighWatermarkRatio, NoiseParams> { return r; },
                  [&](const FalsePositiveRate& fpr) -> std::variant<HighWatermarkRatio, NoiseParams> {
                      return NoiseParams::compute(fpr.value, ndpus, balancing);
                  }),
              spec))
    {
    }

    // Threshold to compare per-DPU cold-query count against (fire when count > threshold).
    uint32_t threshold_for(std::size_t batch_size, uint32_t cold_query_goal) const
    {
        return std::visit(
            overload(
                [&](const HighWatermarkRatio& r) { return static_cast<uint32_t>(cold_query_goal * r.value); },
                [&](const NoiseParams& np) { return np.stored_threshold(batch_size); }),
            policy);
    }

    // Prints the derived noise-aware parameters if applicable; prints nothing for HighWatermarkRatio.
    void print_resolution(std::ostream& ostr) const
    {
        if (const auto* np = std::get_if<NoiseParams>(&policy)) {
            ostr << "overload_threshold.noise_params:"
                 << " p=" << np->p
                 << " L=" << np->L
                 << " K1=" << np->K1
                 << " K2=" << np->K2
                 << "\n";
        }
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
