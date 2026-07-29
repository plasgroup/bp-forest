#pragma once

#include "assert.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


class TimerTree
{
    struct BreakdownParam;
    struct TimerTreeNode;

    struct TimeBreakdown {
        std::vector<std::pair<std::string, TimerTreeNode>> sub_timers;
        std::vector<uint8_t> orig2vec;

        TimeBreakdown() {}
        inline TimeBreakdown(std::initializer_list<BreakdownParam> params);

        void reset()
        {
            for (auto& sub_timer : sub_timers) {
                sub_timer.second.reset();
            }
        }

        void print_labels(std::ostream& ostr, std::string& prefix) const
        {
            bool first = true;
            for (uint8_t i : orig2vec) {
                auto& sub_timer = sub_timers[i];

                if (!first) {
                    ostr << ',';
                }
                first = false;

                const size_t prefix_length = prefix.size();
                prefix += sub_timer.first;
                sub_timer.second.print_labels(ostr, prefix);
                prefix.erase(prefix_length);
            }
        }
        void print(std::ostream& ostr) const
        {
            bool first = true;
            for (uint8_t i : orig2vec) {
                auto& sub_timer = sub_timers[i];

                if (!first) {
                    ostr << ',';
                }
                first = false;

                sub_timer.second.print(ostr);
            }
        }
    };
    struct TimerTreeNode {
        using Duration = std::chrono::nanoseconds;

        Duration time{};
        TimeBreakdown breakdown;

        TimerTreeNode() {}
        TimerTreeNode(std::initializer_list<BreakdownParam> breakdown_params) : breakdown{breakdown_params} {}

        void reset()
        {
            time = Duration::zero();
            breakdown.reset();
        }

        void print_labels(std::ostream& ostr, std::string& prefix) const
        {
            ostr << prefix;
            if (!breakdown.sub_timers.empty()) {
                ostr << ',';

                prefix += '>';
                breakdown.print_labels(ostr, prefix);
                prefix.pop_back();
            }
        }
        void print(std::ostream& ostr) const
        {
            ostr << time.count();
            if (!breakdown.sub_timers.empty()) {
                ostr << ',';

                breakdown.print(ostr);
            }
        }
    };

    struct BreakdownParam {
        std::pair<std::string, TimerTreeNode> data;

        BreakdownParam(std::string&& tag) : data{std::move(tag), {}} {}
        BreakdownParam(std::string&& tag, std::initializer_list<BreakdownParam> params) : data{std::move(tag), params}
        {
        }
    };

    struct LabelPrinter {
        const TimerTree* tree;
        friend std::ostream& operator<<(std::ostream& ostr, const LabelPrinter& printer)
        {
            std::string prefix;
            printer.tree->breakdown.print_labels(ostr, prefix);
            return ostr;
        }
    };
    struct Printer {
        const TimerTree* tree;
        friend std::ostream& operator<<(std::ostream& ostr, const Printer& printer)
        {
            printer.tree->breakdown.print(ostr);
            return ostr;
        }
    };

    TimerTreeNode* current{nullptr};
    TimeBreakdown breakdown;

    friend class ScopedTimer;

public:
    TimerTree(std::initializer_list<BreakdownParam> params) : breakdown(params) {}

    void reset() { breakdown.reset(); }

    LabelPrinter print_labels() const
    {
        return LabelPrinter{this};
    }
    Printer print() const
    {
        return Printer{this};
    }
};

inline TimerTree::TimeBreakdown::TimeBreakdown(std::initializer_list<BreakdownParam> params)
{
    ASSERT(params.size() <= std::numeric_limits<uint8_t>::max() + 1u);

    std::vector<uint8_t> tag_idxs(params.size());
    std::iota(tag_idxs.begin(), tag_idxs.end(), uint8_t{0});
    std::sort(tag_idxs.begin(), tag_idxs.end(), [&](uint8_t lhs, uint8_t rhs) { return params.begin()[lhs].data.first < params.begin()[rhs].data.first; });

    sub_timers.resize(params.size());
    orig2vec.resize(params.size());
    for (size_t i = 0; i < params.size(); i++) {
        sub_timers[i] = std::move(params.begin()[tag_idxs[i]].data);
        orig2vec[tag_idxs[i]] = static_cast<uint8_t>(i);
    }
}

class ScopedTimer
{
    TimerTree* tree;
    TimerTree::TimerTreeNode* prev_timer;

    TimerTree::TimerTreeNode::Duration prev_dur;
    std::chrono::high_resolution_clock::time_point begin_time;

public:
    ScopedTimer(TimerTree& tree, std::string_view tag) : tree{&tree}, prev_timer{tree.current}
    {
        TimerTree::TimeBreakdown& breakdown = prev_timer != nullptr ? prev_timer->breakdown : tree.breakdown;

        const auto iter = std::lower_bound(breakdown.sub_timers.begin(), breakdown.sub_timers.end(), tag,
            [](const std::pair<std::string, TimerTree::TimerTreeNode>& e, std::string_view tag) { return e.first < tag; });
        if (iter == breakdown.sub_timers.cend() || iter->first != tag) {
            std::cerr << "no timer named " << tag << std::endl;
            std::abort();
        }
        tree.current = &iter->second;

        prev_dur = tree.current->time;
        begin_time = std::chrono::high_resolution_clock::now();
    }
    ~ScopedTimer()
    {
        tree->current->time = prev_dur + std::chrono::duration_cast<TimerTree::TimerTreeNode::Duration>(std::chrono::high_resolution_clock::now() - begin_time);
        tree->current = prev_timer;
    }
};
