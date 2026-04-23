#pragma once

#include "assert.hpp"
#include "common.h"

#include <algorithm>
#include <map>
#include <tuple>

using Key = key_uint64_t;

enum class DelimType {
    Base,
    Cold,
    Hot,
};
struct Delim {
    Key key;
    DelimType type;

    friend bool operator<(const Delim& lhs, const Delim& rhs)
    {
        return std::tie(lhs.key, lhs.type) < std::tie(rhs.key, rhs.type);
    }
};
struct PartInfo {
    unsigned dpu;
};
using Delims = std::map<Delim, PartInfo>;

struct Partitioning {
    Delims delims;

    unsigned ndpus() const
    {
        const auto it = std::find_if(delims.rbegin(), delims.rend(), [](const auto& delim) { return delim.first.type == DelimType::Base; });
        ASSERT(it != delims.rend());
        return it->second.dpu + 1;
    }
    Delims::const_iterator get_base(Delims::const_iterator it) const
    {
        while (it != delims.begin() && it->first.type != DelimType::Base) {
            --it;
        }
        ASSERT(it->first.type == DelimType::Base);
        return it;
    }
};
