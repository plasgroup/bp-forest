#pragma once

#include "workload_types.h"

#include <cstddef>
#include <utility>
#include <vector>


struct WorkloadBuffer {
    std::vector<key_int64_t> buffer;

    WorkloadBuffer(std::vector<key_int64_t> b) : buffer{std::move(b)} {}

private:
    size_t consumed = 0;

public:
    // @return {ptr to head, size} of taken buf
    std::pair<key_int64_t*, size_t> take(size_t request)
    {
        const auto next_consumed = std::min(consumed + request, buffer.size());
        const auto res = std::make_pair(buffer.data() + consumed, next_consumed - consumed);
        consumed += next_consumed;
        return res;
    }
};
