#pragma once

#include <cstddef>
#include <utility>
#include <vector>


template <typename T>
struct WorkloadBuffer {
    std::vector<T> buffer;

    WorkloadBuffer() = default;
    WorkloadBuffer(std::vector<T> b) : buffer{std::move(b)} {}
    WorkloadBuffer& operator=(std::vector<T>&& b)
    {
        buffer = std::move(b);
        consumed = 0;
        return *this;
    }

private:
    size_t consumed = 0;

public:
    // @return {ptr to head, size} of peeked buf
    std::pair<T*, size_t> peek(size_t request)
    {
        const auto end = std::min(consumed + request, buffer.size());
        const auto res = std::make_pair(buffer.data() + consumed, end - consumed);
        return res;
    }

    // @return {ptr to head, size} of taken buf
    std::pair<T*, size_t> take(size_t request)
    {
        const auto next_consumed = std::min(consumed + request, buffer.size());
        const auto res = std::make_pair(buffer.data() + consumed, next_consumed - consumed);
        consumed = next_consumed;
        return res;
    }
};
