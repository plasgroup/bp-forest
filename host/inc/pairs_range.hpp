#pragma once

#include "common.h"
#include "host_params.hpp"
#include "linked_list.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>


struct PairsRange {
private:
    const KVPair *pairs_begin, *pairs_end;

public:
    size_t npairs() const { return static_cast<size_t>(pairs_end - pairs_begin); }
    const KVPair* begin() const { return pairs_begin; }
    const KVPair* end() const { return pairs_end; }

    PairsRange() = default;
    PairsRange(const KVPair* begin, const KVPair* end) : pairs_begin{begin}, pairs_end{end} {}
    PairsRange(const PairsRange&) = default;
    PairsRange& operator=(const PairsRange&) = default;
};
struct DataChunkIterator {
    inline static constexpr uint32_t ChunkSizeInBytes = sizeof(KVPair) * KVPairsChunkSize;

    using value_type = DataChunkIterator;
    using reference = value_type&;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using iterator_category = std::random_access_iterator_tag;

    using const_pointer = const value_type*;

private:
    const KVPair *cursor{nullptr}, *part_begin, *part_end;
    uint32_t* p_load;

public:
    uint32_t& load() const { return *p_load; }
    const KVPair* begin() const { return cursor; }
    const KVPair* end() const { return (++DataChunkIterator{*this}).begin(); }
    size_t npairs() const { return static_cast<size_t>(end() - begin()); }

    DataChunkIterator() = default;
    DataChunkIterator(const KVPair* cursor, const KVPair* part_begin, const KVPair* part_end, uint32_t* load)
        : cursor{cursor}, part_begin{part_begin}, part_end{part_end}, p_load{load}
    {
    }
    DataChunkIterator(const DataChunkIterator&) = default;
    DataChunkIterator& operator=(const DataChunkIterator&) = default;
    reference operator*() & { return *this; }
    value_type operator*() && { return *this; }
    pointer operator->() { return this; }
    const_pointer operator->() const { return this; }
    DataChunkIterator& operator++()
    {
        const uintptr_t addr = std::min({reinterpret_cast<uintptr_t>(part_end),
            reinterpret_cast<uintptr_t>(cursor) + ChunkSizeInBytes});
        cursor = reinterpret_cast<const KVPair*>(addr);
        ++p_load;
        return *this;
    }
    DataChunkIterator& operator--()
    {
        const uintptr_t offset_in_part = reinterpret_cast<uintptr_t>(cursor) - reinterpret_cast<uintptr_t>(part_begin),
                        addr = offset_in_part - ChunkSizeInBytes,
                        aligned = (addr + ChunkSizeInBytes - 1) / ChunkSizeInBytes * ChunkSizeInBytes;  // Handling when cursor == part_end
        cursor = reinterpret_cast<const KVPair*>(aligned);
        --p_load;
        return *this;
    }
    DataChunkIterator& operator+=(difference_type d)
    {
        const uintptr_t offset_in_part = reinterpret_cast<uintptr_t>(cursor) - reinterpret_cast<uintptr_t>(part_begin),
                        shifted = offset_in_part + static_cast<uintptr_t>(ChunkSizeInBytes * d),
                        aligned = (shifted + ChunkSizeInBytes - 1) / ChunkSizeInBytes * ChunkSizeInBytes,
                        addr = reinterpret_cast<uintptr_t>(part_begin) + aligned,
                        clamped = std::min({addr, reinterpret_cast<uintptr_t>(part_end)});
        cursor = reinterpret_cast<const KVPair*>(clamped);
        p_load += d;
        return *this;
    }

    friend bool operator==(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return lhs.cursor == rhs.cursor; }
    friend difference_type operator-(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return lhs.p_load - rhs.p_load; }

    DataChunkIterator& operator-=(difference_type d) { return (*this) += (-d); }
    DataChunkIterator operator++(int)
    {
        DataChunkIterator tmp{*this};
        ++*this;
        return tmp;
    }
    DataChunkIterator operator--(int)
    {
        DataChunkIterator tmp{*this};
        --*this;
        return tmp;
    }
    DataChunkIterator operator[](difference_type d) const { return *(*this + d); }

    friend bool operator!=(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return !(lhs == rhs); }
    friend DataChunkIterator operator+(const DataChunkIterator& it, difference_type d)
    {
        DataChunkIterator tmp{it};
        tmp += d;
        return tmp;
    }
    friend DataChunkIterator operator+(difference_type d, const DataChunkIterator& it) { return it + d; }
    friend DataChunkIterator operator-(const DataChunkIterator& it, difference_type d)
    {
        DataChunkIterator tmp{it};
        tmp -= d;
        return tmp;
    }
    friend bool operator<(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return (rhs - lhs) > 0; }
    friend bool operator>(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return rhs < lhs; }
    friend bool operator<=(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return !(lhs > rhs); }
    friend bool operator>=(const DataChunkIterator& lhs, const DataChunkIterator& rhs) { return !(lhs < rhs); }
};
struct ChunkedPairsRange : PairsRange {
private:
    uint32_t* p_load;

public:
    size_t nchunks() const { return (npairs() + KVPairsChunkSize - 1) / KVPairsChunkSize; }
    DataChunkIterator begin() const { return {PairsRange::begin(), PairsRange::begin(), PairsRange::end(), p_load}; }
    DataChunkIterator end() const { return {PairsRange::end(), PairsRange::begin(), PairsRange::end(), p_load + nchunks()}; }
    uint32_t* set_load_ary(uint32_t* p) { return p_load = p; }

    ChunkedPairsRange() = default;
    ChunkedPairsRange(const PairsRange& range, uint32_t* load = nullptr) : PairsRange{range}, p_load{load} {}
    ChunkedPairsRange(const DataChunkIterator& begin, const DataChunkIterator& end) : PairsRange{begin->begin(), end->begin()}, p_load{&begin->load()} {}
    ChunkedPairsRange(const ChunkedPairsRange&) = default;
    ChunkedPairsRange& operator=(const ChunkedPairsRange&) = default;
};

using LinkedPairsRange = LinkedElement<PairsRange>;
using LinkedChunkedPairsRange = LinkedElement<ChunkedPairsRange>;
