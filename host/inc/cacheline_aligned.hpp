#pragma once

#include <new>
#include <type_traits>
#include <utility>


template <typename DataType>
struct CachelineAligned {
#ifdef __cpp_lib_hardware_interference_size
    alignas(std::hardware_constructive_interference_size) DataType data;
#else
    alignas(64) DataType data;
#endif

    CachelineAligned() = default;
    explicit CachelineAligned(DataType&& data) : data{std::move(data)} {}
    explicit CachelineAligned(const DataType& data) : data{data} {}
    CachelineAligned(const CachelineAligned&) = default;
    CachelineAligned& operator=(const CachelineAligned&) = default;

    operator DataType&() & { return data; }
    operator DataType&&() && { return std::move(data); }
    operator const DataType&() const& { return data; }
    operator const DataType&&() const&& { return std::move(data); }

    DataType& get() & { return data; }
    DataType&& get() && { return std::move(data); }
    const DataType& get() const& { return data; }
    const DataType&& get() const&& { return std::move(data); }
};
