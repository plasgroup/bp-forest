#pragma once

#include <cstddef>
#include <memory>


template <typename T>
class ExtendableBuffer
{
    std::unique_ptr<T[]> buffer;
    size_t capacity;

public:
    ExtendableBuffer(size_t capacity = 0) : capacity{capacity}
    {
        if (capacity != 0) {
            buffer.reset(new T[capacity]);
        }
    }

    T& operator[](size_t i) { return buffer[i]; }
    const T& operator[](size_t i) const { return buffer[i]; }

    void reserve(size_t required)
    {
        if (capacity < required) {
            capacity = required;
            buffer.reset(new T[capacity]);
        }
    }
};
