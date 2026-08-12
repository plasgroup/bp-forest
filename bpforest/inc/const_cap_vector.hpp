#pragma once

#include <cstddef>
#include <memory>
#include <new>


template <typename T, size_t N>
class ConstantCapacityVector
{
    alignas(T) std::byte buf[N][sizeof(T)];
    size_t size_ = 0;

public:
    ~ConstantCapacityVector();

    T& operator[](size_t i) { return *std::launder(reinterpret_cast<T*>(&buf[i][0])); }
    const T& operator[](size_t i) const { return *std::launder(reinterpret_cast<const T*>(&buf[i][0])); }
    constexpr size_t size() const { return size_; }

    void resize(size_t new_size);

    constexpr T* begin() { return std::addressof((*this)[0]); }
    constexpr const T* begin() const { return std::addressof((*this)[0]); }
    constexpr const T* cbegin() const { return static_cast<const ConstantCapacityVector>(*this).begin(); }

    constexpr T* end() { return std::addressof((*this)[size_]); }
    constexpr const T* end() const { return std::addressof((*this)[size_]); }
    constexpr const T* cend() const { return static_cast<const ConstantCapacityVector&>(*this).end(); }
};


template <typename T, size_t N>
ConstantCapacityVector<T, N>::~ConstantCapacityVector()
{
    for (size_t i = 0; i < size_; i++) {
        (*this)[i].~T();
    }
}

template <typename T, size_t N>
void ConstantCapacityVector<T, N>::resize(size_t new_size)
{
    if (new_size > N) {
        throw std::bad_alloc{};
    }

    for (size_t i = size_; i < new_size; i++) {
        new (&buf[i][0]) T;
    }
    for (size_t i = new_size; i < size_; i++) {
        (*this)[i].~T();
    }
    size_ = new_size;
}
