#pragma once

#include <type_traits>
#include <utility>


template <typename Func>
struct RAII {
private:
    std::remove_reference_t<Func> func;

public:
    explicit RAII(Func&& func) : func{std::forward<Func>(func)} {}
    ~RAII()
    {
        std::move(func)();
    }

    RAII(const RAII&) = delete;
    RAII& operator=(const RAII&) = delete;
};
