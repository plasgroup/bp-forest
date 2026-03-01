#pragma once

#include <optional>
#include <type_traits>
#include <utility>


template <typename Func>
struct RAII {
private:
    std::optional<std::remove_reference_t<Func>> func;

public:
    explicit RAII(Func&& func) : func{std::forward<Func>(func)} {}
    ~RAII()
    {
        if (func) {
            std::move (*func)();
        }
    }

    RAII(const RAII&) = delete;
    RAII& operator=(const RAII&) = delete;

    RAII(RAII&& other) : func{std::move(other.func)}
    {
        other.func.reset();
    }
    RAII& operator=(RAII&& other)
    {
        if (func) {
            std::move (*func)();
        }
        func = std::move(other.func);
        other.func.reset();
        return *this;
    }

    void finalize()
    {
        if (func) {
            std::move (*func)();
        }
        func.reset();
    }
};
