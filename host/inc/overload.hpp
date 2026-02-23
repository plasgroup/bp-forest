#pragma once

#include <cstddef>
#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>


template <typename... Fs>
class Overload
{
private:
    std::tuple<Fs&...> fs_;

    template <size_t I, typename... Args>
    decltype(auto) invoke_first(Args&&... args)
    {
        if constexpr (I >= sizeof...(Fs)) {
            static_assert(I < sizeof...(Fs), "No matching invocable found.");
        } else {
            auto& f = std::get<I>(fs_);
            using F = std::remove_reference_t<decltype(f)>;

            if constexpr (std::is_invocable_v<F&, Args...>) {
                return std::invoke(f, std::forward<Args>(args)...);
            } else {
                return invoke_first<I + 1>(std::forward<Args>(args)...);
            }
        }
    }

public:
    explicit Overload(Fs&... fs) : fs_(fs...) {}

    template <typename... Args>
    decltype(auto) operator()(Args&&... args) &&
    {
        return invoke_first<0>(std::forward<Args>(args)...);
    }

    template <typename... Args>
    decltype(auto) operator()(Args&&...) const& = delete;
};


template <typename... Fs>
auto overload(Fs&&... fs)
{
    return Overload<Fs...>(fs...);
}
