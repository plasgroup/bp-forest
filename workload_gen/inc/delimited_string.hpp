#pragma once

#include <string_view>


template <class Func>
inline void foreach_delimited_length(std::string_view str, char delim, Func&& func)
{
    for (;;) {
        std::string_view::size_type i_char = str.find_first_of(delim);
        if (i_char == std::string_view::npos) {
            func(str.length());
            break;
        } else {
            func(i_char);
            str.remove_prefix(i_char + 1);
        }
    }
}

template <class Func>
inline void expandCSV(std::string_view csv, Func&& func, char delim = ',')
{
    std::string_view::size_type i_char_end = 0;
    foreach_delimited_length(csv, delim, [&](std::string_view::size_type cell_length) {
        func(csv.substr(i_char_end, cell_length));
        i_char_end += cell_length + 1;
    });
}
