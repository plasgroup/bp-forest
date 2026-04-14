#pragma once

#include "delimited_string.hpp"

#include <dirent.h>
#include <sys/stat.h>

#include <cstring>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>


namespace FileSystem
{

inline uintmax_t file_size(const std::string& path)
{
    struct stat stat_buf;
    if (stat(path.c_str(), &stat_buf) != 0) {
        using namespace std::literals::string_literals;
        throw std::system_error{errno, std::generic_category(), "stat(" + path + ")"};
    }
    return static_cast<uintmax_t>(stat_buf.st_size);
}

inline bool create_directory(const std::string& path)
{
    const int result = mkdir(path.c_str(), 0755);
    if (result != 0) {
        if (errno != EEXIST) {
            throw std::system_error{errno, std::generic_category(), "mkdir(" + path + ")"};
        }
        struct stat stat_buf;
        if (stat(path.c_str(), &stat_buf) == 0) {
            if (!S_ISDIR(stat_buf.st_mode)) {
                throw std::system_error{ENOTDIR, std::generic_category(), "mkdir(" + path + ")"};
            }
        }
    }
    return errno == EEXIST;
}

inline bool create_directories(const std::string& path)
{
    using namespace std::literals::string_literals;

    std::string_view view = path;
    std::string prefix;
    prefix.reserve(path.size());

    bool created = false;

    std::string_view::size_type i_char = 0;
    foreach_delimited_length(path, '/', [&](std::string_view::size_type len) {
        prefix += std::string{view.substr(i_char - (i_char != 0), len + (i_char != 0))};

        if (view.substr(i_char, len) != ".." && !prefix.empty()) {
            created = create_directory(prefix) || created;
        }

        i_char += len + 1;
    });

    return created;
}

inline std::vector<std::string> get_directory_entries(const std::string& dir_path)
{
    DIR* const dir = opendir(dir_path.c_str());
    if (dir == nullptr) {
        throw std::system_error{errno, std::generic_category(), "opendir(" + dir_path + ")"};
    }

    std::vector<std::string> res;

    dirent* dirent;
    errno = 0;
    while ((dirent = readdir(dir)) != nullptr) {
        if (std::strcmp(dirent->d_name, ".") == 0 || std::strcmp(dirent->d_name, "..") == 0) {
            continue;
        }
        res.emplace_back(dirent->d_name);
    }
    if (errno != 0) {
        throw std::system_error{errno, std::generic_category(), "readdir(" + dir_path + ")"};
    }

    if (closedir(dir) != 0) {
        throw std::system_error{errno, std::generic_category(), "closedir(" + dir_path + ")"};
    }

    return res;
}

}  // namespace FileSystem