#pragma once

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <utility>


struct LogBuffer {
    ~LogBuffer() { std::free(buf_); }
    LogBuffer(const LogBuffer&) = delete;
    LogBuffer& operator=(const LogBuffer&) = delete;

    char* get() const { return buf_; }
    size_t size() const { return size_; }

private:
    LogBuffer(char* buf, size_t size) : buf_{buf}, size_{size} {}
    friend struct LogStream;

    char* buf_;
    size_t size_;
};


struct LogStream {
    LogStream();
    ~LogStream();
    LogStream(const LogStream&) = delete;
    LogStream& operator=(const LogStream&) = delete;

    std::FILE* get() const noexcept { return file; }
    std::unique_ptr<LogBuffer> close() &&;

private:
    std::FILE* file;
    char* buf;
    size_t size;
};


#include <cerrno>
#include <system_error>

inline LogStream::LogStream()
{
    file = open_memstream(&buf, &size);
    if (file == nullptr) {
        throw std::system_error{errno, std::generic_category(), "open_memstream"};
    }
}
inline LogStream::~LogStream()
{
    if (file != nullptr) {
        fclose(file);
        free(buf);
    }
}
inline std::unique_ptr<LogBuffer> LogStream::close() &&
{
    if (file == nullptr) {
        throw std::logic_error{"LogStream::close() can only be called once"};
    }

    if (fclose(std::exchange(file, nullptr)) != 0) {
        throw std::system_error{errno, std::generic_category(), "open_memstream"};
    }

    return std::unique_ptr<LogBuffer>{new LogBuffer{buf, size}};
}
