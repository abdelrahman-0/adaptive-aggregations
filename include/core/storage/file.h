#pragma once

#include <fcntl.h>
#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>

#include "common/page.h"
#include "exceptions/exceptions_file.h"

enum FileMode : int8_t { READ, WRITE, READ_WRITE };

class File {
  private:
    std::string path;
    std::size_t size_in_bytes{0};
    std::size_t offset_begin{};
    std::size_t offset_end{};
    int fd{-1};

    [[nodiscard]] bool check_file_exists() const { return std::filesystem::exists({path}); }

    void open(FileMode mode) {
        switch (mode) {
        case READ: {
            fd = ::open(path.c_str(), O_RDONLY | O_NOATIME | O_DIRECT);
            break;
        }
        case WRITE: {
            fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
            break;
        }
        case READ_WRITE:
            break;
        }
        if (fd < 0) {
            throw FileOpenError{};
        }
    }

    void determine_size() {
        struct stat fileStat{};
        if (fstat(fd, &fileStat) < 0) {
            throw FileStatError();
        }
        size_in_bytes = fileStat.st_size;
    }

    void close() const {
        auto ret = ::close(fd);
        if (ret < 0) {
            throw FileCloseError{};
        }
    }

  public:
    File() = default;

    File(std::string path, FileMode mode) : path(std::move(path)) {
        if (mode == READ and !check_file_exists()) {
            throw FileNotExistsError{};
        }
        open(mode);
        determine_size();
    }

    File& operator=(File&& other) noexcept {
        path = std::move(other.path);
        fd = other.fd;
        size_in_bytes = other.size_in_bytes;
        offset_begin = other.offset_begin;
        offset_end = other.offset_end;
        other.fd = -1;
        return *this;
    }

    ~File() {
        if (fd > 0) {
            close();
        }
    }

    void set_offset(std::size_t begin, std::size_t end) {
        offset_begin = begin;
        offset_end = end;
    }

    [[nodiscard]] int get_file_descriptor() const { return fd; }

    [[nodiscard]] std::size_t get_offset_begin() const { return offset_begin; }

    [[nodiscard]] std::size_t get_size() const { return offset_end - offset_begin; }

    [[nodiscard]] std::size_t get_total_size() const { return size_in_bytes; }
};