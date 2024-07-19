#pragma once

#include <fcntl.h>
#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>

#include "exceptions/exceptions_file.h"
#include "page.h"

enum FileMode : int8_t { READ, WRITE, READ_WRITE };

class File {
  private:
    std::string path;
    std::size_t size_in_bytes{0};
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
        struct stat fileStat {};
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
    File(const std::string& path, FileMode mode) : path(path) {
        if (mode == READ && !check_file_exists()) {
            throw FileNotExistsError{};
        }
        open(mode);
        determine_size();
    }

    File(File&& other) noexcept {
        path = std::move(other.path);
        fd = other.fd;
        size_in_bytes = other.size_in_bytes;
        other.fd = -1;
    }

    ~File() {
        if (fd > 0) {
            close();
        }
    }

    [[nodiscard]] int get_file_descriptor() const { return fd; }

    [[nodiscard]] std::size_t get_size_in_bytes() const { return size_in_bytes; }
};