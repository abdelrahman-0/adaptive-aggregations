#pragma once

#include <string>
#include <filesystem>
#include <utility>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "exceptions/exceptions.h"

enum FileMode : int8_t {
    READ, WRITE, READ_WRITE
};

class File {
private:
    std::string path;
    std::size_t size_in_bytes{0};
    int fd{-1};

    [[nodiscard]] bool check_file_exists() const {
        return std::filesystem::exists({path});
    }

    void open(FileMode mode) {
        switch (mode) {
            case READ: {
                fd = ::open(path.c_str(), O_RDONLY | O_NOATIME | O_DIRECT);
                break;
            }
            case WRITE: {
                fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0600);
                break;
            }
            case READ_WRITE:
                break;
        }
        if (fd < 0) {
            throw FileOpenError{};
        }
    }

    void close() {

    }

public:
    File(std::string path, FileMode mode) : path(std::move(path)) {
        if(!check_file_exists()) {
            throw FileNotExistsError{};
        }
        open(mode);
    }

    ~File() {
        close();
    }

};