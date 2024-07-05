#pragma once

#include <cstdint>
#include <liburing.h>

#include "exceptions/exceptions.h"
#include "page.h"
#include "cache.h"

class IO_Manager {
private:
    io_uring ring{};
    Cache cache{};
public:
    explicit IO_Manager(std::size_t num_entries) {
        io_uring_queue_init(num_entries, &ring, 0);
    }

    ~IO_Manager() {
        io_uring_queue_exit(&ring);
    }

    template<std::size_t N>
    void register_file(std::array<int, N> fd) {
        auto res = io_uring_register_files(&ring, &fd, N);
        if (res != 0) {
           throw RegisterFileError{};
        }
    }

    inline void read_block(int fd, uint64_t offset, std::byte* block, std::size_t block_size = page_size, bool registered = false) {
        auto* sqe = io_uring_get_sqe(&ring);
        io_uring_sqe_set_flags(sqe, registered ? IOSQE_FIXED_FILE : 0);
        io_uring_prep_read(sqe, fd, block, block_size, offset);
        io_uring_sqe_set_data(sqe, block);
        io_uring_submit(&ring);
    }
};
