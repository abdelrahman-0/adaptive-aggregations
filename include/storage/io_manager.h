#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <liburing.h>

#include "cache.h"
#include "defaults.h"
#include "exceptions/exceptions_io_uring.h"
#include "page.h"

static constexpr std::size_t local_io_depth = defaults::local_io_depth;

class IO_Manager {
  private:
    io_uring ring{};
    std::array<io_uring_cqe*, local_io_depth> cqes{};
    uint64_t in_flight{0};
    uint64_t cqes_index{0};
    uint64_t cqes_peeked{0};

  public:
    explicit IO_Manager() {
        auto res = io_uring_queue_init(local_io_depth, &ring, 0);
        if (res != 0) {
            throw IOUringInitError{};
        }
    }

    ~IO_Manager() { io_uring_queue_exit(&ring); }

    template <std::size_t N>
    void register_file(const std::array<int, N>& fd) {
        if (io_uring_register_files(&ring, &fd, N) != 0) {
            throw IOUringRegisterFileError{};
        }
    }

    auto peek() { return io_uring_peek_batch_cqe(&ring, cqes.begin(), local_io_depth); }

    void seen() {
        io_uring_cq_advance(&ring, 1);
        in_flight--;
    }

    [[nodiscard]] bool has_inflight_requests() const { return in_flight > 0; }

    template <custom_concepts::is_pointer_type T>
    T get_next_cqe_data() {
        if (cqes_index == cqes_peeked) {
            cqes_peeked = peek();
            cqes_index = 0;
        }
        if (cqes_peeked) {
            seen();
            return reinterpret_cast<T>(cqes[cqes_index++]->user_data);
        } else {
            return nullptr;
        }
    }

    template <bool do_read = true>
    inline void block_io(int fd, uint64_t offset, std::byte* block, std::size_t block_size = defaults::local_page_size,
                         bool registered = false) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        if constexpr (do_read) {
            io_uring_prep_read(sqe, fd, block, block_size, offset);
        } else {
            io_uring_prep_write(sqe, fd, block, block_size, offset);
        }
        io_uring_sqe_set_data(sqe, block);
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        in_flight++;
    }
};
