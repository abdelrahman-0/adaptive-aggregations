#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <liburing.h>
#include <span>

#include "cache.h"
#include "common/page.h"
#include "defaults.h"
#include "exceptions/exceptions_io_uring.h"
#include "page_local.h"

static constexpr std::size_t local_io_depth = defaults::local_io_depth;

class IO_Manager {
  private:
    io_uring ring{};
    std::array<io_uring_cqe*, local_io_depth> cqes{};
    uint64_t inflight{0};
    uint64_t cqes_index{0};
    uint64_t cqes_peeked{0};

  public:
    explicit IO_Manager(uint32_t sqdepth, bool sqpoll) {
        int ret;
        if ((ret = io_uring_queue_init(sqdepth, &ring, sqpoll ? IORING_SETUP_SQPOLL : 0)) < 0) {
            throw IOUringInitError{ret};
        }
    }

    ~IO_Manager() { io_uring_queue_exit(&ring); }

    void register_files(const std::vector<int>& fds) {
        int ret;
        if ((ret = io_uring_register_files(&ring, fds.data(), fds.size())) < 0) {
            throw IOUringRegisterFilesError{ret};
        }
    }

    auto peek() { return io_uring_peek_batch_cqe(&ring, cqes.begin(), local_io_depth); }

    auto wait(uint32_t num_wait) { return io_uring_wait_cqe_nr(&ring, cqes.data(), num_wait); }

    void seen(uint32_t num_seen) {
        io_uring_cq_advance(&ring, num_seen);
        inflight -= num_seen;
    }

    // TODO encode segment_id in user_data
    // TODO move has_inflight_requests to Table to be able to share io_manager and load multiple tables
    [[nodiscard]] bool has_inflight_requests() const { return inflight > 0; }

    template <custom_concepts::pointer_type T>
    T get_next_cqe_data() {
        if (cqes_index == cqes_peeked) {
            cqes_peeked = peek();
            cqes_index = 0;
        }
        if (cqes_peeked) {
            seen(1);
            return reinterpret_cast<T>(cqes[cqes_index++]->user_data);
        } else {
            return nullptr;
        }
    }

    template <typename BufferPagePtr>
    requires std::is_pointer_v<BufferPagePtr>
    auto* get_next_page() {
        wait(1);
        auto* result = reinterpret_cast<BufferPagePtr>(cqes[0]->user_data);
        seen(1);
        return result;
    }

    template <FileMode mode, typename... Attributes>
    inline void sync_io(int fd, uint64_t offset, PageLocal<Attributes...>& block,
                        std::size_t block_size = defaults::local_page_size) {
        async_io<mode>(fd, offset, block, block_size);
        wait(1);
        seen(1);
    }

    template <FileMode mode, typename... Attributes>
    inline void async_io(int fd, uint64_t offset, PageLocal<Attributes...>& block,
                         std::size_t block_size = defaults::local_page_size, bool registered = false) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        if constexpr (mode == FileMode::READ) {
            io_uring_prep_read(sqe, fd, &block, block_size, offset);
        } else {
            io_uring_prep_write(sqe, fd, &block, block_size, offset);
        }
        io_uring_sqe_set_data(sqe, &block);
        io_uring_sqe_set_flags(sqe, registered ? IOSQE_FIXED_FILE : 0);
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        inflight++;
    }

    // TODO registered buffers
    template <FileMode mode, typename... Attributes>
    inline void batch_async_io(int fd, const std::span<Swip> swips, std::vector<PageLocal<Attributes...>>& blocks,
                               bool registered = false) {
        for (auto i{0u}; i < swips.size(); ++i) {
            auto* sqe = io_uring_get_sqe(&ring);
            if (sqe == nullptr) {
                throw IOUringSubmissionQueueFullError{};
            }
            if constexpr (mode == FileMode::READ) {
                io_uring_prep_read(sqe, fd, blocks.data() + i, sizeof(PageLocal<Attributes...>),
                                   swips[i].get_page_index() * sizeof(PageLocal<Attributes...>));
            } else {
                io_uring_prep_write(sqe, fd, blocks.data() + i, sizeof(PageLocal<Attributes...>),
                                    swips[i].get_page_index() * sizeof(PageLocal<Attributes...>));
            }
            io_uring_sqe_set_data(sqe, &blocks[i]);
            io_uring_sqe_set_flags(sqe, registered ? IOSQE_FIXED_FILE : 0);
        }
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == swips.size());
        inflight += swips.size();
    }
};
