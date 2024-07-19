#pragma once

#include <cassert>
#include <liburing.h>

#include "exceptions/exceptions_io_uring.h"
#include "storage/page.h"

static constexpr auto num_pages_per_receiver = 10u;

static constexpr auto network_io_depth = defaults::network_io_depth;

class NetworkManager {
    // network manager
  private:
    std::vector<std::array<std::byte, num_pages_per_receiver * defaults::network_page_size>> comm_buffers;
    std::vector<std::size_t> comm_buffer_sizes;
    std::vector<std::size_t> comm_current_page;
    const std::vector<int>& socket_fds;
    io_uring ring{};
    std::array<io_uring_cqe*, network_io_depth> cqes{};
    uint64_t in_flight{0};

  public:
    NetworkManager(std::integral auto num_receivers, const Connection& connection)
        : comm_buffers(num_receivers), comm_buffer_sizes(num_receivers, 1), comm_current_page(num_receivers, 0),
          socket_fds(connection.get_socket_fds()) {
        if (io_uring_queue_init(network_io_depth, &ring, 0) != 0) {
            throw IOUringInitError{};
        }
    }

    template <typename PageOnBuffer>
    auto get_page(std::size_t destination) {
        auto current_page = get_current_page<PageOnBuffer>(destination);
        if (current_page->is_full()) {
            flush_current_page(destination, current_page);
            return get_new_page<PageOnBuffer>(destination);
        }
        return current_page;
    }

    template <typename PageOnBuffer>
    auto get_current_page(std::size_t destination) {
        return reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + comm_current_page[destination];
    }

    template <typename PageOnBuffer>
    auto get_new_page(std::size_t destination) {
        if (comm_buffer_sizes[destination] == num_pages_per_receiver) {
            unsigned num_cqes_peeked;
            while ((num_cqes_peeked = io_uring_peek_batch_cqe(&ring, cqes.begin(), network_io_depth)) == 0)
                ;
            comm_current_page[destination] = cqes[0]->user_data;
            for (auto i = 0u; i < num_cqes_peeked; ++i) {
                auto page_idx = cqes[i]->user_data;
                (reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + page_idx)->num_tuples = 0;
                io_uring_cq_advance(&ring, 1);
            }
            in_flight -= num_cqes_peeked;
            comm_buffer_sizes[destination] -= (num_cqes_peeked - 1);
        } else {
            do {
                comm_current_page[destination] = (comm_current_page[destination] + 1) % num_pages_per_receiver;
            } while (get_current_page<PageOnBuffer>(destination)->is_full());
            comm_buffer_sizes[destination]++;
        }
        return reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + comm_current_page[destination];
    }

    template <typename PageOnBuffer>
    void flush_current_page(std::size_t destination, PageOnBuffer* current_page) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        // compress page before sending?
        io_uring_prep_send(sqe, socket_fds[destination], current_page, defaults::network_page_size, 0);
        sqe->user_data = comm_current_page[destination];
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        in_flight++;
    }

    template <typename PageOnBuffer>
    void flush_all() {
        // check for non-free pages with tuples > 0
        for (auto i = 0; i < comm_buffers.size(); ++i) {
            auto page = get_current_page<PageOnBuffer>(i);
            if (!page->is_empty()) {
                flush_current_page(i, page);
            }
        }
        // wait for inflight
        while (in_flight > 0) {
            auto num_cqes_peeked = io_uring_peek_cqe(&ring, cqes.begin());
            io_uring_cq_advance(&ring, num_cqes_peeked);
            in_flight -= num_cqes_peeked;
        }
    }
};
