#pragma once

#include <cassert>
#include <liburing.h>

#include "connection.h"
#include "core/page.h"
#include "exceptions/exceptions_io_uring.h"

static std::atomic<std::size_t> pages_sent{0u};
static std::atomic<std::size_t> tuples_sent{0u};
static std::atomic<std::size_t> bytes_sent{0u};

static constexpr auto num_pages_per_node = 10u;

static constexpr auto network_io_depth = defaults::network_io_depth;

enum TrafficDirection : bool { INGRESS, EGRESS };

// traffic manager
struct Traffic {
    const Connection& conn;
    io_uring& ring;
    std::vector<std::array<std::byte, num_pages_per_node * defaults::network_page_size>> comm_buffers;
    std::vector<std::size_t> comm_buffer_sizes;
    std::vector<std::size_t> comm_current_page;
    std::array<io_uring_cqe*, network_io_depth> cqes{};
    uint64_t in_flight{0};

    explicit Traffic(const Connection& conn, io_uring& ring) : conn(conn), ring(ring) {}

    void init() {
        comm_buffers.resize(conn.num_connections);
        comm_buffer_sizes.resize(conn.num_connections, 1);
        comm_current_page.resize(conn.num_connections, 0);
    }

    int register_socket_fds(io_uring* ring_ptr, std::size_t offset) {
        // TODO if offset > 0
        int ret;
        if ((ret = io_uring_register_files(ring_ptr, conn.socket_fds.data(), conn.num_connections)) < 0) {
            throw IOUringRegisterFilesError{ret};
        }
        assert(ret == 0);
        return ret;
    }

    template <typename PageOnBuffer>
    auto get_page(std::size_t destination) {
        auto current_page = get_current_page<PageOnBuffer>(destination);
        if (current_page->full()) {
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
        if (comm_buffer_sizes[destination] == num_pages_per_node) {
            unsigned num_cqes_peeked;
            while ((num_cqes_peeked = io_uring_peek_batch_cqe(&ring, cqes.begin(), network_io_depth)) == 0)
                ;
            comm_current_page[destination] = cqes[0]->user_data;
            for (auto i = 0u; i < num_cqes_peeked; ++i) {
                auto page_idx = cqes[i]->user_data;
                if ((reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + page_idx)->num_tuples >
                    (reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + page_idx)
                        ->max_tuples_per_page) {
                    print("Sent",
                          (reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + page_idx)->num_tuples);
                }
                (reinterpret_cast<PageOnBuffer*>(comm_buffers[destination].begin()) + page_idx)->num_tuples = 0;
                bytes_sent += cqes[i]->res;
                if (cqes[i]->res < 0) {
                    throw NetworkSendError{cqes[i]->res};
                }
                io_uring_cq_advance(&ring, 1);
            }
            in_flight -= num_cqes_peeked;
            comm_buffer_sizes[destination] -= (num_cqes_peeked - 1);
        } else {
            do {
                comm_current_page[destination] = (comm_current_page[destination] + 1) % num_pages_per_node;
            } while (get_current_page<PageOnBuffer>(destination)->full());
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
        if (current_page->num_tuples > current_page->max_tuples_per_page) {
            print("Sent", current_page->num_tuples);
        }
        io_uring_prep_send(sqe, destination, current_page, defaults::network_page_size, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        sqe->user_data = comm_current_page[destination];
        pages_sent++;
        tuples_sent += current_page->num_tuples;
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        in_flight++;
    }

    template <typename PageOnBuffer>
    void flush_all() {
        // check for non-free pages with tuples > 0
        for (auto i = 0; i < comm_buffers.size(); ++i) {
            auto page = get_current_page<PageOnBuffer>(i);
            if (!page->empty()) {
                flush_current_page(i, page);
            }
        }
        // wait for inflight
        while (in_flight > 0) {
            io_uring_peek_cqe(&ring, cqes.data());
            assert(cqes[0]->res == defaults::network_page_size);
            bytes_sent += cqes[0]->res;
            io_uring_cqe_seen(&ring, cqes[0]);
            in_flight -= 1;
        }
    }
};

// Manages ingress and egress traffic via a single io_uring instance
struct NetworkManagerOld {
    io_uring ring{};
    //    Traffic<INGRESS> traffic_ingress;
    Traffic traffic;

    //    NetworkManagerOld(const Connection& conn_ingress, const Connection& conn_egress)
    //        : ring{}, traffic_ingress(conn_ingress, ring), traffic(conn_egress, ring) {
    //        int ret;
    //        if ((ret = io_uring_queue_init(network_io_depth, &ring, 0))) {
    //            throw IOUringInitError{ret};
    //        }
    //        int num_registered{0};
    //        traffic_ingress.init();
    //        num_registered = traffic_ingress.register_socket_fds(&ring, 0);
    //        traffic.init();
    //        traffic.register_socket_fds(&ring, num_registered);
    //    }

    explicit NetworkManagerOld(const Connection& conn) : ring{}, traffic(conn, ring) {
        int ret;
        if ((ret = io_uring_queue_init(network_io_depth, &ring, 0))) {
            throw IOUringInitError{ret};
        }
        traffic.init();
        traffic.register_socket_fds(&ring, 0);
    }
};
