#pragma once

#include <cassert>
#include <liburing.h>

#include "common/page.h"
#include "connection.h"
#include "exceptions/exceptions_io_uring.h"

// Manages either ingress or egress traffic via a single uring instance
template <custom_concepts::is_communication_page BufferPage>
class NetworkManager {
  protected:
    io_uring ring{};
    std::vector<BufferPage> buffers{};
    std::vector<uint32_t> free_pages;

    uint32_t nwdepth;

    void init_ring(bool sqpoll) {
        int ret;
        if ((ret = io_uring_queue_init(next_power_of_2(nwdepth), &ring, sqpoll ? IORING_SETUP_SQPOLL : 0)) < 0) {
            throw IOUringInitError{ret};
        }
    }

    void register_sockets(const std::vector<int>& sockets) {
        int ret;
        if ((ret = io_uring_register_files(&ring, sockets.data(), sockets.size())) < 0) {
            throw IOUringRegisterFilesError{ret};
        }
    }

    void register_buffers() {
        int ret;
        std::vector<::iovec> io_vecs;
        for (auto& buf : buffers) {
            io_vecs.emplace_back(&buf, sizeof(BufferPage));
        }
        if ((ret = io_uring_register_buffers(&ring, io_vecs.data(), io_vecs.size())) < 0) {
            throw IOUringRegisterBuffersError{ret};
        }
    }

    void init_buffers() {
        //        register_buffers();
        std::iota(free_pages.rbegin(), free_pages.rend(), 0u);
    }

  public:
    explicit NetworkManager(uint32_t nwdepth, uint32_t nbuffers, bool sqpoll, const std::vector<int>& sockets)
        : nwdepth(nwdepth), buffers(nbuffers), free_pages(nbuffers) {
        init_ring(sqpoll);
        if (not sockets.empty()) {
            register_sockets(sockets);
        }
        init_buffers();
    }
};

template <custom_concepts::is_communication_page BufferPage>
class IngressNetworkManager : public NetworkManager<BufferPage> {
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    using NetworkManager<BufferPage>::free_pages;
    std::vector<io_uring_cqe*> cqes;
    uint32_t cqes_processed{0};
    uint32_t cqes_peeked{0};
    uint64_t page_recv{0};

  public:
    IngressNetworkManager(uint32_t npeers, uint32_t nwdepth, uint32_t nbuffers, bool sqpoll,
                          const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers, sqpoll, sockets), cqes(nwdepth * 2) {}

    void post_recvs(u16 dst) {
        auto* sqe = io_uring_get_sqe(&ring);
        auto buffer_idx = free_pages.back();
        free_pages.pop_back();
        // fixed buffers?
        assert(sizeof(BufferPage) == defaults::network_page_size);
        io_uring_prep_recv(sqe, dst, buffers.data() + buffer_idx, sizeof(BufferPage), MSG_WAITALL);
        auto* user_data = buffers.data() + buffer_idx;
        io_uring_sqe_set_data(sqe, tag_pointer(user_data, dst));
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_submit(&ring);
    }

    std::tuple<BufferPage*, u16> get_page() {
        if (cqes_processed == cqes_peeked) {
            cqes_processed = 0;
            cqes_peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
            if (cqes_peeked == 0) {
                return {nullptr, static_cast<u16>(-1)};
            }
            for (auto i{0u}; i < cqes_peeked; ++i) {
                if (cqes[i]->res != sizeof(BufferPage)) {
                    throw IOUringRecvError(cqes[i]->res);
                }
            }
        }
        page_recv++;
        auto user_data = io_uring_cqe_get_data(cqes[cqes_processed++]);
        return {get_pointer<BufferPage>(user_data), get_tag(user_data)};
    }

    void done_page(BufferPage* page) {
        free_pages.push_back(page - buffers.data());
        io_uring_cq_advance(&ring, 1);
    }
};

template <custom_concepts::is_communication_page BufferPage>
class MultishotIngressNetworkManager : public NetworkManager<BufferPage> {
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    using NetworkManager<BufferPage>::free_pages;
    std::vector<io_uring_cqe*> cqes;
    uint32_t cqes_processed{0};
    uint32_t cqes_peeked{0};
    uint64_t page_recv{0};

  public:
    MultishotIngressNetworkManager(uint32_t npeers, uint32_t nwdepth, uint32_t nbuffers, bool sqpoll,
                          const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers, sqpoll, sockets), cqes(nwdepth * 2) {}

    void post_recvs(u16 dst) {
        // TODO add registered buffers
        // TODO add recv mshot
        auto* sqe = io_uring_get_sqe(&ring);
        auto buffer_idx = free_pages.back();
        free_pages.pop_back();
        // fixed buffers?
        assert(sizeof(BufferPage) == defaults::network_page_size);
        io_uring_prep_recv(sqe, dst, buffers.data() + buffer_idx, sizeof(BufferPage), MSG_WAITALL);
        auto* user_data = buffers.data() + buffer_idx;
        io_uring_sqe_set_data(sqe, tag_pointer(user_data, dst));
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_submit(&ring);
    }

    std::tuple<BufferPage*, u16> get_page() {
        // TODO get tuples -> returns (begin, end , buff_idx, peer)
        // TODO copy pages
        if (cqes_processed == cqes_peeked) {
            cqes_processed = 0;
            cqes_peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
            if (cqes_peeked == 0) {
                return {nullptr, static_cast<u16>(-1)};
            }
            for (auto i{0u}; i < cqes_peeked; ++i) {
                if (cqes[i]->res != sizeof(BufferPage)) {
                    throw IOUringRecvError(cqes[i]->res);
                }
            }
        }
        page_recv++;
        auto user_data = io_uring_cqe_get_data(cqes[cqes_processed++]);
        return {get_pointer<BufferPage>(user_data), get_tag(user_data)};
    }

    void done_page(BufferPage* page) {
        // advance buffers cqe and re-register buffer
        free_pages.push_back(page - buffers.data());
        io_uring_cq_advance(&ring, 1);
    }
};

template <custom_concepts::is_communication_page BufferPage>
class EgressNetworkManager : public NetworkManager<BufferPage> {
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    using NetworkManager<BufferPage>::free_pages;
    std::vector<BufferPage*> active_buffer;
    io_uring_cqe* cqe{nullptr};
    uint32_t inflight_egress{0};
    uint32_t total_submitted{0};
    uint32_t total_retrieved{0};

  public:
    EgressNetworkManager(uint32_t npeers, uint32_t nwdepth, uint32_t nbuffers, bool sqpoll,
                         const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers, sqpoll, sockets), active_buffer(npeers, nullptr) {
        for (auto peer{0u}; peer < npeers; ++peer) {
            get_new_page(peer);
        }
    }

    [[maybe_unused]] BufferPage* get_new_page(uint32_t dst) {
        BufferPage* result{nullptr};
        if (free_pages.empty()) {
            int res = io_uring_wait_cqe(&ring, &cqe);
            if (res) {
                throw IOUringError{res};
            }
            if (cqe->res != sizeof(BufferPage)) {
                throw IOUringSendError{cqe->res};
            }
            result = get_pointer<BufferPage>(io_uring_cqe_get_data(cqe));
            io_uring_cq_advance(&ring, 1);
            result->clear_tuples();
            inflight_egress--;
            total_retrieved++;
        } else {
            // get a free page
            uint32_t page_idx = free_pages.back();
            free_pages.pop_back();
            result = buffers.data() + page_idx;
        }
        active_buffer[dst] = result;
        return result;
    }

    BufferPage* get_page(uint32_t dst) {
        if (not active_buffer[dst]->full()) {
            return active_buffer[dst];
        }
        flush(dst);
        return get_new_page(dst);
    }

    void flush(uint32_t dst) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        io_uring_prep_send(sqe, dst, active_buffer[dst], sizeof(BufferPage), MSG_WAITALL);
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_sqe_set_data(sqe, tag_pointer(active_buffer[dst], dst));
        inflight_egress++;
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        total_submitted++;
    }

    void flush_all() {
        // loop through peers and flush
        for (auto peer{0u}; peer < active_buffer.size(); ++peer) {
            active_buffer[peer]->set_last_page();
            flush(peer);
        }
        println("total pages sent", total_submitted);
    }

    void wait_all() {
        // wait for all inflight sends
        int ret;
        for (auto i{0u}; i < inflight_egress; ++i) {
            if ((ret = io_uring_wait_cqe(&ring, &cqe)) < 0) {
                throw IOUringWaitError(ret);
            }
            if (cqe->res != sizeof(BufferPage)) {
                throw IOUringSendError{cqe->res};
            }
            io_uring_cq_advance(&ring, 1);
        }
        inflight_egress = 0;
    }
};
