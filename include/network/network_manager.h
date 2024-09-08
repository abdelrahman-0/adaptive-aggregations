#pragma once

#include <cassert>
#include <deque>
#include <liburing.h>
#include <queue>

#include "common/page.h"
#include "connection.h"
#include "exceptions/exceptions_io_uring.h"

// Manages either ingress or egress traffic via a single uring instance
template <custom_concepts::is_communication_page BufferPage>
class NetworkManager {
  protected:
    io_uring ring{};
    std::vector<BufferPage> buffers{};
    std::vector<u32> free_pages;

    u32 nwdepth;

    void init_ring(bool sqpoll) {
        int ret;
        if ((ret = io_uring_queue_init(next_power_of_2(nwdepth), &ring,
                                       IORING_SETUP_SINGLE_ISSUER | (sqpoll ? IORING_SETUP_SQPOLL : 0))) < 0) {
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

    void init_buffers(bool register_bufs) {
        if (register_bufs) {
            register_buffers();
        }
        std::iota(free_pages.rbegin(), free_pages.rend(), 0u);
    }

  public:
    explicit NetworkManager(u32 nwdepth, u32 nbuffers, bool sqpoll, const std::vector<int>& sockets,
                            bool register_bufs = false)
        : nwdepth(nwdepth), buffers(nbuffers), free_pages(nbuffers) {
        init_ring(sqpoll);
        if (not sockets.empty()) {
            register_sockets(sockets);
        }
        init_buffers(register_bufs);
    }
};

template <custom_concepts::is_communication_page BufferPage>
class IngressNetworkManager : public NetworkManager<BufferPage> {
  private:
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    using NetworkManager<BufferPage>::free_pages;
    std::vector<io_uring_cqe*> cqes;
    u32 cqes_processed{0};
    u32 cqes_peeked{0};
    u64 pages_recv{0};

  public:
    IngressNetworkManager(u32 npeers, u32 nwdepth, u32 nbuffers, bool sqpoll, const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers, sqpoll, sockets), cqes(nwdepth * 2) {}

    auto get_pages_recv() const { return pages_recv; }

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
                if (get_pointer<BufferPage>(io_uring_cqe_get_data(cqes[i]))->get_num_tuples() >
                    BufferPage::max_num_tuples_per_page) {
                    hexdump(get_pointer<BufferPage>(io_uring_cqe_get_data(cqes[i])), sizeof(BufferPage));
                    throw std::runtime_error{"received fragmented page!"};
                }
            }
        }
        pages_recv++;
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
  private:
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    std::vector<io_uring_buf_ring*> buf_rings;
    std::vector<io_uring_cqe*> cqes;
    u64 page_recv{0};
    u32 cqes_processed{0};
    u32 cqes_peeked{0};
    u32 nbuffers_per_peer;

    void setup_buf_rings(u32 nbuffers) {
        assert(nbuffers == next_power_of_2(nbuffers));
        int ret;
        for (auto i{0u}; i < buf_rings.size(); ++i) {
            auto* buf_ring = io_uring_setup_buf_ring(&ring, nbuffers * 2, i, 0, &ret);
            if (not buf_ring) {
                throw IOUringSetupBufRingError{ret};
            }
            for (auto j{0u}; j < nbuffers; j++) {
                io_uring_buf_ring_add(buf_ring, buffers.data() + i * nbuffers + j, sizeof(BufferPage), j,
                                      io_uring_buf_ring_mask(nbuffers), j);
            }
            io_uring_buf_ring_advance(buf_ring, nbuffers);
            buf_rings[i] = buf_ring;
        }
    }

  public:
    MultishotIngressNetworkManager(u32 npeers, u32 nwdepth, u32 nbuffers, bool sqpoll, const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers * npeers, sqpoll, sockets), cqes(nwdepth * 2), buf_rings(npeers),
          nbuffers_per_peer(nbuffers) {
        setup_buf_rings(nbuffers);
    }

    void post_recvs(u16 dst) {
        // prepare multi-shot
        auto sqe = io_uring_get_sqe(&ring);
        io_uring_prep_recv_multishot(sqe, dst, nullptr, 0, 0);
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT;
        sqe->buf_group = dst;
        sqe->user_data = dst;
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
    }

    std::tuple<void*, void*, u16, u16> get_page() {
        // TODO handle short recvs with copy
        auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), 1);
        if (peeked == 0) {
            return {nullptr, nullptr, 0, 0};
        }
        auto buf_idx = cqes[0]->flags >> IORING_CQE_BUFFER_SHIFT;
        auto* begin = reinterpret_cast<std::byte*>(buffers.data() + buf_idx);
        auto* end = reinterpret_cast<std::byte*>(buffers.data() + buf_idx) + cqes[0]->res;
        println("Got", cqes[0]->res, "bytes");
        if (cqes[0]->res < 0) {
            throw IOUringMultiShotRecvError{cqes[0]->res};
        }
        println("user data", cqes[0]->user_data);
        assert(cqes[0]->flags & IORING_CQE_F_MORE);

        return {begin, end, buf_idx, cqes[0]->user_data};
    }

    void done_page(u16 dst, u16 buf_idx) {
        // advance buffers cqe and re-register buffer
        io_uring_buf_ring_add(buf_rings[dst], buffers.data() + buf_idx, sizeof(BufferPage), buf_idx,
                              io_uring_buf_ring_mask(nbuffers_per_peer), 0);
        io_uring_buf_ring_cq_advance(&ring, buf_rings[dst], 1);
    }
};

template <custom_concepts::is_communication_page BufferPage>
class SimpleEgressNetworkManager : public NetworkManager<BufferPage> {
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    using NetworkManager<BufferPage>::free_pages;
    std::vector<BufferPage*> active_buffer;
    io_uring_cqe* cqe{nullptr};
    u32 inflight_egress{0};
    u32 total_submitted{0};
    u32 total_retrieved{0};

  public:
    SimpleEgressNetworkManager(u32 npeers, u32 nwdepth, u32 nbuffers, bool sqpoll, const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers, sqpoll, sockets), active_buffer(npeers, nullptr) {
        for (auto peer{0u}; peer < npeers; ++peer) {
            get_new_page(peer);
        }
    }

    [[maybe_unused]] BufferPage* get_new_page(u32 dst) {
        BufferPage* result{nullptr};
        if (free_pages.empty()) {
            int res = io_uring_wait_cqe(&ring, &cqe);
            if (res) {
                throw IOUringError{res};
            }
            if (cqe->res != sizeof(BufferPage)) {
                throw IOUringSendError{cqe->res};
            }
            result = reinterpret_cast<BufferPage*>(io_uring_cqe_get_data(cqe));
            io_uring_cq_advance(&ring, 1);
            result->clear_tuples();
            inflight_egress--;
            total_retrieved++;
        } else {
            // get a free page
            u32 page_idx = free_pages.back();
            free_pages.pop_back();
            result = buffers.data() + page_idx;
        }
        active_buffer[dst] = result;
        return result;
    }

    BufferPage* get_page(u32 dst) {
        if (not active_buffer[dst]->full()) {
            return active_buffer[dst];
        }
        flush(dst);
        return get_new_page(dst);
    }

    void flush(u32 dst) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        io_uring_prep_send(sqe, dst, active_buffer[dst], sizeof(BufferPage), 0);
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_sqe_set_data(sqe, active_buffer[dst]);
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
    }

    void try_drain_pending() const {}

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

template <custom_concepts::is_communication_page BufferPage>
class EgressNetworkManager : public NetworkManager<BufferPage> {
    // needed for dependent lookups
    using NetworkManager<BufferPage>::ring;
    using NetworkManager<BufferPage>::buffers;
    using NetworkManager<BufferPage>::free_pages;
    std::vector<std::queue<BufferPage*>> pending_pages;
    std::vector<bool> has_inflight;
    std::vector<BufferPage*> active_buffer;
    io_uring_cqe* cqe{nullptr};
    std::vector<io_uring_cqe*> cqes;
    u32 inflight_egress{0};
    u32 total_submitted{0};
    u32 total_retrieved{0};

  public:
    EgressNetworkManager(u32 npeers, u32 nwdepth, u32 nbuffers, bool sqpoll, const std::vector<int>& sockets)
        : NetworkManager<BufferPage>(nwdepth, nbuffers, sqpoll, sockets), active_buffer(npeers, nullptr),
          pending_pages(npeers), has_inflight(npeers, false), cqes(nwdepth * 2) {
        for (auto peer{0u}; peer < npeers; ++peer) {
            get_new_page(peer);
        }
    }

    [[maybe_unused]] BufferPage* get_new_page(u32 dst) {
        BufferPage* result{nullptr};
        if (free_pages.empty()) {
            int res = io_uring_wait_cqe(&ring, &cqe);
            if (res) {
                throw IOUringError{res};
            }
            if (cqe->res != sizeof(BufferPage)) {
                throw IOUringSendError{cqe->res};
            }
            auto user_data = io_uring_cqe_get_data(cqe);
            result = get_pointer<BufferPage>(user_data);
            io_uring_cq_advance(&ring, 1);
            auto peeked_dst = get_tag(user_data);
            if (pending_pages[peeked_dst].empty()) {
                has_inflight[peeked_dst] = false;
            } else {
                _flush(peeked_dst, pending_pages[peeked_dst].front());
                pending_pages[peeked_dst].pop();
            }
            result->clear_tuples();
            inflight_egress--;
            total_retrieved++;
        } else {
            // get a free page
            u32 page_idx = free_pages.back();
            free_pages.pop_back();
            result = buffers.data() + page_idx;
        }
        active_buffer[dst] = result;
        return result;
    }

    BufferPage* get_page(u32 dst) {
        if (not active_buffer[dst]->full()) {
            return active_buffer[dst];
        }
        flush(dst);
        return get_new_page(dst);
    }

    void _flush(u16 dst, BufferPage* page) {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        io_uring_prep_send(sqe, dst, page, sizeof(BufferPage), 0);
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_sqe_set_data(sqe, tag_pointer(page, dst));
        inflight_egress++;
        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        total_submitted++;
    }

    void flush(u16 dst) {
        if (has_inflight[dst]) {
            pending_pages[dst].push(active_buffer[dst]);
        } else {
            has_inflight[dst] = true;
            if (pending_pages[dst].empty()) {
                _flush(dst, active_buffer[dst]);
            } else {
                // flush first page in FIFO queue and current page to queue
                _flush(dst, pending_pages[dst].front());
                pending_pages[dst].pop();
                pending_pages[dst].push(active_buffer[dst]);
            }
        }
    }

    void flush_all() {
        // loop through peers and flush
        for (auto peer{0u}; peer < active_buffer.size(); ++peer) {
            active_buffer[peer]->set_last_page();
            flush(peer);
        }
    }

    void try_drain_pending() {
        auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
        for (auto i{0u}; i < peeked; ++i) {
            auto peeked_dst = get_tag(io_uring_cqe_get_data(cqes[i]));
            if (not pending_pages[peeked_dst].empty()) {
                _flush(peeked_dst, pending_pages[peeked_dst].front());
                pending_pages[peeked_dst].pop();
            }
        }
        io_uring_cq_advance(&ring, peeked);
        inflight_egress -= peeked;
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
            auto dst = get_tag(io_uring_cqe_get_data(cqe));
            if (not pending_pages[dst].empty()) {
                _flush(dst, pending_pages[dst].front());
                pending_pages[dst].pop();
            }
        }
        inflight_egress = 0;
    }
};
