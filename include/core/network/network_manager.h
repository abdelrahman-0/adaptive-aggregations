#pragma once

#include <liburing.h>
#include <vector>

#include "defaults.h"
#include "misc/exceptions/exceptions_io_uring.h"
#include "utils/utils.h"

namespace network {

// tagged pointer with two 8-bit tags
// TODO 3 tags for heterogeneous
class UserData {
  private:
    static constexpr u64 pointer_tag_mask = 0x0000FFFFFFFFFFFF;
    static constexpr u64 bottom_tag_mask = 0x00FF000000000000;
    static constexpr u64 top_tag_mask = bottom_tag_mask << 8;
    static constexpr u8 tag_shift = 48;
    static constexpr u8 top_tag_shift = tag_shift + 8;

    uintptr_t data;

    [[nodiscard]]
    auto get_tag() const
    {
        return data >> tag_shift;
    }

    static inline auto tag_pointer(auto* ptr, std::integral auto top_tag, std::integral auto bottom_tag = 0)
    {
        return reinterpret_cast<uintptr_t>(ptr) | (static_cast<u64>(bottom_tag) << tag_shift) | (static_cast<u64>(top_tag) << top_tag_shift);
    }

  public:
    UserData() = default;

    explicit UserData(auto* ptr) : data(reinterpret_cast<uintptr_t>(ptr))
    {
    }

    explicit UserData(auto* ptr, std::integral auto tag) : data(tag_pointer(ptr, tag))
    {
    }

    explicit UserData(auto* ptr, std::integral auto top_tag, std::integral auto bottom_tag) : data(tag_pointer(ptr, top_tag, bottom_tag))
    {
    }

    [[nodiscard]]
    auto as_data() const
    {
        return reinterpret_cast<void*>(data);
    }

    template <typename T = void>
    [[nodiscard]]
    auto get_pointer() const
    {
        return reinterpret_cast<T*>(data & pointer_tag_mask);
    }

    [[nodiscard]]
    u8 get_bottom_tag() const
    {
        return (data & bottom_tag_mask) >> tag_shift;
    }

    [[nodiscard]]
    u8 get_top_tag() const
    {
        return (data & top_tag_mask) >> top_tag_shift;
    }
};

static_assert(sizeof(UserData) == 8);

// base class for sending/receiving fixed-size objects of unique types
template <typename... object_ts>
class BaseNetworkManager {

  protected:
    static constexpr auto nobjects = sizeof...(object_ts);
    io_uring ring{};
    std::vector<io_uring_cqe*> cqes;
    // access object sizes at runtime
    std::array<u64, nobjects> object_sizes{sizeof(object_ts)...};
    u32 nwdepth;
    DEBUGGING(u64 pages_submitted{0});

    template <u16 idx>
    using arg_t = std::tuple_element_t<idx, std::tuple<object_ts...>>;

    template <typename T, u16 idx = 0>
    constexpr u16 get_type_idx()
    {
        static_assert(idx < nobjects);
        if constexpr (std::is_same_v<std::remove_cv_t<T>, arg_t<idx>>) {
            return idx;
        }
        else {
            return get_type_idx<T, idx + 1>();
        }
    }

    void init_ring(bool sqpoll)
    {
        int ret;
        if ((ret = io_uring_queue_init(next_power_2(nwdepth), &ring, IORING_SETUP_SINGLE_ISSUER | (sqpoll ? IORING_SETUP_SQPOLL : 0))) < 0) {
            throw IOUringInitError{ret};
        }
    }

    void register_sockets(const std::vector<int>& sockets)
    {
        int ret;
        if ((ret = io_uring_register_files(&ring, sockets.data(), sockets.size())) < 0) {
            throw IOUringRegisterFilesError{ret};
        }
    }

    explicit BaseNetworkManager(u32 nwdepth, bool sqpoll, const std::vector<int>& sockets, bool register_bufs = false) : nwdepth(nwdepth), cqes(nwdepth * 2)
    {
        init_ring(sqpoll);
        if (not sockets.empty()) {
            register_sockets(sockets);
        }
    }
};

// class for sending fixed-size objects of unique types
template <bool is_heterogeneous, typename... object_ts>
class EgressNetworkManager : public BaseNetworkManager<object_ts...> {
    using base_t = BaseNetworkManager<object_ts...>;
    using base_t::cqes;
    using base_t::nobjects;
    using base_t::object_sizes;
    using base_t::ring;

  protected:
    std::vector<std::conditional_t<is_heterogeneous, tbb::concurrent_queue<UserData>, std::queue<UserData>>> pending_objects;
    std::vector<bool> has_inflight;
    std::vector<std::function<void(void*)>> consumer_fns;
    u32 inflight_egress{0};
    u16 npeers;

  public:
    EgressNetworkManager(u32 npeers, u32 nwdepth, bool sqpoll, const std::vector<int>& sockets)
        : base_t(nwdepth, sqpoll, sockets), pending_objects(npeers), has_inflight(npeers, false), consumer_fns(nobjects, [](void*) {}), npeers(npeers)
    {
    }

    template <typename T>
    void register_consumer_fn(std::function<void(T*)>& fn)
    {
        // wrapper to allow void* args
        consumer_fns[base_t::template get_type_idx<T>()] = [fn](void* obj) { fn(reinterpret_cast<T*>(obj)); };
    }

    void flush_object(UserData user_data, u64 size)
    {
        auto* sqe = io_uring_get_sqe(&ring);
        if (sqe == nullptr) {
            throw IOUringSubmissionQueueFullError{};
        }
        io_uring_prep_send(sqe, user_data.get_top_tag(), user_data.get_pointer(), size, 0);
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_sqe_set_data(sqe, user_data.as_data());
        io_uring_submit(&ring);
        inflight_egress++;
    }

    void flush_object(UserData user_data)
    {
        // extract size from type
        flush_object(user_data, object_sizes[user_data.get_bottom_tag()]);
    }

    [[maybe_unused]]
    bool flush_dst(u16 dst)
    requires(is_heterogeneous)
    {
        UserData user_data{};
        if (pending_objects[dst].try_pop(user_data)) {
            flush_object(user_data);
            return true;
        }
        return false;
    }

    [[maybe_unused]]
    bool flush_dst(u16 dst)
    requires(not is_heterogeneous)
    {
        if (not pending_objects[dst].empty()) {
            flush_object(pending_objects[dst].front());
            pending_objects[dst].pop();
            return true;
        }
        return false;
    }

    void try_drain_pending()
    {
        auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
        for (u32 i{0}; i < peeked; ++i) {
            if (cqes[i]->res <= 0) {
                throw IOUringSendError{cqes[i]->res};
            }
            UserData user_data{io_uring_cqe_get_data(cqes[i])};
            u16 peeked_dst = user_data.get_top_tag();
            has_inflight[peeked_dst] = flush_dst(peeked_dst);
            consumer_fns[user_data.get_bottom_tag()](user_data.get_pointer());
        }
        io_uring_cq_advance(&ring, peeked);
        inflight_egress -= peeked;
    }

    void wait_all()
    {
        // wait for all inflight sends
        int ret;
        for (auto i{0u}; i < inflight_egress; ++i) {
            if ((ret = io_uring_wait_cqe(&ring, cqes.data())) < 0) {
                throw IOUringWaitError(ret);
            }
            UserData user_data{io_uring_cqe_get_data(cqes[0])};
            auto obj_idx = user_data.get_bottom_tag();
            if (cqes[0]->res != object_sizes[obj_idx]) {
                throw IOUringSendError{cqes[0]->res};
            }
            io_uring_cq_advance(&ring, 1);
            flush_dst(user_data.get_top_tag());
            consumer_fns[obj_idx](user_data.get_pointer());
        }
    }
};

template <typename... object_ts>
class HomogeneousEgressNetworkManager : public EgressNetworkManager<false, object_ts...> {
    using base_t = EgressNetworkManager<false, object_ts...>;
    using base_t::flush_dst;
    using base_t::flush_object;
    using base_t::has_inflight;
    using base_t::pending_objects;

  private:
  public:
    HomogeneousEgressNetworkManager(u32 npeers, u32 nwdepth, bool sqpoll, const std::vector<int>& sockets) : base_t(npeers, nwdepth, sqpoll, sockets)
    {
    }

    template <typename T>
    void send(u16 dst, T* obj)
    {
        UserData user_data{obj, dst, base_t::template get_type_idx<T>()};
        if (has_inflight[dst]) {
            pending_objects[dst].push(user_data);
        }
        else {
            has_inflight[dst] = true;
            if (flush_dst(dst)) {
                pending_objects[dst].push(user_data);
            }
            else {
                flush_object(user_data, sizeof(T));
            }
        }
    }
};

template <typename... object_ts>
class HeterogeneousEgressNetworkManager : public EgressNetworkManager<true, object_ts...> {
    using base_t = EgressNetworkManager<true, object_ts...>;
    using base_t::flush_dst;
    using base_t::has_inflight;
    using base_t::inflight_egress;
    using base_t::npeers;
    using base_t::pending_objects;

  private:
    std::atomic<bool> continue_egress{true};

  public:
    HeterogeneousEgressNetworkManager(u32 npeers, u32 nwdepth, bool sqpoll, const std::vector<int>& sockets) : base_t(npeers, nwdepth, sqpoll, sockets)
    {
    }

    template <typename T>
    void send(u16 dst, T* obj)
    {
        pending_objects[dst].push(UserData{obj, dst, base_t::template get_type_idx<T>()});
    }

    [[maybe_unused]]
    bool try_flush_all()
    {
        bool flushed_dst{false};
        for (auto dst{0u}; dst < npeers; ++dst) {
            if (not has_inflight[dst]) {
                // no short-circuit
                flushed_dst |= (has_inflight[dst] = flush_dst(dst));
            }
        }
        return flushed_dst;
    }

    void wait_all()
    {
        do {
            base_t::wait_all();
            inflight_egress = 0;
        } while (continue_egress.load() or try_flush_all());
    }

    void finished_egress()
    {
        continue_egress = false;
    }
};

// class for receiving fixed-size objects of unique types
template <bool is_heterogeneous, typename... object_ts>
class IngressNetworkManager : public BaseNetworkManager<object_ts...> {
    using base_t = BaseNetworkManager<object_ts...>;
    using base_t::cqes;
    using base_t::nobjects;
    using base_t::object_sizes;
    using base_t::ring;
    DEBUGGING(using base_t::pages_submitted);

  private:
    std::vector<std::function<void(void*, u32 /* local dst */)>> consumer_fns;

  public:
    IngressNetworkManager(u32 npeers, u32 nwdepth, bool sqpoll, const std::vector<int>& sockets)
        : base_t(nwdepth, sqpoll, sockets), consumer_fns(nobjects, [](void*, u32) {})
    {
    }

    template <typename T>
    void register_consumer_fn(std::function<void(T*, u32 /* local dst */)>& fn)
    {
        // wrapper to allow void* args
        consumer_fns[base_t::template get_type_idx<T>()] = [fn](void* obj, u32 dst) { fn(reinterpret_cast<T*>(obj), dst); };
    }

    DEBUGGING(auto get_pages_recv() const { return pages_submitted; })

    void post_recvs(UserData user_data, u64 size)
    {
        auto* sqe = io_uring_get_sqe(&ring);
        io_uring_prep_recv(sqe, user_data.get_top_tag(), user_data.get_pointer(), size, MSG_WAITALL);
        io_uring_sqe_set_data(sqe, user_data.as_data());
        sqe->flags |= IOSQE_FIXED_FILE | IOSQE_IO_LINK;
        io_uring_submit(&ring);
    }

    template <typename T>
    void post_recvs(u16 dst, T* obj)
    {
        UserData user_data{obj, dst, base_t::template get_type_idx<T>()};
        post_recvs(user_data, sizeof(T));
    }

    void consume_done()
    {
        auto cqes_peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
        for (auto i{0u}; i < cqes_peeked; ++i) {
            UserData user_data{io_uring_cqe_get_data(cqes[i])};
            if (cqes[i]->res != object_sizes[user_data.get_bottom_tag()]) {
                throw IOUringRecvError(cqes[i]->res);
            }
            consumer_fns[user_data.get_bottom_tag()](user_data.get_pointer(), user_data.get_top_tag());
        }
        DEBUGGING(pages_submitted += cqes_peeked;)
        io_uring_cq_advance(&ring, cqes_peeked);
    }
};

template <typename... object_ts>
using HomogeneousIngressNetworkManager = IngressNetworkManager<false, object_ts...>;

template <typename... object_ts>
using HeterogeneousIngressNetworkManager = IngressNetworkManager<true, object_ts...>;

} // namespace network
