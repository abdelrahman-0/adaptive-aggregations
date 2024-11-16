#pragma once

#include <liburing.h>
#include <vector>

#include "defaults.h"
#include "misc/exceptions/exceptions_io_uring.h"
#include "utils/utils.h"

namespace network {

// tagged pointer with two 8-bit tags
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
template <typename... object_ts>
class EgressNetworkManager : public BaseNetworkManager<object_ts...> {
    using base_t = BaseNetworkManager<object_ts...>;
    using base_t::cqes;
    using base_t::nobjects;
    using base_t::object_sizes;
    using base_t::ring;

  private:
    std::vector<std::queue<UserData>> pending_objects;
    std::vector<bool> has_inflight;
    u32 inflight_egress{0};
    std::vector<std::function<void(void*)>> consumer_fns;

  public:
    EgressNetworkManager(u32 npeers, u32 nwdepth, bool sqpoll, const std::vector<int>& sockets)
        : base_t(nwdepth, sqpoll, sockets), pending_objects(npeers), has_inflight(npeers, false), consumer_fns(nobjects, [](void*) {})
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
        inflight_egress++;
        io_uring_submit(&ring);
    }

    void flush_object(UserData user_data)
    {
        // extract size from type
        flush_object(user_data, object_sizes[user_data.get_bottom_tag()]);
    }

    template <typename T>
    void try_flush(u16 dst, T* obj)
    {
        UserData user_data{obj, dst, base_t::template get_type_idx<T>()};
        if (has_inflight[dst]) {
            pending_objects[dst].push(user_data);
        }
        else {
            has_inflight[dst] = true;
            if (pending_objects[dst].empty()) {
                flush_object(user_data, sizeof(T));
            }
            else {
                // flush first object in dst queue and add current object to queue
                flush_object(pending_objects[dst].front());
                pending_objects[dst].pop();
                pending_objects[dst].push(user_data);
            }
        }
    }

    void try_drain_pending()
    {
        auto peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
        for (auto i{0u}; i < peeked; ++i) {
            UserData user_data{io_uring_cqe_get_data(cqes[i])};
            auto peeked_dst = user_data.get_top_tag();
            if (not pending_objects[peeked_dst].empty()) {
                flush_object(pending_objects[peeked_dst].front());
                pending_objects[peeked_dst].pop();
            }
            else {
                has_inflight[peeked_dst] = false;
            }
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
            auto dst = user_data.get_top_tag();
            if (not pending_objects[dst].empty()) {
                flush_object(pending_objects[dst].front());
                pending_objects[dst].pop();
            }
            consumer_fns[obj_idx](user_data.get_pointer());
        }
    }
};

// class for receiving fixed-size objects of unique types
template <typename... object_ts>
class IngressNetworkManager : public BaseNetworkManager<object_ts...> {
    using base_t = BaseNetworkManager<object_ts...>;
    using base_t::cqes;
    using base_t::nobjects;
    using base_t::object_sizes;
    using base_t::ring;
    DEBUGGING(using base_t::pages_submitted);

  private:
    std::vector<std::function<void(void*, u32 /* local dst */)>> consumer_fns;

    // TODO post recvs just outside morsel loop
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

    void consume_pages()
    {
        auto cqes_peeked = io_uring_peek_batch_cqe(&ring, cqes.data(), cqes.size());
        for (auto i{0u}; i < cqes_peeked; ++i) {
            UserData user_data{io_uring_cqe_get_data(cqes[i])};
            DEBUGGING(if (cqes[i]->res != object_sizes[user_data.get_bottom_tag()]) { throw IOUringRecvError(cqes[i]->res); })
            consumer_fns[user_data.get_bottom_tag()](user_data.get_pointer(), user_data.get_top_tag());
        }
        DEBUGGING(pages_submitted += cqes_peeked;)
        io_uring_cq_advance(&ring, cqes_peeked);
    }
};

} // namespace network
