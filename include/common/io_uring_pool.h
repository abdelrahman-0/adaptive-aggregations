//#pragma once
//
//#include <liburing.h>
//#include <vector>
//
//#include "defaults.h"
//
//class IOUringPool {
//  private:
//    std::vector<io_uring> pool{};
//    uint32_t ring_depth{};
//    bool sqpoll{};
//
//  public:
//    explicit IOUringPool(uint64_t num_rings, uint64_t ring_depth = defaults::network_io_depth, bool sqpoll = false)
//        : ring_depth(ring_depth), sqpoll(sqpoll) {}
//
//    void register_fds(const std::vector<int>& fds) {
//        for (auto fd : fds) {
//            pool.emplace_back();
//            auto& ring = pool.back();
//            int ret;
//            if ((ret = io_uring_queue_init(ring_depth, &ring, sqpoll ? IORING_SETUP_SQPOLL : 0))) {
//                throw IOUringInitError{ret};
//            }
//
//            if ((ret = io_uring_register_files(&ring, conn.socket_fds.data(), conn.num_connections)) < 0) {
//                throw IOUringRegisterFilesError{ret};
//            }
//        }
//    }
//};