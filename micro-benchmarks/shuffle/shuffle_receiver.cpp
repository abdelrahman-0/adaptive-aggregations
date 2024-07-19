#include <cassert>
#include <gflags/gflags.h>
#include <liburing.h>

#include "network/connection.h"
#include "storage/page.h"

DEFINE_int32(senders, 1, "number of sender nodes to accept requests from");

#define SCHEMA int64_t, int64_t, int32_t, std::array<unsigned char, 4>

using ResultTuple = std::tuple<SCHEMA>;
using ResultPage = PageLocal<ResultTuple>;
using NetworkPage = PageCommunication<ResultTuple>;

int main() {
    //     setup connection
    //        Connection connection{0, FLAGS_senders};
    //        connection.setup();
    char ip[INET_ADDRSTRLEN];

    addrinfo* res;
    auto hints = Connection::get_connection_hints();
    ::getaddrinfo(nullptr, defaults::port, &hints, &res);
    auto sock_fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);

    int yes = 1;
    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
        perror("setsockopt");
        exit(1);
    }

    ::bind(sock_fd, res->ai_addr, res->ai_addrlen);
    listen(sock_fd, 100);

    // listen loop
    ::inet_ntop(res->ai_family, res->ai_addr, ip, sizeof ip);
    // now accept an incoming connection:
    sockaddr_storage their_addr;
    auto addr_size = sizeof their_addr;
    println("Listening for connections on ", std::string(ip));
    auto new_fd = ::accept(sock_fd, (sockaddr*)&their_addr, reinterpret_cast<socklen_t*>(&addr_size));
    println("Accepted");
    ::inet_ntop(their_addr.ss_family, (sockaddr*)&their_addr, ip, sizeof ip);
    println("Received connection from ", std::string(ip));

    // multi-threaded?
    NetworkPage page;

    io_uring ring{};
    io_uring_queue_init(256, &ring, 0);

    auto i = 0u;
    while (true) {
        auto sqe = io_uring_get_sqe(&ring);
        io_uring_prep_recv(sqe, new_fd, &page, defaults::network_page_size, 0);

        auto num_submitted = io_uring_submit(&ring);
        assert(num_submitted == 1);
        io_uring_cqe* cqe;
        auto ret = io_uring_wait_cqe(&ring, &cqe);
        assert(ret == 0);
//        page.print_contents();
        println("Received: ", ++i, "pages");
        io_uring_cqe_seen(&ring, cqe);
    }
}
