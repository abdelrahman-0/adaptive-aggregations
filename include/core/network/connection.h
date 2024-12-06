#pragma once

#include <arpa/inet.h>
#include <cstring>
#include <liburing.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <utility>
#include <vector>

#include "bench/bench.h"
#include "defaults.h"
#include "misc/exceptions/exceptions_network.h"

static constexpr auto communication_port_base = defaults::port;
static char ip_buffer[INET_ADDRSTRLEN];

static auto get_connection_hints(bool use_ipv6 = false)
{
    ::addrinfo hints{};
    ::memset(&hints, 0, sizeof(addrinfo));
    hints.ai_family   = use_ipv6 ? AF_INET6 : AF_INET; // use IPv4/IPv6
    hints.ai_socktype = SOCK_STREAM;                   // TCP
    hints.ai_flags    = AI_PASSIVE | AI_NUMERICHOST;   // use my IP
    return hints;
}

// TODO rewrite
// open multiple TCP connections to a particular destination
struct Connection {
    std::string connection_ip{};
    std::vector<int> socket_fds{};
    u32 num_connections{};
    u32 node_id{};
    u32 nthreads{};
    u32 thread_id{};

    Connection() = default;

    explicit Connection(const u32 node_id, const u32 nthreads, const u32 thread_id, const u32 num_connections = 1)
        : socket_fds(num_connections, -1), num_connections(num_connections), node_id(node_id), nthreads(nthreads), thread_id(thread_id)
    {
    }

    explicit Connection(const u32 node_id, const u32 nthreads, const u32 thread_id, std::string connection_ip, const u32 num_connections = 1)
        : connection_ip(std::move(connection_ip)), socket_fds(num_connections, -1), num_connections(num_connections), node_id(node_id), nthreads(nthreads), thread_id(thread_id)
    {
    }

    ~Connection() = default;

    // incoming connections
    void setup_ingress()
    {
        ::addrinfo* local;
        const auto hints            = get_connection_hints();
        const auto thread_comm_port = std::to_string(communication_port_base + node_id * nthreads + thread_id);
        if (int ret; (ret = ::getaddrinfo(nullptr, thread_comm_port.c_str(), &hints, &local)) != 0) {
            throw NetworkPrepareAddressError{ret};
        }
        auto sock_fd = ::socket(local->ai_family, local->ai_socktype, local->ai_protocol);
        if (sock_fd == -1) {
            throw NetworkSocketSetupError{};
        }

        int reuse = 1;
        if (::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1) {
            throw NetworkSocketOptError{};
        }

        socklen_t size = sizeof(defaults::kernel_recv_buffer_size);
        if (::setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &defaults::kernel_recv_buffer_size, size) == -1) {
            throw NetworkSocketOptError{};
        }

        auto double_kernel_recv_buffer_size{0u};
        ::getsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &double_kernel_recv_buffer_size, &size);

        // print("setting recv buff (excl. kernel bookkeeping):", defaults::kernel_recv_buffer_size);
        // print("size of recv buff (incl. kernel bookkeeping):", double_kernel_recv_buffer_size);

        if (::bind(sock_fd, local->ai_addr, local->ai_addrlen) == -1) {
            throw NetworkSocketBindError{};
        }

        if (::listen(sock_fd, defaults::listen_queue_depth) == -1) {
            throw NetworkSocketListenError{};
        }

        // accept an incoming connection
        ::inet_ntop(local->ai_family, local->ai_addr, ip_buffer, sizeof(ip_buffer));

        // listen loop
        for (auto i = 0u; i < node_id; ++i) {
            ::sockaddr_storage ingress_addr{};
            socklen_t addr_size   = sizeof(ingress_addr);
            const auto ingress_fd = ::accept(sock_fd, (sockaddr*)&ingress_addr, &addr_size);
            if (ingress_fd < 0) {
                throw NetworkSocketAcceptError{};
            }
            u32 incoming_node_id;
            ::recv(ingress_fd, &incoming_node_id, sizeof(incoming_node_id), MSG_WAITALL);
            DEBUGGING(print("accepted connection from node", incoming_node_id, "( ip:", std::string(ip_buffer), ")"));
            socket_fds[incoming_node_id] = ingress_fd;
            ::inet_ntop(ingress_addr.ss_family, &ingress_addr, ip_buffer, sizeof(ip_buffer));
        }
        ::freeaddrinfo(local);
        ::close(sock_fd);
    }

    // outgoing connections
    void setup_egress(u32 outgoing_node_id)
    {
        if (num_connections == 0) {
            return;
        }
        int ret;
        auto hints            = get_connection_hints();
        auto thread_comm_port = std::to_string(communication_port_base + outgoing_node_id * nthreads + thread_id);
        DEBUGGING(print("opening"s, num_connections, "connections to:"s, connection_ip, "..."s));
        for (auto i = 0u; i < num_connections; ++i) {
            // setup connection structs
            addrinfo* peer;
            if ((ret = ::getaddrinfo(connection_ip.c_str(), thread_comm_port.c_str(), &hints, &peer)) != 0) {
                throw NetworkPrepareAddressError{ret};
            }

            // find first valid peer address
            for (auto rp = peer; rp != nullptr; rp = rp->ai_next) {
                socket_fds[i] = ::socket(peer->ai_family, peer->ai_socktype, peer->ai_protocol);
                if (socket_fds[i] == -1) {
                    ::close(socket_fds[i]);
                    continue;
                }

                socklen_t size = sizeof(defaults::kernel_send_buffer_size);
                if (::setsockopt(socket_fds[i], SOL_SOCKET, SO_SNDBUF, &defaults::kernel_send_buffer_size, size) == -1) {
                    throw NetworkSocketOptError{};
                }

                auto double_kernel_send_buffer_size{0u};
                ::getsockopt(socket_fds[i], SOL_SOCKET, SO_SNDBUF, &double_kernel_send_buffer_size, &size);

                ::linger sl{};
                sl.l_onoff  = 1; /* non-zero value enables linger option in kernel */
                sl.l_linger = 2;
                setsockopt(socket_fds[i], SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));

                break;
            }
            const auto* ip = reinterpret_cast<sockaddr_in*>(peer->ai_addr);
            ::inet_ntop(peer->ai_family, &(ip->sin_addr), ip_buffer, sizeof(ip_buffer));

            // connect to peer
            if (::connect(socket_fds[i], peer->ai_addr, peer->ai_addrlen) == -1) {
                throw NetworkConnectionError(ip_buffer);
            }

            ::send(socket_fds[i], &node_id, sizeof(node_id), MSG_WAITALL);

            sockaddr_in sin{};
            socklen_t len = sizeof(sin);
            ::getsockname(socket_fds[i], (struct sockaddr*)&sin, &len);

            // free addrinfo linked list
            ::freeaddrinfo(peer);
        }
    }

    void close_connections()
    {
        for (const auto i : socket_fds) {
            //            ::close(i);
            ::shutdown(i, SHUT_RDWR);
        }
    }
};
