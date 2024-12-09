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

static constexpr auto communication_port_base = defaults::port_base_worker;
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

// open multiple TCP connections to a particular destination
struct Connection {

    // incoming connections
    static decltype(auto) setup_ingress(const std::string& port, std::integral auto num_connections = 1)
    {
        ::addrinfo* local;
        const auto hints = get_connection_hints();
        // const auto thread_comm_port = std::to_string(communication_port_base + node_id * nthreads + thread_id);
        if (int ret; (ret = ::getaddrinfo(nullptr, port.c_str(), &hints, &local)) != 0) {
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

        if (::bind(sock_fd, local->ai_addr, local->ai_addrlen) == -1) {
            throw NetworkSocketBindError{};
        }

        if (::listen(sock_fd, defaults::listen_queue_depth) == -1) {
            throw NetworkSocketListenError{};
        }

        // accept incoming connections
        ::inet_ntop(local->ai_family, local->ai_addr, ip_buffer, sizeof(ip_buffer));

        // listen loop
        auto socket_fds = std::vector<int>(num_connections);
        while (num_connections--) {
            ::sockaddr_storage ingress_addr{};
            socklen_t addr_size   = sizeof(ingress_addr);
            const auto ingress_fd = ::accept(sock_fd, reinterpret_cast<sockaddr*>(&ingress_addr), &addr_size);
            if (ingress_fd < 0) {
                throw NetworkSocketAcceptError{};
            }
            node_id_t incoming_node_id;
            ::recv(ingress_fd, &incoming_node_id, sizeof(node_id_t), MSG_WAITALL);
            DEBUGGING(print("accepted connection from node", incoming_node_id, "( ip:", std::string(ip_buffer), ")"));
            socket_fds[incoming_node_id] = ingress_fd;
            ::inet_ntop(ingress_addr.ss_family, &ingress_addr, ip_buffer, sizeof(ip_buffer));
        }
        ::freeaddrinfo(local);
        ::close(sock_fd);
        return socket_fds;
    }

    static int setup_egress(node_id_t node_id, const std::string& ip_str, const std::string& port)
    {
        // setup connection structs
        auto hints = get_connection_hints();
        addrinfo* peer;
        if (int ret = ::getaddrinfo(ip_str.c_str(), port.c_str(), &hints, &peer); ret != 0) {
            throw NetworkPrepareAddressError{ret};
        }

        // find first valid peer address
        int socket_fd{-1};
        for (auto rp = peer; rp != nullptr; rp = rp->ai_next) {
            socket_fd = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (socket_fd == -1) {
                ::close(socket_fd);
                continue;
            }

            socklen_t size = sizeof(defaults::kernel_send_buffer_size);
            if (::setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, &defaults::kernel_send_buffer_size, size) == -1) {
                throw NetworkSocketOptError{};
            }

            ::linger sl{};
            sl.l_onoff  = 1; /* non-zero value enables linger option in kernel */
            sl.l_linger = 2;
            setsockopt(socket_fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
            break;
        }
        ENSURE(socket_fd != -1);

        // connect to peer
        if (::connect(socket_fd, peer->ai_addr, peer->ai_addrlen) == -1) {
            const auto* ip = reinterpret_cast<sockaddr_in*>(peer->ai_addr);
            ::inet_ntop(peer->ai_family, &(ip->sin_addr), ip_buffer, sizeof(ip_buffer));
            throw NetworkConnectionError(ip_buffer);
        }
        ::send(socket_fd, &node_id, sizeof(node_id_t), 0);
        // free addrinfo linked list
        ::freeaddrinfo(peer);
        return socket_fd;
    }

    static decltype(auto) setup_egress(node_id_t node_id, const std::string& ip_str, const std::string& port, std::integral auto num_connections)
    {
        DEBUGGING(print("opening"s, num_connections, "connections to:"s, ip_str, "..."s));
        auto socket_fds = std::vector<int>(num_connections);
        for (auto i = 0u; i < num_connections; ++i) {
            socket_fds[i] = setup_egress(node_id, ip_str, port);
        }
        return socket_fds;
    }

    static void close_connections(std::vector<int>& socket_fds)
    {
        for (auto fd : socket_fds) {
            ::shutdown(fd, SHUT_RDWR);
        }
    }
};
