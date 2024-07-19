#pragma once

#include <arpa/inet.h>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "defaults.h"
#include "exceptions/exceptions_network.h"
#include "utils/custom_concepts.h"
#include "utils/utils.h"

static constexpr auto communication_port = defaults::port;
static char ip_buffer[INET_ADDRSTRLEN];

// template <custom_concepts::Can_Send_Recv T>
class Connection {
  private:
    std::size_t num_receivers{0};
    std::size_t num_senders{0};
    std::vector<int> socket_fds{};

  public:
    static auto get_connection_hints() {
        addrinfo hints{};
        memset(&hints, 0, sizeof(addrinfo));
        hints.ai_family = AF_INET;                    // use IPv4
        hints.ai_socktype = SOCK_STREAM;              // TCP
        hints.ai_flags = AI_PASSIVE | AI_NUMERICHOST; // use my IP
        return hints;
    }
    explicit Connection(int num_receivers, int num_senders = 0u)
        : num_receivers(num_receivers), num_senders(num_senders), socket_fds(num_receivers, -1) {}

    void setup() {
        if (num_receivers == 0 && num_senders == 0)
            return;
        int rv;
        auto hints = get_connection_hints();
        for (auto i = 0u; i < num_receivers; ++i) {
            // setup connection structs
            auto receiver_ip = std::string{defaults::subnet} + std::to_string(defaults::receiver_host_base + i);
            println<' '>("connecting to", receiver_ip, "...");
            addrinfo* peer;
            if ((rv = getaddrinfo(receiver_ip.c_str(), communication_port, &hints, &peer)) != 0) {
                throw NetworkPrepareAddressError(rv);
            }

            // find first valid peer address
            for (auto rp = peer; rp != nullptr; rp = rp->ai_next) {
                socket_fds[i] = socket(peer->ai_family, peer->ai_socktype, peer->ai_protocol);
                if (socket_fds[i] == -1) {
                    close(socket_fds[i]);
                    continue;
                }
                break;
            }

            // connect to peer
            if ((rv = connect(socket_fds[i], peer->ai_addr, peer->ai_addrlen)) != 0) {
                auto* ipv4 = reinterpret_cast<sockaddr_in*>(peer->ai_addr);
                inet_ntop(peer->ai_family, &(ipv4->sin_addr), ip_buffer, sizeof(ip_buffer));
                throw NetworkConnectionError(ip_buffer);
            }

            // free addrinfo linked list
            freeaddrinfo(peer);
        }
    }

    [[nodiscard]] auto& get_socket_fds() const { return socket_fds; }

    void close_connections() {
        for (auto i : socket_fds) {
            ::close(i);
        }
    }
};
