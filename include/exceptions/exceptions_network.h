#pragma once

#include <exception>
#include <netdb.h>
#include <string>
#include <system_error>

using namespace std::string_literals;

class NetworkError : public std::runtime_error {
  public:
    explicit NetworkError(const std::string& msg) : std::runtime_error(msg) {}
};

class NetworkPrepareAddressError : public NetworkError {
  public:
    explicit NetworkPrepareAddressError(int val) : NetworkError("getaddrinfo: "s + gai_strerror(val)){};
};

class NetworkConnectionError : public NetworkError {
  public:
    explicit NetworkConnectionError(const char* ip_address)
        : NetworkError("Error connecting to " + std::string{ip_address}){};
};