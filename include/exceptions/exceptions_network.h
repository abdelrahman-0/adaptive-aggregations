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

class NetworkGenericError : public NetworkError {
  public:
    explicit NetworkGenericError(const std::string& msg) : NetworkError(msg + " -- " + strerror(errno)) {}
    explicit NetworkGenericError(const std::string& msg, int error) : NetworkError(msg + " -- " + strerror(-error)) {}
};

class NetworkSocketSetupError : public NetworkGenericError {
  public:
    explicit NetworkSocketSetupError() : NetworkGenericError("Error during socket setup"){};
};

class NetworkSocketOptError : public NetworkGenericError {
  public:
    explicit NetworkSocketOptError() : NetworkGenericError("Error setting socket options"){};
};

class NetworkSocketBindError : public NetworkGenericError {
  public:
    explicit NetworkSocketBindError() : NetworkGenericError("Error binding socket"){};
};

class NetworkSocketListenError : public NetworkGenericError {
  public:
    explicit NetworkSocketListenError() : NetworkGenericError("Error listening on socket"){};
};

class NetworkSocketAcceptError : public NetworkGenericError {
  public:
    explicit NetworkSocketAcceptError() : NetworkGenericError("Error accepting connection"){};
};

class NetworkSendError : public NetworkGenericError {
  public:
    NetworkSendError() : NetworkGenericError("Error sending data"){};
    explicit NetworkSendError(int error) : NetworkGenericError("Error sending data", error){};
};

class NetworkRecvError : public NetworkGenericError {
  public:
    NetworkRecvError() : NetworkGenericError("Error receiving data"){};
    explicit NetworkRecvError(int error) : NetworkGenericError("Error receiving data", error){};
};
