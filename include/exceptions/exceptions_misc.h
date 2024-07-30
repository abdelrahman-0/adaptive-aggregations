#pragma once

#include <cstring>
#include <exception>
#include <netdb.h>
#include <string>
#include <system_error>

using namespace std::string_literals;

class GenericError : public std::runtime_error {
  public:
    explicit GenericError(const std::string& msg) : std::runtime_error(msg + " -- " + strerror(errno)) {}
};

class GetResourceUsageError : public GenericError {
  public:
    GetResourceUsageError() : GenericError("Error getting resource usage"s) {}
};
