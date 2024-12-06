#pragma once

#include <cstring>
#include <exception>
#include <netdb.h>
#include <string>
#include <system_error>

using namespace std::string_literals;

namespace except {

class GenericError : public std::runtime_error {
  public:
    explicit GenericError(const std::string& msg) : std::runtime_error(msg)
    {
    }
};

class EnsureError : public GenericError {
  public:
    explicit EnsureError(const std::string& msg) : GenericError{msg}
    {
    }
};

class GetResourceUsageError : public GenericError {
  public:
    GetResourceUsageError() : GenericError("Error getting resource usage"s)
    {
    }
};

class InvalidOptionError : public GenericError {
  public:
    explicit InvalidOptionError(const std::string& msg) : GenericError("Invalid option: " + msg)
    {
    }
};

} // namespace except
