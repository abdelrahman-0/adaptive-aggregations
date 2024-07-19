#pragma once

#include <exception>
#include <string>
#include <system_error>

class IOUringInitError : public std::runtime_error {
  public:
    IOUringInitError() : std::runtime_error("Could not initialize io_uring") {}
};

class IOUringRegisterFileError : public std::runtime_error {
public:
  IOUringRegisterFileError() : std::runtime_error("Error registering file") {}
};

class IOUringSubmissionQueueFullError : public std::runtime_error {
public:
  IOUringSubmissionQueueFullError() : std::runtime_error("Submission queue is full") {}
};
