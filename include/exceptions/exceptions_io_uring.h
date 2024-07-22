#pragma once

#include <exception>
#include <string>
#include <system_error>

class IOUringError : public std::runtime_error {
  public:
    explicit IOUringError(const std::string& msg) : std::runtime_error(msg) {}
    IOUringError(const std::string& msg, int error) : std::runtime_error(msg + " -- " + strerror(-error)) {}
};

class IOUringInitError : public IOUringError {
  public:
    explicit IOUringInitError(int ret) : IOUringError("Could not initialize io_uring", ret) {}
};

class IOUringRegisterFileError : public IOUringError {
  public:
    explicit IOUringRegisterFileError(int ret) : IOUringError("Error registering file", ret) {}
};

class IOUringSubmissionQueueFullError : public IOUringError {
  public:
    IOUringSubmissionQueueFullError() : IOUringError("Submission queue is full") {}
};

class IOUringSubmissionError : public IOUringError {
  public:
    IOUringSubmissionError() : IOUringError("Submission error") {}
};

class IOUringSocketError : public IOUringError {
  public:
    IOUringSocketError() : IOUringError("Direct socket setup error") {}
};

class IOUringConnectError : public IOUringError {
  public:
    explicit IOUringConnectError(const char* ip_address)
        : IOUringError("Error connecting to " + std::string{ip_address}) {}
};

class IOUringMultiShotRecvError : public IOUringError {
  public:
    explicit IOUringMultiShotRecvError(int error) : IOUringError("Error submitting multishot receive request", error) {}
};
