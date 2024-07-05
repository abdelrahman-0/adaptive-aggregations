#pragma once

#include <exception>
#include <string>

class FileError : public std::runtime_error {
public:
    explicit FileError(const std::string &msg) : std::runtime_error(msg) {}
};

class FileNotExistsError : public FileError {
public:
    FileNotExistsError() : FileError("File path does not exist") {}
};

class FileOpenError : public FileError {
public:
    FileOpenError() : FileError("Could not open file") {}
};

class RegisterFileError : public std::runtime_error {
public:
    RegisterFileError() : std::runtime_error("Error registering file") {}
};

class SubmissionQueueFullError : public std::runtime_error {
public:
    SubmissionQueueFullError() : std::runtime_error("Submission queue is full") {}
};
