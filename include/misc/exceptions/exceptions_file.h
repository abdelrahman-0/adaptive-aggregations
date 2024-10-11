#pragma once

#include <exception>
#include <string>
#include <system_error>

class FileError : public std::system_error {
  public:
    explicit FileError(const std::string& msg) : std::system_error(errno, std::system_category(), msg) {}
};

class FileNotExistsError : public FileError {
  public:
    FileNotExistsError() : FileError("File path does not exist") {}
};

class FileOpenError : public FileError {
  public:
    FileOpenError() : FileError("Could not open file") {}
};

class FileCloseError : public FileError {
  public:
    FileCloseError() : FileError("Could not close file") {}
};

class FileStatError : public FileError {
  public:
    FileStatError() : FileError("File stat error") {}
};
