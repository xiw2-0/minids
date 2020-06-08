/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Stream used by logger to store a line of log.

#ifndef LOG_STREAM_H_
#define LOG_STREAM_H_

#include <cstdint>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <string>

#include <chrono>
#include <functional>

using std::string;

namespace logging {


class LogStream {
 public:
  LogStream() = default;
  ~LogStream() = default;

  LogStream &operator<<(bool arg);
  LogStream &operator<<(char arg);

  LogStream &operator<<(int16_t arg);
  LogStream &operator<<(uint16_t arg);
  LogStream &operator<<(int32_t arg);
  LogStream &operator<<(uint32_t arg);
  LogStream &operator<<(int64_t arg);
  LogStream &operator<<(uint64_t arg);

  LogStream &operator<<(float arg);
  LogStream &operator<<(double arg);

  LogStream &operator<<(const char *arg);
  LogStream &operator<<(const string &arg);

  /// Return buf and size
  /// \param p_buf pointer to buf
  /// \param size pointer to size
  void buf(char** p_buf, size_t* size);

 private:
  /// Append s to buf_
  void append(const char *s);

  //
  // buffer for a line of log
  //
  static constexpr int buf_size_ = 4 * 1024;

  char buf_[buf_size_] = {0};
  size_t buf_pointer_ = 0;


  
};


/// global output
extern std::function<void(const char*, size_t)> g_out;

/// global flush
extern std::function<void()> g_flush;

  
} // namespace logging










#endif