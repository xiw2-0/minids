/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include "logging/logger.h"

#include <thread>
#include <sstream>

namespace logging {

const char *log_level_str[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};

LogLevel Logger::g_log_level_ = INFO;

Logger::Logger(LogLevel level, const char *file, const char *function, uint32_t line)
    : log_level_(level), file_(file), function_(function), line_(line) {
  
  constexpr int HEADER_SIZE = 36;
  char log_header[HEADER_SIZE] = {0};

  // set time stamp
  time_t time_now;
  ::time(&time_now);
  auto tm_now = ::localtime(&time_now);

  auto size_ = ::strftime(log_header, HEADER_SIZE, "%Y/%m/%d %H:%M:%S", tm_now);
  auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::system_clock().now().time_since_epoch())
                   .count();
  
  ::snprintf(log_header + size_, HEADER_SIZE - size_, ".%06u", static_cast<uint32_t>(timestamp % 1000000));
  stream_ << log_header;

  // set thread id
  auto id = std::this_thread::get_id();

  std::ostringstream ss;
  ss << id;
  stream_ << ' ' << ss.str();

  // set log level
  stream_ << ' ' << log_level_str[level] << ' ';
}

Logger::~Logger() {
  // add file and line info
  char file_buf[128] = {0};
  ::snprintf(file_buf, 128, " - %s:%s:%u\n", file_, function_, line_);
  stream_ << file_buf;

  char* buf;
  size_t size;
  stream_.buf(&buf, &size);
  
  // flush
  g_out(buf, size);
  g_flush();

  if (log_level_ == FATAL) {
    ::abort();
  }
}

void Logger::set_log_level(LogLevel level) {
  g_log_level_ = level;
}

LogLevel Logger::log_level() {
  return g_log_level_;
}


LogStream &Logger::stream() {
  return stream_;
}

} // namespace logging


