/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Logger class and macros.


#ifndef LOGGER_H_
#define LOGGER_H_



#include "logging/log_stream.h"


namespace logging {


enum LogLevel: int {
  TRACE = 0,
  DEBUG,
  INFO,
  WARN,
  ERROR,
  FATAL
};

/// A very simple logger.
class Logger {
 public:
  Logger(LogLevel level, const char *file, const char *function, uint32_t line);
  ~Logger();

  /// Set global log level
  static void set_log_level(LogLevel level);

  /// Get log level
  static LogLevel log_level();

  /// return log stream which binds with this logger
  LogStream &stream();

 private:
  
  LogStream stream_;
  LogLevel log_level_;

  //
  // file and line info
  //
  const char *file_;
  const char *function_;
  const uint32_t line_;

  /// global log level
  static LogLevel g_log_level_;
};



} // namespace logging



#define LOG_TRACE if (logging::Logger::log_level() <= logging::TRACE) \
    logging::Logger(logging::TRACE, __FILE__, __FUNCTION__, __LINE__).stream()

#define LOG_DEBUG if (logging::Logger::log_level() <= logging::DEBUG) \
    logging::Logger(logging::DEBUG, __FILE__, __FUNCTION__, __LINE__).stream()

#define LOG_INFO if (logging::Logger::log_level() <= logging::INFO) \
    logging::Logger(logging::INFO, __FILE__, __FUNCTION__, __LINE__).stream()

#define LOG_WARN if (logging::Logger::log_level() <= logging::WARN) \
    logging::Logger(logging::WARN, __FILE__, __FUNCTION__, __LINE__).stream()

#define LOG_ERROR \
    logging::Logger(logging::ERROR, __FILE__, __FUNCTION__, __LINE__).stream()

#define LOG_FATAL \
    logging::Logger(logging::FATAL, __FILE__, __FUNCTION__, __LINE__).stream()



#endif
