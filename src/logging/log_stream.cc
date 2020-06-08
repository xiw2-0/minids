/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include "logging/log_stream.h"

#include <thread>
#include <sstream>



namespace logging {


void LogStream::buf(char** p_buf, size_t* size) {
  *p_buf = buf_;
  *size = buf_pointer_;
}

LogStream &LogStream::operator<<(bool arg) {
  if(arg)
    append("true");
  else
    append("false");
  return *this;
}

LogStream &LogStream::operator<<(char arg) {
  char str[2] = {arg,'\0'};
  append(str);

  return *this;
}

LogStream &LogStream::operator<<(int16_t arg) {
  auto str = std::to_string(arg);
  append(str.c_str());
  
  return *this;
}

LogStream &LogStream::operator<<(uint16_t arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(int32_t arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(uint32_t arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(int64_t arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(uint64_t arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(float arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(double arg) {
  auto str = std::to_string(arg);
  append(str.c_str());

  return *this;
}

LogStream &LogStream::operator<<(const char *arg) {
  append(arg);

  return *this;
}

LogStream &LogStream::operator<<(const string &arg) {
  append(arg.c_str());

  return *this;
}

void LogStream::append(const char *s) {
  auto buf_left = buf_size_ - buf_pointer_ > 0 ? buf_size_ - buf_pointer_ : 0;
  buf_pointer_ += ::snprintf(buf_ + buf_pointer_, buf_left, "%s", s);
}


/// global output
std::function<void(const char*, size_t)> g_out = [](const char* buf, size_t size){
  ::fwrite(buf, 1, size, ::stdout);
};

/// global flush
std::function<void()> g_flush = [](){
  ::fflush(::stdout);
};



} // namespace logging
