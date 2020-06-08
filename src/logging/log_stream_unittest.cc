/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Test for log stream

#include "logging/log_stream.h"

#include <cassert>
#include <cstring>

void TestBool() {
  logging::LogStream s;

  char *buf;
  size_t size = 0;

  s.buf(&buf, &size);
  assert(::strcmp(buf, "") == 0);
  assert(size == 0);

  s << true;
  s.buf(&buf, &size);
  assert(::strcmp(buf, "true") == 0);
  assert(size == 4);

  s << ' ' << false;
  s.buf(&buf, &size);
  assert(::strcmp(buf, "true false") == 0);
  assert(size == 10);

  ::printf("bool test pass ...\n");
}

void TestNumber() {
  logging::LogStream s;

  char *buf;
  size_t size = 0;

  s.buf(&buf, &size);
  assert(::strcmp(buf, "") == 0);
  assert(size == 0);

  s << 0;
  s.buf(&buf, &size);
  assert(::strcmp(buf, "0") == 0);
  assert(size == 1);
  
  string str("0 ");
  s << ' ' << 9.99;
  s.buf(&buf, &size);
  str += std::to_string(9.99);
  assert(::strcmp(buf, str.c_str()) == 0);
  assert(size == str.size());

  s << ' ' << -66;
  s.buf(&buf, &size);
  str += " ";
  str += std::to_string(-66);
  assert(::strcmp(buf, str.c_str()) == 0);
  assert(size == str.size());

  s << ' ' << -8.88;
  s.buf(&buf, &size);
  str += " ";
  str += std::to_string(-8.88);
  assert(::strcmp(buf, str.c_str()) == 0);
  assert(size == str.size());

  ::printf("number test pass ...\n");
}

void TestString() {
  logging::LogStream s;

  char *buf;
  size_t size = 0;

  s.buf(&buf, &size);
  assert(::strcmp(buf, "") == 0);
  assert(size == 0);

  s << "this is good!";
  s.buf(&buf, &size);
  assert(::strcmp(buf, "this is good!") == 0);
  assert(size == 13);

  string str("std::string");
  s << ' ' << str;
  s.buf(&buf, &size);
  assert(::strcmp(buf, "this is good! std::string") == 0);
  assert(size == 25);

  ::printf("string test pass ...\n");
}

void TestLogStream() {
  ::printf("Test LogStream...\n");

  TestBool();
  TestNumber();
  TestString();

  ::printf("\n");
}