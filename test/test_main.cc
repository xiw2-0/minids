// MIT License
//
// Copyright (c) 2020 xiw
// \author wang xi
// \brief Test all unit test functions

#include <cstdio>

extern void TestLogStream();
extern void TestLogger();
extern void TestConfig();


int main(int argc, char const *argv[]) {
  printf("=================Test starts=================\n\n");

  TestLogStream();
  TestLogger();
  TestConfig();
  
  printf("=================Test ends=================\n");
  return 0;
}
