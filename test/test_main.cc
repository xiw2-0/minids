// MIT License
//
// Copyright (c) 2020 xiw
// \author wang xi
// \brief Test all unit test functions

#include <cstdio>

extern void TestLogStream();
extern void TestLogger();


int main(int argc, char const *argv[]) {
  printf("=================Test starts=================\n\n");

  TestLogStream();
  TestLogger();
  
  printf("=================Test ends=================\n");
  return 0;
}
