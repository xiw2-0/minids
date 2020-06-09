/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Test for config

#include "config/config.h"

#include <cassert>
#include <cstdio>
#include <fcntl.h>



void TestParseLine() {

  class TestConfig: public config::Config {
   public:
    TestConfig(const char* file): config::Config(file){}

    void TestParseLine() {
      string line {"serverip = 9.9.9.1 \n"};
      parseLine(line);
      
      string ans {"9.9.9.1"}, val;
      bool ret = get("serverip", &val);
      assert(ret);
      assert(val == ans);
      ::printf("%s pass\n", line.c_str());


      line = "serverip = 127.0.0.1\t # this is comment\n";
      ans = "127.0.0.1";
      parseLine(line);
      ret = get("serverip", &val);
      assert(ret);
      assert(val == ans);
      ::printf("%s pass\n", line.c_str());


      line = "serverport=127001";
      int port_ans = 127001, port = 0;
      parseLine(line);
      ret = get("serverport", &port);
      assert(ret);
      assert(port == port_ans);
      ::printf("%s pass\n", line.c_str());


      line = "percent=0.55";
      double per_ans = 0.55, per = 0.0;
      parseLine(line);
      ret = get("percent", &per);
      assert(ret);
      assert(per == per_ans);
      ::printf("%s pass\n", line.c_str());


      line = "open = true";
      bool open_ans = true, open = false;
      parseLine(line);
      ret = get("open", &open);
      assert(ret);
      assert(open == open_ans);
      ::printf("%s pass\n", line.c_str());


      line = "go";
      parseLine(line);
      ret = get("go", &port);
      assert(ret == false);
      ::printf("%s pass\n", line.c_str());

      line = " # this is comment";
      assert(true == parseLine(line));
      ::printf("%s pass\n", line.c_str());

    }
  };
  ::printf("Test ParseLine...\n");

  TestConfig t("test");
  t.TestParseLine();
  ::printf("\n");
}

void TestParse() {
  char file_content[] = "serverip = 127.0.0.1\r\n serverport = 12345 \n duration=12.5 \n log=false\n # this is comment! \n";
  char file_name[] = "/tmp/test_parse";
  auto f = ::fopen(file_name, "w");
  ::fputs(file_content, f);
  ::fclose(f);

  config::Config conf(file_name);
  assert(conf.parse());
  
  string ip;
  assert(conf.get("serverip", &ip));
  assert(ip == "127.0.0.1");
  int port;
  assert(conf.get("serverport", &port));
  assert(port == 12345);
  double duration;
  assert(conf.get("duration", &duration));
  assert(duration == 12.5);
  bool log;
  assert(conf.get("log", &log));
  assert(log == false);
  assert(false == conf.get("notkey", &duration));

  ::remove(file_name);
  ::printf("Test config file parsing pass...\n");
}


void TestConfig() {
  ::printf("Test Config...\n");

  TestParseLine();
  TestParse();
  ::printf("\n");
}