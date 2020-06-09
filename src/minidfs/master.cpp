/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Master program in minidfs.

#include <iostream>
#include <minidfs/dfs_master.hpp>
#include "logging/logger.h"
#include "config/config.h"

const char* config_file = "./config/master.txt";

string nameSysFile("./data/namesys");
string editLogFile("./data/editlog");
int serverPort = 12345;
int maxConn = 3;
int replicationFactor = 1;
int nThread = 2;

void configure() {
  config::Config c(config_file);
  c.parse();
  c.get("nameSysFile", &nameSysFile);
  c.get("editLogFile", &editLogFile);
  c.get("serverPort", &serverPort);
  c.get("maxConn", &maxConn);
  c.get("replicationFactor", &replicationFactor);
  c.get("nThread", &nThread);
}

/// Start Master and provide services endlessly.
int main(int argc, char const *argv[])
{
  configure();
  logging::Logger::set_log_level(logging::INFO);
  LOG_INFO << "Start Master...";
  minidfs::DFSMaster master(nameSysFile, editLogFile,serverPort, maxConn, replicationFactor, nThread);
  
  if (argc == 2 && 0 == strcmp(argv[1], "-format")) {
    master.format();
  } else {
    master.startRun();
  }
  return 0;
}
