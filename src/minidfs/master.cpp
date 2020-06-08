/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Master program in minidfs.

#include <iostream>
#include <minidfs/dfs_master.hpp>
#include "logging/logger.h"

const string nameSysFile("./data/namesys");
const string editLogFile("./data/editlog");
const int serverPort = 12345;
const int maxConn = 3;
const int replicationFactor = 1;
const int nThread = 2;

/// Start Master and provide services endlessly.
int main(int argc, char const *argv[])
{
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
