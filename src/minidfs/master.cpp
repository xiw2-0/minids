/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Master program in minidfs.

#include <iostream>
#include <minidfs/dfs_master.hpp>

const string nameSysFile("./data/namesys");
const int serverPort = 12345;
const int maxConn = 3;
const int replicationFactor = 1;

/// Start Master and provide services endlessly.
int main(int argc, char const *argv[])
{
  std::cout << "Start Master...\n";
  auto master = minidfs::DFSMaster(nameSysFile, serverPort, maxConn, replicationFactor);
  
  if (argc == 2 && 0 == strcmp(argv[1], "-format")) {
    master.format();
  } else {
    master.startRun();
  }
  return 0;
}
