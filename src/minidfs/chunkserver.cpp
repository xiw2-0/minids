/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Master program in minidfs.

#include <iostream>
#include <minidfs/dfs_chunkserver.hpp>

/// the port where master serves
const int masterPort = 12345;
/// the IP where master serves
const string masterIP = "127.0.0.1";

/// the port where chunkserver serves
const int serverPort = 90909;
/// the IP where chunkserver serves
const string serverIP = "127.0.0.1";

const int maxConnections = 5;

/// folder that stores the blocks
const string dataDir = "./data/chunkserver";

/// block size
const long long blockSize = 2 * 1024 * 1024;

const size_t nThread = 2;

/// data sending/recving buffer size
const int BUFFER_SIZE = 2 * 1024;

/// heart beat interval, in ms
const long long HEART_BEAT_INTERVAL = 3000;

/// block report interval, in ms
const long long BLOCK_REPORT_INTERVAL = 7000;

/// at the startup, don't retrieve block tasks from master, in ms
const long long BLK_TASK_STARTUP_INTERVAL = 13000;


/// Start Chunkserver and provide services endlessly.
int main(int argc, char const *argv[])
{
  std::cout << "Start Chunkserver...\n";
  minidfs::DFSChunkserver chunkserver(masterIP, masterPort, serverIP, serverPort,
                                        dataDir, blockSize, maxConnections, BUFFER_SIZE,
                                        nThread,
                                        HEART_BEAT_INTERVAL, BLOCK_REPORT_INTERVAL,
                                        BLK_TASK_STARTUP_INTERVAL);
  
  chunkserver.run();
  return 0;
}
