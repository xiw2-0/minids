/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Master program in minidfs.

#include <iostream>
#include <minidfs/dfs_chunkserver.hpp>
#include "logging/logger.h"
#include "config/config.h"

const char* config_file = "./config/chunkserver.txt";

int masterPort = 12345;
string masterIP = "127.0.0.1";
int serverPort = 90909;
string serverIP = "127.0.0.1";
int maxConnections = 5;
string dataDir = "./data/chunkserver";
long long blockSize = 2 * 1024 * 1024;
size_t nThread = 2;
int BUFFER_SIZE = 2 * 1024;
long long HEART_BEAT_INTERVAL = 3000;
long long BLOCK_REPORT_INTERVAL = 7000;
long long BLK_TASK_STARTUP_INTERVAL = 13000;

void configure() {
  config::Config c(config_file);
  c.parse();
  c.get("masterPort", &masterPort);
  c.get("masterIP", &masterIP);
  c.get("serverPort", &serverPort);
  c.get("serverIP", &serverIP);
  c.get("maxConnections", &maxConnections);
  c.get("dataDir", &dataDir);
  c.get("blockSize", &blockSize);
  c.get("nThread", &nThread);
  c.get("BUFFER_SIZE", &BUFFER_SIZE);
  c.get("HEART_BEAT_INTERVAL", &HEART_BEAT_INTERVAL);
  c.get("BLOCK_REPORT_INTERVAL", &BLOCK_REPORT_INTERVAL);
  c.get("BLK_TASK_STARTUP_INTERVAL", &BLK_TASK_STARTUP_INTERVAL);

}


/// Start Chunkserver and provide services endlessly.
int main(int argc, char const *argv[])
{
  configure();
  logging::Logger::set_log_level(logging::INFO);
  LOG_INFO << "Start Chunkserver...";
  minidfs::DFSChunkserver chunkserver(masterIP, masterPort, serverIP, serverPort,
                                        dataDir, blockSize, maxConnections, BUFFER_SIZE,
                                        nThread,
                                        HEART_BEAT_INTERVAL, BLOCK_REPORT_INTERVAL,
                                        BLK_TASK_STARTUP_INTERVAL);
  
  chunkserver.run();
  return 0;
}
