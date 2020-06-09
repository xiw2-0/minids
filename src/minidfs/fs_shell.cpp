/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief A simple program to interact with master and chunk_server to write/read data.


#include <iostream>
#include <string>

#include <minidfs/dfs_client.hpp>
#include "logging/logger.h"
#include "config/config.h"

using std::string;
using std::cout;
using std::cin;

const char* config_file = "./config/shell.txt";

string masterIP;
int masterPort;
int bufferSize;
string bufferBlkName;
long long blockSize;

void configure() {
  config::Config c(config_file);
  c.parse();

  c.get("masterIP", &masterIP);
  c.get("masterPort", &masterPort);
  c.get("bufferSize", &bufferSize);
  c.get("bufferBlkName", &bufferBlkName);
  c.get("blockSize", &blockSize);
}

void usage() {
  cout << "Usage: ./bin/dfs_shell"
       << " [-ls <path>] \n"
       << " [-put <src> <dst>]\n"
       << " [-get <src> <dst>]\n"
       << " [-rm <filename>]\n"
       << " [-exists <file>]\n"
       << " [-mkdir <path>]\n"
       << std::endl;
}


int main(int argc, char const *argv[]) {
  logging::Logger::set_log_level(logging::INFO);

  configure();
  minidfs::DFSClient client(masterIP, masterPort, bufferSize, bufferBlkName, blockSize);
  if (argc < 3 || argc > 4) {
     LOG_ERROR << "Wrong number of arguments.";
    usage();
  }
  if (strcmp("-ls", argv[1]) == 0) {
    string path(argv[2]);
    std::vector<minidfs::FileInfo> items;
    if (-1 == client.ls(path, items)) {
      return 0;
    }
    cout << "Name \t IsDir \t Length\n";
    for (const auto& i : items) {
      cout << i.name() << "\t" << i.isdir() << "\t" << i.filelen() << std::endl;
    }
  } else if (strcmp("-put", argv[1]) == 0) {
    if (argc != 4) {
       LOG_ERROR << "Wrong number of arguments.";
      usage();
      return 0;
    }
    string src(argv[2]), dst(argv[3]);
    if (-1 == client.putFile(src, dst)) {
       LOG_ERROR << "Failed to put a file";
      return 0;
    }
     LOG_INFO << "Succeed to put a file";
  } else if (strcmp("-get", argv[1]) == 0) {
    if (argc != 4) {
       LOG_ERROR << "Wrong number of arguments.";
      usage();
      return 0;
    }
    string src(argv[2]), dst(argv[3]);
    if (-1 == client.getFile(src, dst)) {
       LOG_ERROR << "Failed to get a file";
      return 0;
    }
     LOG_INFO << "Succeed to get a file";
  } else if (strcmp("-rm", argv[1]) == 0) {
    string filename(argv[2]);
    if (-1 == client.remove(filename)) {
       LOG_ERROR << "Failed to remove a file";
      return 0;
    }
     LOG_INFO << "Succeed to remove a file";
  } else if (strcmp("-exists", argv[1]) == 0) {
    string file(argv[2]);
    if (-1 == client.exists(file)) {
       LOG_ERROR << "Failed to query a file";
      return 0;
    }
     LOG_INFO << "Succeed to query a file";
  } else if (strcmp("-mkdir", argv[1]) == 0) {
    string path(argv[2]);
    if (-1 == client.mkdir(path)) {
       LOG_ERROR << "Failed to make a directory";
      return 0;
    }
     LOG_INFO << "Succeed to make a directory";
  } else {
     LOG_ERROR << "Wrong arguments.";
    usage();
  }
  return 0;
}
