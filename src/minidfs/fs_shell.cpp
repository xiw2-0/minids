/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief A simple program to interact with master and chunk_server to write/read data.


#include <iostream>
#include <string>

#include <minidfs/dfs_client.hpp>

using std::string;
using std::cout;
using std::cin;

const string masterIP = "127.0.0.1";
const int masterPort = 12345;
const int bufferSize = 2 * 1024;
const string bufferBlkName = "/tmp/dfs_client_buf";
const long long blockSize = 2 * 1024 * 1024;



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
  minidfs::DFSClient client(masterIP, masterPort, bufferSize, bufferBlkName, blockSize);
  if (argc < 3 || argc > 4) {
    cout << "[DFSShell] " << "Wrong number of arguments.\n";
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
      cout << "[DFSShell] " << "Wrong number of arguments.\n";
      usage();
      return 0;
    }
    string src(argv[2]), dst(argv[3]);
    if (-1 == client.putFile(src, dst)) {
      cout << "[DFSShell] " << "Failed to put a file\n";
      return 0;
    }
    cout << "[DFSShell] " << "Succeed to put a file\n";
  } else if (strcmp("-get", argv[1]) == 0) {
    if (argc != 4) {
      cout << "[DFSShell] " << "Wrong number of arguments.\n";
      usage();
      return 0;
    }
    string src(argv[2]), dst(argv[3]);
    if (-1 == client.getFile(src, dst)) {
      cout << "[DFSShell] " << "Failed to get a file\n";
      return 0;
    }
    cout << "[DFSShell] " << "Succeed to get a file\n";
  } else if (strcmp("-rm", argv[1]) == 0) {
    string filename(argv[2]);
    if (-1 == client.remove(filename)) {
      cout << "[DFSShell] " << "Failed to remove a file\n";
      return 0;
    }
    cout << "[DFSShell] " << "Succeed to remove a file\n";
  } else if (strcmp("-exists", argv[1]) == 0) {
    string file(argv[2]);
    if (-1 == client.exists(file)) {
      cout << "[DFSShell] " << "Failed to query a file\n";
      return 0;
    }
    cout << "[DFSShell] " << "Succeed to query a file\n";
  } else if (strcmp("-mkdir", argv[1]) == 0) {
    string path(argv[2]);
    if (-1 == client.mkdir(path)) {
      cout << "[DFSShell] " << "Failed to make a directory\n";
      return 0;
    }
    cout << "[DFSShell] " << "Succeed to make a directory\n";
  } else {
    cout << "[DFSShell] " << "Wrong arguments.\n";
    usage();
  }
  return 0;
}
