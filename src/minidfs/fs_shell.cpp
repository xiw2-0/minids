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

int main(int argc, char const *argv[]) {
  cout << "Hello world!" << std::endl;
  //minidfs::DFSClient client("127.0.0.1", 12345);
  //minidfs::LocatedBlock locatedBlk;
  //cout << "create...\n";
  //client.create("/test.txt", &locatedBlk);
  //cout << locatedBlk.DebugString();
  return 0;
}
