/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implentation of class DFSClient.

#include <minidfs/dfs_client.hpp>

namespace minidfs {

DFSClient::DFSClient(const string& serverIP, int serverPort):
  master(new rpc::ClientProtocolProxy(serverIP, serverPort)) {
}

DFSClient::~DFSClient() {
}

int DFSClient::getBlockLocations(const string& file, LocatedBlocks* locatedBlks) {
  return master->getBlockLocations(file, locatedBlks);
}

int DFSClient::create(const string& file, LocatedBlock* locatedBlk) {
  return master->create(file, locatedBlk);
}

} // namespace minidfs

