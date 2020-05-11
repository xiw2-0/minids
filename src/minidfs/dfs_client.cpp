/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implentation of class DFSClient.

#include <minidfs/dfs_client.hpp>

namespace minidfs {

DFSClient::DFSClient(const string& serverIP, int serverPort, int buf)
    : master(new rpc::ClientProtocolProxy(serverIP, serverPort)),
      masterIP(serverIP), masterPort(serverPort), BUFFER_SIZE(buf) {
}

DFSClient::~DFSClient() {
}

int DFSClient::getFile(const string& src, const string& dst) {
  RemoteReader reader(masterIP, masterPort, src, BUFFER_SIZE);
  if (-1 == reader.open()) {
    return -1;
  }

  std::ofstream f(src, std::ios::out | std::ios::binary | std::ios::trunc);
  reader.readAll(f);

  f.clear();
  f.close();
  return 0;
}

RemoteReader DFSClient::getReader(const string& src) {
  return RemoteReader(masterIP, masterPort, src, BUFFER_SIZE);
}

} // namespace minidfs

