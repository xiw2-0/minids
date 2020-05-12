/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implentation of class DFSClient.

#include <minidfs/dfs_client.hpp>

namespace minidfs {

DFSClient::DFSClient(const string& serverIP, int serverPort, int buf,
                     const string& bufferBlkName, const long long blockSize)
    : master(new rpc::ClientProtocolProxy(serverIP, serverPort)),
      masterIP(serverIP), masterPort(serverPort), BUFFER_SIZE(buf),
      bufferBlkName(bufferBlkName), blockSize(blockSize) {
}

DFSClient::~DFSClient() {
}

int DFSClient::putFile(const string& src, const string& dst) {
  RemoteWriter writer(masterIP, masterPort, dst, BUFFER_SIZE, 2,
                      blockSize, bufferBlkName);
  if (-1 == writer.open()){
    return -1;
  }
  std::ifstream f(src, std::ios::in | std::ios::binary);
  if (-1 == writer.writeAll(f)) {
    return -1;
  }
  if (-1 == writer.close()) {
    return -1;
  }

  f.clear();
  f.close();
  return 0;
}

int DFSClient::getFile(const string& src, const string& dst) {
  RemoteReader reader(masterIP, masterPort, src, BUFFER_SIZE, bufferBlkName);
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
  return RemoteReader(masterIP, masterPort, src, BUFFER_SIZE, bufferBlkName);
}

RemoteWriter DFSClient::getWriter(const string& dst) {
  return RemoteWriter(masterIP, masterPort, dst, BUFFER_SIZE, 2,
                      blockSize, bufferBlkName);
}

} // namespace minidfs

