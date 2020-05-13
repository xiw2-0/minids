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

int DFSClient::remove(const string& filename) {
  int retOp =  master->remove(filename);
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[DFSClient] "  << "Failed to remove " << filename << std::endl
         << "Error code "  << retOp << std::endl;
    return -1;
  }
  return 0;
}

int DFSClient::exists(const string& file) {
  int retOp =  master->exists(file);
  if (retOp == OpCode::OP_FAILURE) {
    cerr << "[DFSClient] "  << "Failed to query " << file << std::endl;
    return -1;
  }
  if (retOp == OpCode::OP_EXIST) {
    cerr << "[DFSClient] "  << file << "exists" << std::endl;
  } else {
    cerr << "[DFSClient] "  << file << "doesn't exist" << std::endl;
  }
  
  return 0;
}

int DFSClient::mkdir(const string& dirname) {
  int retOp =  master->makeDir(dirname);
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[DFSClient] "  << "Failed to create " << dirname << std::endl
         << "Error code "  << retOp << std::endl;
    return -1;
  }
  return 0;
}

int DFSClient::ls(const string& dirname, std::vector<FileInfo>& items) {
  FileInfos infos;
  int retOp = master->listDir(dirname, infos);
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[DFSClient] "  << "Failed to ls " << dirname << std::endl
         << "Error code "  << retOp << std::endl;
    return -1;
  }

  for(int i = 0; i < infos.fileinfos_size(); ++i) {
    items.push_back(infos.fileinfos(i));
  }
  return 0;
}

} // namespace minidfs

