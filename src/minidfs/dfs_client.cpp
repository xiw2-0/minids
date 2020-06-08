/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implentation of class DFSClient.

#include <minidfs/dfs_client.hpp>
#include "logging/logger.h"

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
  if (f.is_open() == false) {
    LOG_ERROR << "Failed to open " << src;
    f.clear();
    f.close();
    return -1;
  }

  if (-1 == writer.writeAll(f)) {
    return -1;
  }
  if (-1 == writer.remoteClose()) {
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

  std::ofstream f(dst, std::ios::out | std::ios::binary | std::ios::trunc);
  if (f.is_open() == false) {
    LOG_ERROR << "Failed to open " << dst;
    f.clear();
    f.close();
    return -1;
  }
  if (-1 == reader.readAll(f)) {
    f.clear();
    f.close();
    return -1;
  }

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
    LOG_ERROR  << "Failed to remove " << filename
         << "Error code "  << retOp;
    return -1;
  }
  return 0;
}

int DFSClient::exists(const string& file) {
  int retOp =  master->exists(file);
  if (retOp == OpCode::OP_FAILURE) {
    LOG_ERROR  << "Failed to query " << file;
    return -1;
  }
  if (retOp == OpCode::OP_EXIST) {
    LOG_INFO  << file << " exists";
  } else {
    LOG_INFO  << file << " doesn't exist";
  }
  
  return 0;
}

int DFSClient::mkdir(const string& dirname) {
  int retOp =  master->makeDir(dirname);
  if (retOp != OpCode::OP_SUCCESS) {
    LOG_ERROR  << "Failed to create " << dirname
         << "Error code "  << retOp;
    return -1;
  }
  return 0;
}

int DFSClient::ls(const string& dirname, std::vector<FileInfo>& items) {
  FileInfos infos;
  int retOp = master->listDir(dirname, infos);
  if (retOp != OpCode::OP_SUCCESS) {
    LOG_ERROR  << "Failed to ls " << dirname
         << "Error code "  << retOp;
    return -1;
  }

  for(int i = 0; i < infos.fileinfos_size(); ++i) {
    items.push_back(infos.fileinfos(i));
  }
  return 0;
}

} // namespace minidfs

