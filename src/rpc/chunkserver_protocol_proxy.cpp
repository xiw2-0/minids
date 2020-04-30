/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <rpc/chunkserver_protocol_proxy.hpp>



namespace rpc {

ChunkserverProtocolProxy::ChunkserverProtocolProxy(const string& serverIP, int serverPort)
    : client(RPCClient(serverIP, serverPort)) {
}


int ChunkserverProtocolProxy::heartBeat(const minidfs::ChunkserverInfo& chunkserverInfo){
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Connection failed\n";
    return OpCode::OP_FAILURE;
  }

  /// send the request
  if (client.sendRequest(101, chunkserverInfo.SerializeAsString()) < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Failed to send request\n";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();

  return status;
}

int ChunkserverProtocolProxy::blkReport(const minidfs::ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) {
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Connection failed\n";
    return OpCode::OP_FAILURE;
  }

  /// construct the request
  minidfs::BlockReport blkReport;
  *blkReport.mutable_chunkserverinfo() = chunkserverInfo;
  for (int blkid : blkIDs) {
    blkReport.add_blkids(blkid);
  }

  /// send the request
  if (client.sendRequest(102, blkReport.SerializeAsString()) < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Failed to send request\n";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  minidfs::BlkIDs deletedBlkIDs;
  deletedBlkIDs.ParseFromString(response);

  for (int i = 0; i < deletedBlkIDs.blkids_size(); ++i) {
    deletedBlks.push_back(deletedBlkIDs.blkids(i));
  }
  
  /// close the connection
  client.closeConnection();

  return status;
}

int ChunkserverProtocolProxy::getBlkTask(const minidfs::ChunkserverInfo& chunkserverInfo, minidfs::BlockTasks* blkTasks) {
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Connection failed\n";
    return OpCode::OP_FAILURE;
  }

  /// send the request
  if (client.sendRequest(101, chunkserverInfo.SerializeAsString()) < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Failed to send request\n";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  blkTasks->ParseFromString(response);

  /// close the connection
  client.closeConnection();

  return status;
}

int ChunkserverProtocolProxy::recvedBlks(const minidfs::ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs) {
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Connection failed\n";
    return OpCode::OP_FAILURE;
  }

  /// construct the request
  minidfs::BlockReport blkReport;
  *blkReport.mutable_chunkserverinfo() = chunkserverInfo;
  for (int blkid : blkIDs) {
    blkReport.add_blkids(blkid);
  }

  /// send the request
  if (client.sendRequest(102, blkReport.SerializeAsString()) < 0) {
    cerr << "[ChunkserverProtocolProxy] "  << "Failed to send request\n";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  
  /// close the connection
  client.closeConnection();

  return status;
}


} // namespace rpc