/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <rpc/chunkserver_protocol_proxy.hpp>
#include "logging/logger.h"


namespace rpc {

ChunkserverProtocolProxy::ChunkserverProtocolProxy(const string& serverIP, int serverPort)
    : client(RPCClient(serverIP, serverPort)) {
}


int ChunkserverProtocolProxy::heartBeat(const minidfs::ChunkserverInfo& chunkserverInfo){
  /// connect Master
  if (client.connectMaster() < 0) {
    LOG_ERROR  << "Connection to master failed";
    return OpCode::OP_FAILURE;
  }

  /// send the request
  if (client.sendRequest(101, chunkserverInfo.SerializeAsString()) < 0) {
    LOG_ERROR  << "Failed to send heart beat";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();

  LOG_DEBUG << "Send heartbeat rpc successfully";
  return status;
}

int ChunkserverProtocolProxy::blkReport(const minidfs::ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) {
  /// connect Master
  if (client.connectMaster() < 0) {
    LOG_ERROR  << "Connection to master failed";
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
    LOG_ERROR  << "Failed to send blk report";
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

  LOG_DEBUG << "Send blk report rpc successfully";
  return status;
}

int ChunkserverProtocolProxy::getBlkTask(const minidfs::ChunkserverInfo& chunkserverInfo, minidfs::BlockTasks* blkTasks) {
  /// connect Master
  if (client.connectMaster() < 0) {
    LOG_ERROR << "Connection to master failed";
    return OpCode::OP_FAILURE;
  }

  /// send the request
  if (client.sendRequest(103, chunkserverInfo.SerializeAsString()) < 0) {
    LOG_ERROR << "Failed to send get blk task request";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  blkTasks->ParseFromString(response);

  /// close the connection
  client.closeConnection();

  LOG_DEBUG << "Send get blk task rpc successfully";
  return status;
}

int ChunkserverProtocolProxy::recvedBlks(const minidfs::ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs) {
  /// connect Master
  if (client.connectMaster() < 0) {
    LOG_ERROR << "Connection to master failed";
    return OpCode::OP_FAILURE;
  }

  /// construct the request
  minidfs::BlockReport blkReport;
  *blkReport.mutable_chunkserverinfo() = chunkserverInfo;
  for (int blkid : blkIDs) {
    blkReport.add_blkids(blkid);
  }

  /// send the request
  if (client.sendRequest(104, blkReport.SerializeAsString()) < 0) {
    LOG_ERROR << "Failed to send recvedBlks rpc request";
    return OpCode::OP_FAILURE;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  
  /// close the connection
  client.closeConnection();

  LOG_DEBUG << "Send recved blks rpc successfully";
  return status;
}


} // namespace rpc