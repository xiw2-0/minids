/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for ChunkserverProtocolProxy

#ifndef CHUNKSERVER_PROTOCOL_PROXY_H_
#define CHUNKSERVER_PROTOCOL_PROXY_H_

#include <minidfs/chunkserver_protocol.hpp>
#include <minidfs/op_code.hpp>
#include <proto/minidfs.pb.h>
#include <rpc/rpc_client.hpp>

using minidfs::OpCode;

namespace rpc {

/// \brief ChunkserverProtocol is the communication protocol between DFSChunkserver with Master.
///
/// Chunkserver sends hearbeat to inform the Master that it's still alive.
/// Chunkserver sends blockreport to tell the Master about all the blocks it has.
/// This class is just an interface.
class ChunkserverProtocolProxy: public minidfs::ChunkserverProtocol {
 private:
  RPCClient client;

 public:
  ChunkserverProtocolProxy(const string& serverIP, int serverPort);

  /// \brief Send heartbeat information to Master. MethodID = 101.
  ///
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \return return OpCode.
  virtual int heartBeat(const minidfs::ChunkserverInfo& chunkserverInfo) override;

  /// \brief Send block report to Master. MethodID = 102.
  ///
  /// The chunkserver informs Master about all the blocks it has
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkIDs all the block ids it has
  /// \param deletedBlks all the blocks it should delete
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int blkReport(const minidfs::ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) override;

  /// \brief Get block task from Master. MethodID = 103.
  ///
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkTasks block tasks from master. 
  ///        It is the returning parameter.      
  /// \return return OpCode.
  virtual int getBlkTask(const minidfs::ChunkserverInfo& chunkserverInfo, minidfs::BlockTasks* blkTasks) override;

  /// \brief Inform Master about the received blocks. MethodID = 104.
  ///
  /// The chunkserver informs Master about all the blocks it received
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkIDs all the block ids it received
  /// \return return OpCode.
  virtual int recvedBlks(const minidfs::ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs) override;

};

} // namespace rpc
    
#endif