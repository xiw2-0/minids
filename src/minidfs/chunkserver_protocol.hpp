/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for Protocol b/w chunkservers and the master.

#ifndef CHUNKSERVER_PROTOCOL_H_
#define CHUNKSERVER_PROTOCOL_H_

#include <string>
#include <proto/minidfs.pb.h>

using std::string;

namespace minidfs {

/// \brief ChunkserverProtocol is the communication protocol between DFSChunkserver with Master.
///
/// Chunkserver sends hearbeat to inform the Master that it's still alive.
/// Chunkserver sends blockreport to tell the Master about all the blocks it has.
/// This class is just an interface.
class ChunkserverProtocol {
 public:
  /// \brief Send heartbeat information to Master. MethodID = 101.
  ///
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \return return OpCode. OpCode::OP_SUCCESS on success, OpCode::OP_FAILURE for error.
  virtual int heartBeat(const ChunkserverInfo& chunkserverInfo) = 0;

  /// \brief Send block report to Master. MethodID = 102.
  ///
  /// The chunkserver informs Master about all the blocks it has
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkIDs all the block ids it has
  /// \param deletedBlks all the blocks it should delete
  ///        It is the returning parameter. 
  /// \return return OpCode. OpCode::OP_SUCCESS on success, OpCode::OP_FAILURE for error.
  virtual int blkReport(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) = 0;

  /// \brief Get block task from Master. MethodID = 103.
  ///
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkTasks block tasks from master. 
  ///        It is the returning parameter.      
  /// \return return OpCode.
  virtual int getBlkTask(const ChunkserverInfo& chunkserverInfo, BlockTasks* blkTasks) = 0;

  /// \brief Inform Master about the received blocks. MethodID = 104.
  ///
  /// The chunkserver informs Master about all the blocks it received
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkIDs all the block ids it received
  /// \return return OpCode.
  virtual int recvedBlks(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs) = 0;

};

} // namespace minidfs
    
#endif