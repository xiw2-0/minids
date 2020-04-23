/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSMaster.

#ifndef DFS_MASTER_
#define DFS_MASTER_

#include <minidfs/client_protocol.hpp>

namespace minidfs {

/// \brief DFSMaster implements server-end of \interface ClientProtocol 
/// and \interface ChunkserverProtocol. It responds to the rpc calls from
/// \class DFSClient and \class DFSChunkserver.
///
/// Master receives rpc calls from dfs client and dfs chunkservers.
/// When master receives a new socket, it generates a new thread to handle
/// this request.
class DFSMaster: public ClientProtocol{
 private:
  /* data */
  /// RPCServer server;
 public:
  DFSMaster(/* args */);
  ~DFSMaster();

  /// \brief News a thread to run \class RPCServer and waits it endlessly.
  void startRun();
};




} // namespace minidfs




#endif