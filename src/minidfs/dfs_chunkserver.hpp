/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSChunkserver.

#ifndef DFS_CHUNKSERVER_H_
#define DFS_CHUNKSERVER_H_

#include <dirent.h>
#include <string>
#include <thread>

#include <minidfs/chunkserver_protocol.hpp>
#include <rpc/chunkserver_protocol_proxy.hpp>

using std::string;


namespace minidfs {

/// \brief DFSChunkserver sends heartbeat/report to master. And it
/// sends/receives data from client/another chunkserver.
/// It has two threads. In the main thread, it communicates with master.
/// The other thread is running as a deamon to do data block related work.
/// The second thread serves at a specific port and waits the data request
/// from clients/chunkservers. The second thread will fork a new thread
/// each time to deal with the new-coming request.
/// TODO: xiw, Thread pool will be used here, if time is available.
class DFSChunkserver {

 private:
  /// master is used to communicate with Master.
  std::unique_ptr<ChunkserverProtocol> master;

  /// the port where chunkserver serves
  int serverPort;
  /// the IP where chunkserver serves
  string serverIP;

  int maxConnections;

  ChunkserverInfo chunkserverInfo;

  /// folder that stores the blocks
  string dataDir;
  /// store the block ids
  std::set<int> servedBlks;

  /// block size
  long long blockSize;

 public:
  /// \brief Create a DFSChunkserver.
  ///
  /// \param masterIP the IP of master
  /// \param masterPort the port of master
  /// \param serverIP IP of the chunkserver's host
  /// \param serverPort port of chunkserver
  /// \param dataDir folder which contains the stored blocks
  DFSChunkserver(const string& masterIP, int masterPort,
                 const string& serverIP, int serverPort,
                 const string& dataDir, long long blkSize,
                 int maxConnections);

  /// \brief The chunkserver will run and exit only when this program is shut down.
  /// It provides services to clients to handle data writing/reading requests.
  /// The data service is running as a daemon thread.
  /// It sends heartbeat/blockreport to the master periodically. 
  void run();

 private:

  /// \brief This method will provide data sending/receiving service to clients/chunkservers.
  /// It runs as a daemon thread.
  void dataService();


 private:
  /// \brief Scan the stored blocks.
  ///
  /// \return Opcode
  int scanStoredBlocks();

  /// \brief Handle block reading/writing requests.
  ///
  /// \param connfd the received socket fd
  void handleBlockRequest(int connfd);

  /// \brief Receive a block from client/chunkserver and forward it to other chunkservers
  /// if necessary.
  int recvBlock();

  /// \brief Send a block back to client. 
  int sendBlock();

  /// \brief This method is called when the chunkserver gets a block copy task
  /// from master. It sends a block to the targeted chunkservers.
  int replicateBlock();


  /// \brief Send heartbeat to Master.
  int heartBeat();

  /// \brief Send block report to master. It is revoked as the start of chunkserver.
  /// And it is revoked every BLOCK_REPORT_PERIOD.
  int blkReport();

  /// \brief Get block tasks from master. Usually, they are copy tasks because some blocks
  /// broke or some chunkservers died.
  int getBlkTask();

  /// \brief Invoked when the chunkserver receives new blocks from clients/chunkservers.
  int recvedBlks();
};





} // namespace minidfs

#endif