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
#include <fstream>
#include <chrono>

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
///
/// Block writing request format:
/// OP_WRITE : len(LocatedBlock) : LocatedBlock : len(data) : data
/// 1 byte   : 2 bytes           : n bytes      : 8 bytes   : n bytes
///
/// Block reading request format:
/// OP_READ  : len(Block) : Block 
/// 1 byte   : 2 bytes    : n bytes
///
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
  std::set<int> blksServed;

  /// recently received blocks
  std::set<int> blksRecved;

  /// block size
  long long blockSize;

  /// data sending/recving buffer size
  const int BUFFER_SIZE;

  /// heart beat interval, in ms
  const long long HEART_BEAT_INTERVAL;

  /// block report interval, in ms
  const long long BLOCK_REPORT_INTERVAL;

  /// at the startup, don't retrieve block tasks from master, in ms
  const long long BLK_TASK_STARTUP_INTERVAL;

 public:
  /// \brief Create a DFSChunkserver.
  ///
  /// \param masterIP the IP of master
  /// \param masterPort the port of master
  /// \param serverIP IP of the chunkserver's host
  /// \param serverPort port of chunkserver
  /// \param dataDir folder which contains the stored blocks
  /// \param blkSize preferred block size
  /// \param maxConnections the max connections from clients / other chunkservers
  /// \param BUFFER_SIZE the data sending/receiving buffer size
  /// \param HEART_BEAT_INTERVAL heartbeat interval
  /// \param BLOCK_REPORT_INTERVAL block report interval
  /// \param BLK_TASK_STARTUP_INTERVAL time period before it is available to fetch block tasks
  DFSChunkserver(const string& masterIP, int masterPort,
                 const string& serverIP, int serverPort,
                 const string& dataDir, long long blkSize,
                 int maxConnections, int BUFFER_SIZE,
                 long long HEART_BEAT_INTERVAL,
                 long long BLOCK_REPORT_INTERVAL,
                 long long BLK_TASK_STARTUP_INTERVAL);

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
  ///
  /// Block writing request format:
  /// OP_WRITE : len(LocatedBlock) : LocatedBlock : len(data) : data
  /// 1 byte   : 2 bytes           : n bytes      : 8 bytes   : n bytes
  ///
  /// Response format:
  /// OpCode   :
  /// 1 byte   :
  ///
  /// \param connfd the received socket fd
  /// \return return 0 on success, -1 for errors
  int recvBlock(int connfd);

  /// \brief Send a block back to client. 
  ///
  /// Block reading request format:
  /// OP_READ  : len(Block) : Block 
  /// 1 byte   : 2 bytes    : n bytes
  ///
  /// Response format:
  /// OpCode   : len(data) : data
  /// 1 byte   : 8 bytes   : n bytes
  ///
  /// \param connfd the received socket fd
  /// \return return 0 on success, -1 for errors
  int sendBlock(int connfd);

  /// \brief This method is called when the chunkserver gets a block copy task
  /// from master. It sends a block to the targeted chunkservers.
  ///
  /// In this method, the chunkserver connects the target remote server firstly.
  /// Then it sends the data writing request and data to the remote.
  ///
  /// Block writing request format:
  /// OP_WRITE : len(LocatedBlock) : LocatedBlock : len(data) : data
  /// 1 byte   : 2 bytes           : n bytes      : 8 bytes   : n bytes
  ///
  /// Response format:
  /// OpCode   :
  /// 1 byte   :
  ///
  /// \param locatedB contains info about the block and remote chunkserver
  /// \return return 0 on success, -1 for errors
  int replicateBlock(const LocatedBlock& locatedB);

  /// \brief Send heartbeat to Master.
  ///
  /// \return return OpCode.
  int heartBeat();

  /// \brief Send block report to master. It is revoked as the start of chunkserver.
  /// And it is revoked every BLOCK_REPORT_PERIOD. It gets invalid blocks list and
  /// delete them.
  /// 
  /// \return return OpCode.
  int blkReport();

  /// \brief Get block tasks from master. Usually, they are copy tasks because some blocks
  /// broke or some chunkservers died.
  /// 
  /// \return return OpCode.
  int getBlkTask();

  /// \brief Invoked when the chunkserver receives new blocks from clients/chunkservers.
  /// 
  /// \return return OpCode.
  int recvedBlks();

 private:
  /// \brief Send block data to client/chunkserver through the connected socket.
  ///
  /// Block sending format:
  /// len(data) : data
  /// 8 bytes   : n bytes
  ///
  /// \param connfd connected socket fd, either from client or to another chunkserver
  /// \param bID ID of the block to be sent
  /// \return return 0 on success, -1 for errors
  int sendBlkData(int connfd, int bID);

  /// \brief Connect the remote Chunkserver.
  ///
  /// \param serverIP remote chunkserver ip
  /// \param serverPort remote chunkserver port
  /// \return return connected socket fd on success, -1 for errors.
  int connectRemote(string serverIP, int serverPort);
};





} // namespace minidfs

#endif