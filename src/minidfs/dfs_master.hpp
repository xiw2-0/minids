/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSMaster.

#ifndef DFS_MASTER_
#define DFS_MASTER_

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <random>
#include <atomic>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <minidfs/chunkserver_protocol.hpp>
#include <minidfs/client_protocol.hpp>
#include <minidfs/op_code.hpp>
#include <proto/minidfs.pb.h>
#include <rpc/rpc_server.hpp>


namespace minidfs {

/// \brief ChunkserverInfo Hasher is used when ChunkserverInfo is taken as
/// the key of std::unordered_set/std::unordered_map
class ChunkserverInfoHasher {
 public:
  std::size_t operator()(const ChunkserverInfo &key) const{
		using std::size_t;
		using std::hash;
 
		return ((hash<string>()(key.chunkserverip())
			     ^ (hash<int>()(key.chunkserverport()) << 1)) >> 1);
	}
};

/// \brief ChunkserverInfo EqualTo is used when ChunkserverInfo is taken as
/// the key of std::unordered_set/std::unordered_map
class ChunkserverInfoEqualTo {
 public:
	bool operator()(const ChunkserverInfo &lhs, const ChunkserverInfo &rhs) const {
		return lhs.chunkserverip()  == rhs.chunkserverip()
			     && lhs.chunkserverport() == rhs.chunkserverport();
	}
};

/// \brief ChunkserverInfo LessThan is used when ChunkserverInfo is taken as
/// the key of std::set/std::map
class ChunkserverInfoLessThan {
 public:
	bool operator()(const ChunkserverInfo &lhs, const ChunkserverInfo &rhs) const {
    if (lhs.chunkserverip() == rhs.chunkserverip()) {
      return lhs.chunkserverport() < rhs.chunkserverport();
    }
		return lhs.chunkserverip()  < rhs.chunkserverip();
	}
};

/// \brief DFSMaster implements server-end of \interface ClientProtocol 
/// and \interface ChunkserverProtocol. It responds to the rpc calls from
/// \class DFSClient and \class DFSChunkserver.
///
/// Master receives rpc calls from dfs client and dfs chunkservers.
/// When master receives a new socket, it generates a new thread to handle
/// this request.
/// Locking orders: mutexFileNameSys > mutexMemoryNameSys > 
///                 mutexChunkserverBlock > mutexInCreating
/// currentMaxDfID and currentMaxBlkID use atomic int
class DFSMaster: public ClientProtocol, public ChunkserverProtocol{
 private:

  /// namesystem file
  string nameSysFile;

  /// editlog file
  string editLogFile;

  /// current editlog id
  std::atomic<int> editlogID;

  ///#1 mutex for name system in disk
  std::recursive_mutex mutexFileNameSys;

  /// Server waits for the rpcs call and forwards the calls to master.
  rpc::RPCServer server;

  ///#2 mutex for name system in memory
  std::recursive_mutex mutexMemoryNameSys;

  /// \brief Maps from directory/file name string to dfID (directory/file id)
  ///
  /// The string name here needs to be full name, e.g. "/data/dfs/file.txt".
  /// "file.txt" only is not allowed.
  /// This will be serialized to local disk.
  std::unordered_map<string, int> dfIDs;

  /// The max dfID that has been allocated
  std::atomic<int> currentMaxDfID;

  /// inversion of dfIDs
  std::unordered_map<int, string> dfNames;

  /// \brief Maps from directory ID to subdirs'/files' ID
  ///
  /// This will be serialized to local disk.
  std::unordered_map<int, std::vector<int>> dentries;

  /// \brief Maps from file id to block ids. Dirs are not included here.
  ///
  /// This will be serialized to local disk.
  std::unordered_map<int, std::vector<int>> inodes;

  /// The max block ID that has been allocated
  std::atomic<int> currentMaxBlkID;

  /// \brief Maps from block id to blocks.
  std::unordered_map<int, Block> blks;

  ///#3 mutex for chunkserver-block related information
  std::recursive_mutex mutexChunkserverBlock;

  /// Maps from block id to chunkservers
  std::unordered_map<int, std::vector<ChunkserverInfo>> blkLocs;

  /// alive chunkservers
  std::unordered_map<ChunkserverInfo, bool, ChunkserverInfoHasher, ChunkserverInfoEqualTo> aliveChunkservers;

  /// blks that need to be replicated.
  /// The 1st is block id; the 2nd is replication factor.
  std::unordered_map<int, int> blksToBeReplicated;

  ///#4 mutex for files/blocks in creating status
  std::recursive_mutex mutexInCreating;

  /// record the set of file names still in creating process, not finish yet.
  /// the 1st element is file name, the 2nd is a list of block ids
  std::unordered_map<string, std::vector<int>> filesInCreating;

  /// record the set of blocks still in creating process, not finish yet.
  /// The associated chunkserver info is included
  /// (block id, located block) pair
  std::unordered_map<int, LocatedBlock> blocksInCreating;

 private:
  /// number of replicas
  int replicationFactor;

  /// magic code of edit log
  const int editlogMagicCode = 1234;

  /// max number of edit log
  const int maxEditLogEntry = 5;

  /// status checking interval, in ms
  const int STATUS_CHECK_INTERVAL = 7000;


 public:
  /// \brief Construct the Master.
  ///
  /// \param nameSysFile file name of name system
  /// \param editLogFile file name of edit log
  /// \param serverPort Master's serving port
  /// \param maxConns maximum number of connections
  /// \param replicationFactor number of replicas for every block
  DFSMaster(const string& nameSysFile, const string& editLogFile,
            int serverPort, int maxConns, int replicationFactor);

  ~DFSMaster();

  /// \brief Format the name system
  ///
  /// Reserve only the root dir and delete all the others.
  /// Serialize the name system into local disk.
  int format();

  /// \brief First, this method call inits the Master. Then it
  /// enters the safemode. Finally, it offers service to clients.
  ///
  /// In the first step, the Master read the persistent file 
  /// from local storage. Then, it listens the block reports from
  /// datanodes. In this second phase, the Master doesn't offer
  /// service to clients. When the Master feels safe to 
  /// start providing service to clients, it starts to respond to clients.
  void startRun();

  /// \brief Take a snapshot of the name system and store it to local disk.
  /// At the same time, edit log is reset to empty file.
  int checkpoint();

  /// \brief Is it safe to recv requests from clients?
  /// Safe when there is at least one chunkserver alive for each blk. 
  bool isSafe();

  /// \brief Check whether all chunkservers are still alive.
  /// Check the edit log size. Check the status every STATUS_CHECK_INTERVAL.
  /// It runs as a daemon thread to find dead nodes.
  void statusChecker();

  ////////////////////////
  /// ClientProtocol
  ////////////////////////


  ////////////////// File reading/writing


  /// \brief Get a file's block location information from Master. MethodID = 1.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int getBlockLocations(const string& file, LocatedBlocks* locatedBlks) override;

  /// \brief Create a file. MethodID = 2.
  ///
  /// This operation informs Master to create the meta info for the first block.
  /// Generally, the client will call addBlock() and complete() after create() call.
  /// When the client creates a file,
  /// the master should add it to filesInCreating first.
  /// When the master receives the complete() RPC call, it
  /// finalizes the file into the namesystem and assigns a new
  /// DfID to the newly created file.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int create(const string& file, LocatedBlock* locatedBlk) override;

  /// \brief Add a block when the client has finished the previous block. MethodID = 3.
  ///
  /// When the client create() a file and finishes the 1st block, it calls addBlock()
  /// to add a block to continue writing the rest of the file.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int addBlock(const string& file, LocatedBlock* locatedBlk) override;

  /// \brief Confirm that a block has been written successfully. MethodID = 4.
  ///
  /// When the client create()/addBlock() successfully, it should send ack to
  /// inform the master. This request will let the master know how much data the
  /// client has written.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information. It contains
  ///        the information of written block.
  /// \return return OpCode.
  virtual int blockAck(const LocatedBlock& locatedBlk) override;  

  /// \brief Complete writing a file. It should be called when the client has completed writing. MethodID = 5.
  ///
  /// calling order: create() [-> addBlock()] -> complete()
  /// This is the last step when creating a file. It is called when the client has finished writing.
  /// This operation will log the change of name system to the local disk.
  ///
  /// \param file the file name stored in minidfs.
  /// \return return OpCode.
  virtual int complete(const string& file) override;


  /////////////////////////////// Name system operations


  /// \brief Remove/Delete a file. MethodID = 11.
  ///
  /// \param file the file to be removed.
  /// \return return OpCode.
  virtual int remove(const string& file) override;

  /// \brief To tell whether a file exists. MethodID = 12.
  ///
  /// \param file the query file.
  /// \return return OpCode.
  virtual int exists(const string& file) override;

  /// \brief Create a new folder. MethodID = 13.
  ///
  /// \param dirName the folder name stored in minidfs.
  /// \return return OpCode.
  virtual int makeDir(const string& dirName) override;

  /// \brief List items contained in a given folder. MethodID = 14.
  ///
  /// \param dirName the folder name stored in minidfs.
  /// \param items items contained in dirName. Each item describes info about a file.
  ///        It is the returning parameter.
  /// \return return OpCode.
  virtual int listDir(const string& dirName, FileInfos& items) override;
  
  
  ////////////////////////
  /// ChunkserverProtocol
  ////////////////////////


  /// \brief Send heartbeat information to Master. MethodID = 101.
  ///
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \return return OpCode.
  virtual int heartBeat(const ChunkserverInfo& chunkserverInfo) override;

  /// \brief Send block report to Master. MethodID = 102.
  ///
  /// The chunkserver informs Master about all the blocks it has
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkIDs all the block ids it has
  /// \param deletedBlks all the blocks it should delete
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int blkReport(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) override;
  
  /// \brief Get block task from Master. MethodID = 103.
  ///
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkTasks block tasks from master. 
  ///        It is the returning parameter.      
  /// \return return OpCode.
  virtual int getBlkTask(const ChunkserverInfo& chunkserverInfo, BlockTasks* blkTasks) override;
  
  /// \brief Inform Master about the received blocks. MethodID = 104.
  ///
  /// The chunkserver informs Master about all the blocks it received
  /// \param chunkserverInfo containing the ip and port of the chunkserver
  /// \param blkIDs all the block ids it received
  /// \return return OpCode.
  virtual int recvedBlks(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs) override;
 
 private:
  /// \brief Serialize the fdIDs/inodes/dentries to local disk.
  int serializeNameSystem();

  /// \brief Parse the fdIDs/inodes/dentries from local disk.
  int parseNameSystem();

  /// \brief Init Master. Parse the persistent name system.
  int initMater();

 private:
  /// Utils function

  /// Split the path to get the parent dir
  void splitPath(const string& path, string& dir);

  /// tranform chunkserverinfo into chunkserver id
  //int getChunkserverID(const ChunkserverInfo& chunkserverInfo);

  /// find blocks to be replicated
  void findBlksToBeReplicated(const ChunkserverInfo& chunkserver);

  /// distribute the blkTask
  void distributeBlkTask(int blockID, int repFactor, BlockTask* blkTask);

  /// Allocate chunkservers for a block
  ///
  /// \param cs the returned servers
  /// \return return 0 on success, -1 for errors
  int allocateChunkservers(std::vector<ChunkserverInfo>& cs);

  /// Get the length of a given file
  ///
  /// \param fileID the ID of the given file
  /// \return the total length of the given file; otherwise it returns -1
  long long getFileLength(int fileID);

  /// Log the edit into logeditFile.
  ///
  /// \param editString serialized EditLog
  /// \return return 0 on success, -1 for errors
  int logEdit(const string& editString);

  /// Replay the edit logs to recover the name system.
  ///
  /// \return return 0 on success, -1 for errors
  int replayEditLog();
};






} // namespace minidfs




#endif