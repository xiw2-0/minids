/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSMaster.

#ifndef DFS_MASTER_
#define DFS_MASTER_

#include <fstream>

#include <minidfs/client_protocol.hpp>
#include <proto/minidfs.pb.h>
#include <rpc/rpc_server.hpp>


namespace minidfs {

/// \brief DFSMaster implements server-end of \interface ClientProtocol 
/// and \interface ChunkserverProtocol. It responds to the rpc calls from
/// \class DFSClient and \class DFSChunkserver.
///
/// Master receives rpc calls from dfs client and dfs chunkservers.
/// When master receives a new socket, it generates a new thread to handle
/// this request.
/// TODO: xiW add read/write lock to avoid concurrent errors and thus to support concurrence.
class DFSMaster: public ClientProtocol{
 private:

  /// namesystem file
  string nameSysFile;


  /// Server waits for the rpcs call and forwards the calls to master.
  rpc::RPCServer server;

  /// \brief Maps from directory/file name string to dfID (directory/file id)
  ///
  /// The string name here needs to be full name, e.g. "/data/dfs/file.txt".
  /// "file.txt" only is not allowed.
  /// This will be serialized to local disk.
  std::map<string, int> dfIDs;
  /// The max dfID that has been allocated
  int currentMaxDfID;

  /// \brief Maps from directory ID to subdirs'/files' ID
  ///
  /// This will be serialized to local disk.
  std::map<int, std::vector<int>> dentries;

  /// \brief Maps from file id to block ids. Dirs are not included here.
  ///
  /// This will be serialized to local disk.
  std::map<int, std::vector<int>> inodes;
  /// The max block ID that has been allocated
  int currentMaxBlkID;
  /// \brief Maps from block id to blocks.
  std::map<int, Block> blks;

  /// Maps from block id to chunkservers
  /// TODO: xiw This can be changed to chunkserver id too.
  /// Maybe in later version.
  std::map<int, std::vector<string>> blkLocs;


 public:
  /// \brief Construct the Master.
  ///
  /// \param dfIDFile
  /// \param dentryFile
  /// \param inodeFile
  /// \param serverPort Master's serving port
  /// \param maxConns maximum number of connections
  DFSMaster(const string& nameSysFile,
            int serverPort, int maxConns);

  ~DFSMaster();

  /// \brief Init Master. Parse the persistent name system. Wait
  /// for the report from the chunkservers.
  void initMater();

  /// \brief News a thread to run \class RPCServer and waits it endlessly.
  void startRun();

  ///
  int checkpoint();

  /// \brief Get a file's block location information from Master. MethodID = 1.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return returning status. 0 on success, 1 for errors.
  virtual int getBlockLocations(const string& file, LocatedBlocks* locatedBlks) override;

  /// \brief Create a file. MethodID = 2.
  ///
  /// This operation informs Master to create the meta info for the first block
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return returning status. 0 on success, 1 for errors.
  virtual int create(const string& file, LocatedBlock* locatedBlk) override;
 
 
 
 private:
  /// \brief Serialize the fdIDs/inodes/dentries to local disk.
  int serializeNameSystem();

  /// \brief Parse the fdIDs/inodes/dentries from local disk.
  int parseNameSystem();
};




} // namespace minidfs




#endif