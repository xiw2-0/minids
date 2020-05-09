/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSClient.

#ifndef DFS_CLIENT_H_
#define DFS_CLIENT_H_

#include <string>
#include <minidfs/client_protocol.hpp>
#include <rpc/client_protocol_proxy.hpp>

using std::string;

namespace minidfs {

/// \brief DFSClient is used to communicate with Master and Chunkserver.
///
/// It communicates with Master node to get/set the metadata of files/directories.
/// It interacts with Chunkserver to send/recv chunk data.
class DFSClient {
 private:
  /// 
  std::unique_ptr<ClientProtocol> master;

 public:
  /// \brief Create a DFSClient given the Master's IP and port.
  DFSClient(const string& serverIP, int serverPort);

  ~DFSClient();

  /// \brief Get a file's block location information from Master.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  int getBlockLocations(const string& file, LocatedBlocks* locatedBlks);


  /// \brief Put a local file to the distributed file system
  int putFile(const string& src, const string& dst);

  /// \brief Copy a file in the distributed file system to the local fs
  int getFile(const string& src, const string& dst);

  uint64_t read(const string& filename, void* buffer, uint64_t offset, uint64_t size);

  /// 
  int remove(const string& filename);

  int mkdir(const string& dirname);

  /// \brief List the 
  int ls(const string& dirname, std::vector<string>& items);








 private:
  /// \brief Create a file. MethodID = 2.
  ///
  /// This operation informs Master to create the meta info for the first block
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  int create(const string& file, LocatedBlock* locatedBlk);

};