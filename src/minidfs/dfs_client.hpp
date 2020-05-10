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

/// \brief DFSClient provides the interface for users to 
/// read a file in dfs or to write a file to dfs. And it also
/// provides the methods to manage the dfs name system, e.g.
/// mkdir, ls.
///
/// Recently, it has the following functions:
/// 1) put a file from local to dfs
/// 2) get a file from dfs
/// 3) get a reader/writer of a file directly
/// 4) remove a file
/// 5) to know whether a file/directory exists
/// 6) make a folder
/// 7) list all the items in a folder
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


  /// \brief Put a local file to the distributed file system
  int putFile(const string& src, const string& dst);

  /// \brief Copy a file in the distributed file system to the local fs
  int getFile(const string& src, const string& dst);

  /// \brief 
  void getReader(const string& src);

  void getWriter(const string& dst);

  /// 
  int remove(const string& filename);

  int exists(const string& file);

  int mkdir(const string& dirname);

  /// \brief List the items contained in the dirname
  int ls(const string& dirname, std::vector<FileInfo>& items);








 private:

};





} // namespace minidfs

#endif