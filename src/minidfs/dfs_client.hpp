/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSClient.

#ifndef DFS_CLIENT_H_
#define DFS_CLIENT_H_

#include <string>

#include <minidfs/client_protocol.hpp>
#include <minidfs/remote_reader.hpp>
#include <minidfs/remote_writer.hpp>
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
  /// master is used to communicate with the Master
  std::unique_ptr<ClientProtocol> master;

  string masterIP;
  int masterPort;

  const int BUFFER_SIZE;
  const string bufferBlkName;

  const long long blockSize;

 public:
 
  /// \brief Create a DFSClient given the Master's IP and port.
  DFSClient(const string& serverIP, int serverPort, int buf,
            const string& bufferBlkName, const long long blockSize);

  ~DFSClient();


  /// \brief Put a local file to the distributed file system
  ///
  /// \param src source file in local fs
  /// \param dst target file in dfs
  int putFile(const string& src, const string& dst);

  /// \brief Copy a file in the distributed file system to the local fs
  ///
  /// \param src source file in dfs
  /// \param dst target file in local fs
  /// \return return 0 on success, -1 for errors
  int getFile(const string& src, const string& dst);

  /// \brief Given the source file name, get a reader directly.
  /// Call open() before using it!
  ///
  /// \param src source file in dfs
  /// \return remote reader of src
  RemoteReader getReader(const string& src);

  /// \brief Given the target file name, get a writer directly.
  /// Call open() before using it and close() after using it!
  ///
  /// \param dst target file in dfs
  /// \return remote writer of dst
  RemoteWriter getWriter(const string& dst);

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