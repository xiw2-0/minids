/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RemoteWriter.

#ifndef REMOTE_WRITER_H_
#define REMOTE_WRITER_H_

#include <string>
#include <minidfs/client_protocol.hpp>
#include <rpc/client_protocol_proxy.hpp>

using std::string;

namespace minidfs {

class RemoteWriter {
 private:
  /// this is used to communicate with Master
  std::unique_ptr<ClientProtocol> master;

  /// each reader serves for only one file
  const string filename;

 public:
  RemoteWriter(/* args */);
  ~RemoteWriter();
  
  /// \brief Connect the master to get the block information.
  ///
  /// \return return 0 on success, -1 for errors
  int open();

  /// \brief Write size bytes of data with offset from buffer into the file in dfs.
  /// The default offset means to append data to the file.
  ///
  /// \param buffer data buffer to be written
  /// \param size size of data to be written
  /// \param offset where to start writting, default is to append at the end of the file.
  /// \return size of data read  
  uint64_t write(const void* buffer, uint64_t size, uint64_t offset = UINT64_MAX);

 private:
  

  
};







} // namespace minidfs

#endif