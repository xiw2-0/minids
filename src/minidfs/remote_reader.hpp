/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RemoteReader.

#ifndef REMOTE_READER_H_
#define REMOTE_READER_H_

#include <string>
#include <minidfs/client_protocol.hpp>
#include <rpc/client_protocol_proxy.hpp>

using std::string;

namespace minidfs {


/// RemoteReader opens a file in dfs and reads the data from it.

class RemoteReader {
 private:
  /// this is used to communicate with Master
  std::unique_ptr<ClientProtocol> master;
  
 public:
  RemoteReader(/* args */);
  ~RemoteReader();

  uint64_t read(void* buffer, uint64_t offset, uint64_t size);

 private:
  void open(const string& filename);


  /// \brief Get a file's block location information from Master.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  int getBlockLocations(const string& file, LocatedBlocks* locatedBlks);
};


} // namespace minidfs

#endif