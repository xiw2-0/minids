/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for Protocol b/w client and master.

#ifndef CLIENT_PROTOCOL_H_
#define CLIENT_PROTOCOL_H_

#include <string>
#include <proto/minidfs.pb.h>

using std::string;

namespace minidfs {

/// \brief ClientProtocol is the communicate protocol between DFSClient with Master.
///
/// It communicates with Master node to get/set the metadata of files/directories.
/// This class is just an interface.
class ClientProtocol {
 public:
  /// \brief Get a file's block location information from Master. MethodID = 1.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int getBlockLocations(const string& file, LocatedBlocks* locatedBlks) = 0;

  /// \brief Create a file. MethodID = 2.
  ///
  /// This operation informs Master to create the meta info for the first block
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int create(const string& file, LocatedBlock* locatedBlk) = 0;
};

} // namespace minidfs
    
#endif