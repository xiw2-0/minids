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
  //////////////////////////
  /// File reading/writing
  //////////////////////////

  /// \brief Get a file's block location information from Master. MethodID = 1.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int getBlockLocations(const string& file, LocatedBlocks* locatedBlks) = 0;

  /// \brief Create a file. MethodID = 2.
  ///
  /// This operation informs Master to create the meta info for the first block.
  /// Generally, the client will call addBlock() and complete() after create() call.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int create(const string& file, LocatedBlock* locatedBlk) = 0;

  /// \brief Add a block when the client has finished the previous block. MethodID = 3.
  ///
  /// When the client create() a file and finishes the 1st block, it calls addBlock()
  /// to add a block to continue writing the rest of the file.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return return OpCode.
  virtual int addBlock(const string& file, LocatedBlock* locatedBlk) = 0;

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
  virtual int blockAck(const LocatedBlock& locatedBlk) = 0;  

  /// \brief Complete writing a file. It should be called when the client has completed writing. MethodID = 5.
  ///
  /// calling order: create() [-> addBlock()] -> complete()
  /// This is the last step when creating a file. It is called when the client has finished writing.
  /// This operation will log the change of name system to the local disk.
  ///
  /// \param file the file name stored in minidfs.
  /// \return return OpCode.
  virtual int complete(const string& file) = 0;


  //////////////////////////
  /// Name system operations
  //////////////////////////

  /// \brief Remove/Delete a file. MethodID = 11.
  ///
  /// \param file the file to be removed.
  /// \return return OpCode.
  virtual int remove(const string& file) = 0;

  /// \brief To tell whether a file exists. MethodID = 12.
  ///
  /// \param file the query file.
  /// \return return OpCode.
  virtual int exists(const string& file) = 0;

  /// \brief Create a new folder. MethodID = 13.
  ///
  /// \param dirName the folder name stored in minidfs.
  /// \return return OpCode.
  virtual int makeDir(const string& dirName) = 0;

  /// \brief List items contained in a given folder. MethodID = 14.
  ///
  /// \param dirName the folder name stored in minidfs.
  /// \param items items contained in dirName. Each item describes info about a file.
  ///        It is the returning parameter.
  /// \return return OpCode.
  virtual int listDir(const string& dirName,  FileInfos& items) = 0;
};

} // namespace minidfs
    
#endif