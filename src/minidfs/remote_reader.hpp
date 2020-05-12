/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RemoteReader.

#ifndef REMOTE_READER_H_
#define REMOTE_READER_H_

#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <string>
#include <fstream>

#include <minidfs/client_protocol.hpp>
#include <rpc/client_protocol_proxy.hpp>

using std::string;

namespace minidfs {


/// RemoteReader opens a file in dfs and reads the data from it.

class RemoteReader {
 private:
  /// this is used to communicate with Master
  std::unique_ptr<ClientProtocol> master;

  /// each reader serves for only one file
  const string filename;

  /// file related blocks
  LocatedBlocks lbs;

  /// keep track of the file pointer. It is the position to be read next.
  uint64_t pos;

  /// buffer size
  const int BUFFER_SIZE;

  /// current block which pos points to
  LocatedBlock currentLB;

  /// start of the buffered block data. The start is contained in the buffer.
  int64_t bufferedStart;
  /// end of the buffered block data. The end is contained in the buffer.
  int64_t bufferedEnd;
  /// filename used as the buffer of remote block data
  const string bufferBlkName;

  

 public:
  /// Construct a remote reader.
  ///
  /// \param serverIP IP of master
  /// \param serverPort port of master
  /// \param file the file to be read
  RemoteReader(const string& serverIP, int serverPort, const string& file, int bufferSize,
               const string& bufferBlkName);

  ~RemoteReader();
  
  /// \brief Connect the master to get the block information.
  /// Open a file before any other operations!!!
  /// Open a file and seek to the start of the file at the same time.
  ///
  /// \return return 0 on success, -1 for errors
  int open();

  /// \brief Read size bytes of data with offset into buffer.
  /// The user should be careful about the size of buffer. Call open() first!
  ///
  /// \param buffer data buffer to be read into
  /// \param size size of data to be read
  /// \return size of data read  
  int64_t read(void* buffer, uint64_t size);

  /// \brief Read all the data into f. Call open() first!
  ///
  /// \param f output local file stream
  /// \return size of data read
  int64_t readAll(std::ofstream& f);

  /// \brief Change the file pointer to offset. And this
  /// operation will update pos and currentLB.
  ///
  /// \param offset 
  /// \return return 0 on success, -1 for errors
  int remoteSeek(uint64_t offset);



 private:
  /// \brief Read a block from remote chunkserver and update bufferedStart
  /// and bufferedEnd. The block will be stored in bufferBlkName.
  ///
  /// \param lb located block info
  /// \return return 0 on success, -1 for errors.
  int bufferOneBlk(const LocatedBlock& lb);

  /// \brief Read a whole block into a output file stream
  /// from a remote chunkserver.
  ///
  /// \param f output file stream
  /// \param lb block to be read
  /// \return size of data read  
  int64_t readBlk(std::ofstream& f, const LocatedBlock& lb) const;

  /// \brief Connect with remote chunkservers.
  /// \return return the connected socket fd with one of chunkservers.
  /// return -1 for errors.
  int connChunkserver(const LocatedBlock& lb) const;

  /// \brief Send block reading request.
  ///
  /// \param sockfd the connected socket fd
  /// \param blk block to be read
  /// \return return 0 on success, -1 for errors.
  int blkReadRequest(int sockfd, const Block& blk) const;

  /// \brief Get the required LocatedBlock corresponding to the offset.
  ///
  /// \param offset offset
  /// \param lb located block corresponding to the offset
  /// \return return 0 on success, -1 for errors.
  int getLocatedBlk(int64_t offset, LocatedBlock& lb);
};


} // namespace minidfs

#endif