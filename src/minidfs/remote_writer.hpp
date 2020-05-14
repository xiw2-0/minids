/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RemoteWriter.

#ifndef REMOTE_WRITER_H_
#define REMOTE_WRITER_H_

#include <string>
#include <chrono>
#include <thread>
#include <fstream>

#include <minidfs/client_protocol.hpp>
#include <rpc/client_protocol_proxy.hpp>

using std::string;

namespace minidfs {


/// Currently, random write is not supported!
/// When writing data to dfs, client caches the data in a local file
/// firstly. When the file size reaches BLOCK_SIZE or the write is done,
/// send the whole block to remote chunkserver. 
/// TODO: xiw, don't use write() and writeAll() together!!! Some bugs are
/// remained to be solved, e.g. pos pointer.
class RemoteWriter {
 private:
  /// this is used to communicate with Master
  std::unique_ptr<ClientProtocol> master;

  /// each reader serves for only one file
  const string filename;

  /// current block which pos points to
  LocatedBlock currentLB;

  /// block size
  const int64_t BLOCK_SIZE;

  /// keep track of the file pointer. It is the position to be write next.
  uint64_t pos;

  /// buffer size in memory
  const int BUFFER_SIZE;
  /// the 1st writing buffer
  ///std::vector<char> globalBuf;
  /// pointer to the end of the written part of globalBuf
  /// When the buffer is empty, globalBufPos = 0.
  ///int globalBufPos;

  /// start of the block data buffer file. The start is contained in the buffer.
  /// It indicates the start position of the current cached block in the whole file.
  int64_t blockStart;

  /// current position of the buffered block data file. The pos is not contained in the buffer.
  /// It is the next position to be written. Relative position. Range from 0~BLOCK_SIZE
  int64_t blockPos;

  /// filename used as the buffer of remote block data
  const string bufferBlkName;

  /// number of times to try connecting the remote before giving up
  const int nTrial;

 public:
  RemoteWriter(const string& serverIP, int serverPort, const string& file,
               int bufferSize, int nTrial, long long blockSize,
               const string& bufferBlkName);
  RemoteWriter(RemoteWriter&& writer);
  ~RemoteWriter();
  
  /// \brief Connect the master to get the 1st block information
  /// which contains the chunkservers to be written to.
  ///
  /// \return return 0 on success, -1 for errors
  int open();

  /// \brief Write size bytes of data with offset from buffer into the file in dfs.
  /// The default offset means to append data to the file. Call open() first and remoteClose() at last!
  ///
  /// \param buffer data buffer to be written
  /// \param size size of data to be written
  /// \return size of data written 
  int64_t write(const void* buffer, uint64_t size);


  /// \brief Write all the data from f to dfs. Call open() first and remoteClose() at last!
  ///
  /// \param f input local file stream
  /// \return size of data written
  int64_t writeAll(std::ifstream& f);

  
  /// \brief Flush the data first and send complete rpc call to Master.
  /// The file is completed until remoteClose() is called!
  int remoteClose();


 private:

  /// \brief Write a whole block from an input file stream
  /// to remote chunkservers
  ///
  /// \param f output file stream
  /// \param lb block to be written
  /// \return size of data written  
  int64_t writeBlk(std::ifstream& f, const LocatedBlock& lb) const;

  /// \brief Connect with remote chunkservers. Wait 2s if the first
  /// trial is failed. Try at most twice.
  ///
  /// \return return the connected socket fd with the 1st chunkserver.
  /// return -1 for errors.
  int connChunkserver(const ChunkserverInfo& cs) const;

  /// \brief Send block writing request.
  ///
  /// \param sockfd the connected socket fd
  /// \param blk block to be written
  /// \return return 0 on success, -1 for errors.
  int blkWriteRequest(int sockfd, const LocatedBlock& lb) const;

  /// \brief Get a new LocatedBlock after the current one is finished.
  /// Send addBlock() rpc to Master. After getting response from Master,
  /// this method will set currentLB to the returned value.
  /// And this method will set blockStart and blockPos
  ///
  /// \return return 0 on success, -1 for errors.
  int addBlk();

  /// \brief Flush the remaining cached data to remote chunkservers if any.
  int remoteFlush();


};







} // namespace minidfs

#endif