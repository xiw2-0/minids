/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include <minidfs/remote_writer.hpp>

namespace minidfs {



RemoteWriter::RemoteWriter(const string& serverIP, int serverPort, const string& file,
                           int bufferSize, int nTrial, long long blockSize,
                           const string& bufferBlkName)
    : master(new rpc::ClientProtocolProxy(serverIP, serverPort)),
      filename(file), BUFFER_SIZE(bufferSize), nTrial(nTrial), BLOCK_SIZE(blockSize),
      bufferBlkName(bufferBlkName) {
  pos = 0;
  bufferStart = 0;
  bufferEnd = BLOCK_SIZE;
}

RemoteWriter::RemoteWriter(RemoteWriter&& writer)
    : master(std::move(writer.master)), filename(writer.filename),
      BUFFER_SIZE(writer.BUFFER_SIZE), nTrial(writer.nTrial), BLOCK_SIZE(writer.BLOCK_SIZE),
      bufferBlkName(writer.bufferBlkName) {
  pos = 0;
  bufferStart = 0;
  bufferEnd = BLOCK_SIZE;
}

RemoteWriter::~RemoteWriter() {
}

int RemoteWriter::open() {
  int retOp = master->create(filename, &currentLB);
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[RemoteWriter] "  << "Failed to open " << filename << std::endl
         << "Error code: "  << retOp << std::endl;
    return -1;
  }
  pos = 0;
  bufferStart = 0;
  bufferEnd = BLOCK_SIZE;
  return 0;
}

int64_t RemoteWriter::write(const void* buffer, uint64_t size) {
  long long byteLeft = size;
  long long byteWritten = 0;
  while (byteLeft > 0) {
    /// the buffer in local fs is full
    if (pos > bufferEnd) {
      /// set block size befor sending it to chunkserver
      currentLB.mutable_block()->set_blocklen(bufferEnd - bufferStart + 1);

      std::ifstream fIn(bufferBlkName, std::ios::in | std::ios::binary);
      int writtenSize = writeBlk(fIn, currentLB);
      if (writtenSize == -1) {
        fIn.clear();
        fIn.close();
        return -1;
      }
      fIn.clear();
      fIn.close();
      /// after send the buffered block, clear the file
      std::ofstream fOut(bufferBlkName, std::ios::out | std::ios::binary | std::ios::trunc);
      fOut.clear();
      fOut.close();

      /// ask for new block
      if (-1 == addBlk()) {
        return -1;
      }
    }

    long long nWrite = std::min(byteLeft, bufferEnd - (long long)pos + 1);
    std::ofstream fOut(bufferBlkName, std::ios::out | std::ios::binary | std::ios::trunc);
    fOut.write((char*)buffer + pos, nWrite);
    pos += nWrite;
  
    byteLeft -= nWrite;
    byteWritten += nWrite;

    fOut.clear();
    fOut.close();
  }

  return byteWritten;
}

int64_t RemoteWriter::writeAll(std::ifstream& f) {
  /// send datalen
  long long fileLen = - f.tellg();
  f.seekg(0, std::ios::end);
  fileLen += f.tellg();
  /// seek to start
  f.seekg(0, std::ios::beg);
  
  long long nLeft = fileLen;
  while (nLeft > 0) {
    long long nWrite = std::min((long long)BLOCK_SIZE, nLeft);
    /// set block size befor sending it to chunkserver
    currentLB.mutable_block()->set_blocklen(nWrite);
    if (-1 == writeBlk(f, currentLB)){
      return -1;
    }
    pos += nWrite;
    nLeft -= nWrite;
    if (nLeft > 0) {
      if (-1 == addBlk()){
        return -1;
      }
    }
  }

  return fileLen;
}


int RemoteWriter::close() {
  /// flush the remaining data if any
  if (-1 == remoteFlush()) {
    return -1;
  }

  int retOp = master->complete(filename);
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[RemoteWriter] "  << "Failed to complete " << filename << std::endl
         << "Error code: "  << retOp << std::endl;
    return -1;
  }
  cerr << "[RemoteWriter] "  << "Succeed to write " << filename << std::endl;
  return 0;
}

int64_t RemoteWriter::writeBlk(std::ifstream& f, const LocatedBlock& lb) const {
  if (lb.chunkserverinfos_size() < 1) {
    return -1;
  }

  int sockfd = connChunkserver(lb.chunkserverinfos(0));
  if (sockfd == -1) {
    return -1;
  }

  //
  // send data to remote chunkserver
  //
  /// send datalen
  long long blkLen = lb.block().blocklen();

  uint64_t dataLen = blkLen;
  uint32_t halfLen = dataLen >> 32;
  /// send the first half
  halfLen = htonl(halfLen);
  if (send(sockfd, &halfLen, 4, 0) < 0) {
    return -1;
  }
  halfLen = dataLen;
  halfLen = htonl(halfLen);
  /// the second half
  if (recv(sockfd, &halfLen, 4, 0) < 0) {
    return -1;
  }

  /// send data
  std::vector<char> dataBuffer(BUFFER_SIZE);
  long long byteLeft = dataLen;
  while (byteLeft > 0) {
    int nRead = byteLeft < BUFFER_SIZE ? byteLeft : BUFFER_SIZE;
    /// read from file
    f.read(dataBuffer.data(), nRead);
    if (send(sockfd, dataBuffer.data(), nRead, 0) == -1) {
      return -1;
    }
    byteLeft -= nRead;
  }

  /// wait for response from chunkserver
  char retOp =0;
  if (recv(sockfd, &retOp, 1, 0) == -1) {
    return -1;
  }
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[RemoteWriter] "  << "Failed to write block " << lb.block().blockid() << std::endl
         << "Error code: "  << retOp << std::endl;
    return -1;
  }

  /// send ack to master
  int opFromMaster = master->blockAck(lb);
  if (opFromMaster != OpCode::OP_SUCCESS) {
    cerr << "[RemoteWriter] "  << "Failed to send ack of block " << lb.block().blockid() << std::endl
         << "Error code: "  << retOp << std::endl;
    return -1;
  }


  cerr << "[RemoteWriter] "  << "Succeed sending block: " << lb.block().blockid() << std::endl;
  return dataLen;
}

int RemoteWriter::connChunkserver(const ChunkserverInfo& cs) const {
  /// chunkserver IP
  string serverIP = cs.chunkserverip();
  /// chunkserver port
  int serverPort = cs.chunkserverport();

  struct sockaddr_in serverAddr;
  /// set the chunkserver struct
  memset(&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(serverPort);
  if (inet_pton(AF_INET, serverIP.c_str(), &serverAddr.sin_addr) < 0) {
    cerr << "[RemoteWriter] "  << "inet_pton() error for: " << serverIP << std::endl;
    return -1;
  }

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    cerr << "[RemoteWriter] "  << "Failed to create socket\n" << strerror(errno) << " errno: " << errno << std::endl; 
    return -1;
  }

  int i = 0;
  for (i = 0; i < nTrial; ++i) {
    if (connect(sockfd, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
      cerr << "[RemoteWriter] "  << "Cannot connect to " << serverIP << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }
    cerr << "[RemoteWriter] "  << "Succeed to connect chunkserver:\n" << serverIP << ":" << serverPort << std::endl;
    break;
  }

  /// n trials are all failed 
  if (i == nTrial) {
    return -1;
  }

  return sockfd;
}

int RemoteWriter::blkWriteRequest(int sockfd, const LocatedBlock& lb) const {
  /// send write opcode
  char opRead = OpCode::OP_WRITE;
  if (send(sockfd, &opRead, 1, 0) == -1) {
    return -1;
  }

  /// send located block length
  string lbInfo = lb.SerializeAsString();
  uint16_t len = htons(lbInfo.size());
  if (send(sockfd, &len, 2, 0) == -1) {
    return -1;
  }

  /// send located block info
  if (send(sockfd, lbInfo.c_str(), lbInfo.size(), 0) == -1) {
    return -1;
  }

  cerr << "[RemoteReader] " << "Succeed to send request\n";
  return 0;
}

int RemoteWriter::addBlk() {
  int retOp = master->addBlock(filename, &currentLB);
  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[RemoteWriter] "  << "Failed to add a new block to " << filename << std::endl
         << "Error code: "  << retOp << std::endl;
    return -1;
  }
  bufferStart = bufferEnd + 1;
  bufferEnd += BLOCK_SIZE;
  return 0;
}

int RemoteWriter::remoteFlush() {
  if (pos == bufferStart) {
    return 0;
  }
  /// set block size befor sending it to chunkserver
  currentLB.mutable_block()->set_blocklen(pos - bufferStart);

  std::ifstream fIn(bufferBlkName, std::ios::in | std::ios::binary);
  int writtenSize = writeBlk(fIn, currentLB);
  if (writtenSize == -1) {
    fIn.clear();
    fIn.close();
    return -1;
  }

  fIn.clear();
  fIn.close();
  return 0;
}

} // namespace minidfs




