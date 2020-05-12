/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include <minidfs/remote_reader.hpp>

namespace minidfs {



RemoteReader::RemoteReader(const string& serverIP, int serverPort, const string& file, 
                           int bufferSize, const string& bufferBlkName)
    : master(new rpc::ClientProtocolProxy(serverIP, serverPort)),
      filename(file), BUFFER_SIZE(bufferSize), bufferBlkName(bufferBlkName) {
  pos = 0;
  lbs.Clear();

  bufferedStart = -1;
  bufferedEnd = -1;
}

RemoteReader::~RemoteReader(){
}

int RemoteReader::open() {
  int retOp = master->getBlockLocations(filename, &lbs);

  if (retOp != OpCode::OP_SUCCESS) {
    cerr << "[RemoteReader] "  << "Failed to open " << filename << std::endl
         << "Error code "  << retOp << std::endl;
    return -1;
  }
  return remoteSeek(pos);
}

int64_t RemoteReader::read(void* buffer, uint64_t size) {
  long long byteLeft = size;
  long long byteWritten = 0;
  while (byteLeft > 0) {
    if (pos > bufferedEnd) {
      if (-1 == remoteSeek(pos)) {
        return -1;
      }
      bufferOneBlk(currentLB);
    }
    long long nRead = byteLeft < (bufferedEnd-pos+1) ? byteLeft : (bufferedEnd-pos+1);
    
    std::ifstream fIn(bufferBlkName, std::ios::in | std::ios::binary);
    fIn.seekg(pos - bufferedStart);

    fIn.read((char*)buffer+byteWritten, nRead);
    pos += nRead;

    byteLeft -= nRead;
    byteWritten += nRead;

    fIn.clear();
    fIn.close();
  }

  return byteWritten;
}

int64_t RemoteReader::readAll(std::ofstream& f) {
  long long nRead = 0;
  while (-1 != remoteSeek(pos)) {
    long long newRead = readBlk(f, currentLB);
    if (newRead == -1) {
      return -1;
    }
    nRead += newRead;
    pos += newRead;
  }
  return nRead;
}

int RemoteReader::remoteSeek(uint64_t offset) {
  if (-1 == getLocatedBlk(offset, currentLB)) {
    return -1;
  }
  pos = offset;
  return 0;
}

int RemoteReader::bufferOneBlk(const LocatedBlock& lb) {
  std::ofstream f(bufferBlkName, std::ios::out | std::ios::binary | std::ios::trunc);
  auto readSize = readBlk(f, lb);

  if (readSize == -1) {
    f.clear();
    f.close();
    return -1;
  }

  /// update buffered range
  bufferedStart = bufferedEnd + 1;
  bufferedEnd += readSize;

  f.clear();
  f.close();
  return 0;
}

int64_t RemoteReader::readBlk(std::ofstream& f, const LocatedBlock& lb) const {
  int sockfd = connChunkserver(lb);

  if (sockfd == -1) {
    cerr << "[RemoteReader] " << "Failed to get block\n";
    close(sockfd);
    return -1;
  }

  /// send read request
  if (-1 == blkReadRequest(sockfd, lb.block())) {
    cerr << "[RemoteReader] " << "Failed to send block reading request\n";
    close(sockfd);
    return -1;
  }
  
  /// recv opcode
  char opRet = 0;
  if (recv(sockfd, &opRet, 1, 0) == -1) {
    close(sockfd);
    return -1;
  }

  if (opRet != OpCode::OP_SUCCESS) {
    cerr << "[RemoteReader] "  << "Failed to open " << filename << std::endl
         << "Error code "  << opRet << std::endl;
    close(sockfd);
    return -1;
  }

  //
  // recv data from remote
  //

  /// recv data length
  uint64_t dataLen = 0;
  uint32_t halfLen = 0;
  /// read the first half
  if (recv(sockfd, &halfLen, 4, 0) < 0) {
    close(sockfd);
    return -1;
  }
  dataLen = ntohl(halfLen);
  dataLen <<= 32;
  /// the second half
  if (recv(sockfd, &halfLen, 4, 0) < 0) {
    close(sockfd);
    return -1;
  }
  dataLen += ntohl(halfLen);

  std::vector<char> dataBuffer(BUFFER_SIZE);
  long long byteLeft = dataLen;
  while (byteLeft > 0) {
    int nRead = byteLeft < BUFFER_SIZE ? byteLeft : BUFFER_SIZE;
    if ((nRead = recv(sockfd, dataBuffer.data(), nRead, 0)) == -1) {
      close(sockfd);
      return -1;
    }
    /// write to file stream
    f.write(dataBuffer.data(), nRead);
    byteLeft -= nRead;
  }

  close(sockfd);
  return 0;
}

int RemoteReader::connChunkserver(const LocatedBlock& lb) const {
  /// chunkserver IP
  string serverIP;
  /// chunkserver port
  int serverPort;
  struct sockaddr_in serverAddr;

  int sockfd;
  int i = -1;
  for (i = 0; i < lb.chunkserverinfos_size(); ++i) {
    serverIP = lb.chunkserverinfos(i).chunkserverip();
    serverPort = lb.chunkserverinfos(i).chunkserverport();

    /// set the chunkserver struct
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(serverPort);
    if (inet_pton(AF_INET, serverIP.c_str(), &serverAddr.sin_addr) < 0) {
      cerr << "[RemoteReader] "  << "inet_pton() error for: " << serverIP << std::endl;
      continue;
    }
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
      cerr << "[RemoteReader] "  << "Failed to create socket\n" << strerror(errno) << " errno: " << errno << std::endl; 
      continue;
    }
    if (connect(sockfd, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
      cerr << "[RemoteReader] "  << "Cannot connect to " << serverIP << std::endl;
      continue;
    }
    cerr << "[RemoteReader] "  << "Succeed to connect chunkserver:\n" << serverIP << ":" << serverPort << std::endl;
    break;
  }

  /// not even one chunkserver is connectable
  if (i == -1 || i == lb.chunkserverinfos_size()) {
    return -1;
  }

  return sockfd;
}

int RemoteReader::blkReadRequest(int sockfd, const Block& blk) const {
  /// send read opcode
  char opRead = OpCode::OP_READ;
  if (send(sockfd, &opRead, 1, 0) == -1) {
    return -1;
  }

  /// send block length
  string blockinfo = blk.SerializeAsString();
  uint16_t len = htons(blockinfo.size());
  if (send(sockfd, &len, 2, 0) == -1) {
    return -1;
  }

  /// send block info
  if (send(sockfd, blockinfo.c_str(), blockinfo.size(), 0) == -1) {
    return -1;
  }

  cerr << "[RemoteReader] " << "Succeed to send request\n";
  return 0;
}

int RemoteReader::getLocatedBlk(int64_t offset, LocatedBlock& lb){
  int64_t totalLength = 0;
  for (int i = 0; i < lbs.locatedblks_size(); ++i) {
    int64_t blkLen = lbs.locatedblks(i).block().blocklen();
    if (totalLength + blkLen >= offset) {
      lb = lbs.locatedblks(i);
      return 0;
    }
    totalLength += blkLen;
  }
  return -1;
}

} // namespace minidfs
