/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <minidfs/dfs_chunkserver.hpp>


namespace minidfs {
DFSChunkserver::DFSChunkserver(const string& masterIP, int masterPort,
                               const string& serverIP, int serverPort,
                               const string& dataDir, long long blkSize,
                               int maxConnections, int BUFFER_SIZE)
    : master(new rpc::ChunkserverProtocolProxy(masterIP, masterPort)),
      serverIP(serverIP), serverPort(serverPort), dataDir(dataDir), blockSize(blkSize),
      maxConnections(maxConnections), BUFFER_SIZE(BUFFER_SIZE){
}

void DFSChunkserver::dataService() {

  int listenSockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (listenSockfd == -1) {
    cerr << "[DFSChunkserver] "  << "Failed to create a socket: " << strerror(errno) << std::endl;
    return;
  }
  struct sockaddr_in serverAddr;
  memset(&serverAddr, 0, sizeof(serverAddr));

  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddr.sin_port = htons(serverPort);

  if (bind(listenSockfd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == -1) {
    cerr << "[DFSChunkserver] "  << "Failed to bind the socket: " << strerror(errno) << std::endl;
    return;
  }

  if (listen(listenSockfd, maxConnections) == -1) {
    cerr << "[DFSChunkserver] "  << "Failed to listen to the socket: " << strerror(errno) << std::endl;
    return;
  }
  std::cout << "Start listening: \n";

  chunkserverInfo.set_chunkserverip(serverIP);
  chunkserverInfo.set_chunkserverport(serverPort);

  while(true) {
    struct sockaddr_in clientAddr;
    socklen_t clientAddrSize = sizeof(clientAddr);
    int connfd = accept(listenSockfd, (struct sockaddr*)&clientAddr, &clientAddrSize);
    if (connfd == -1) {
      cerr << "[DFSChunkserver] "  << "Failed to accept socket " << strerror(errno) << std::endl;
      continue;
    }
    /// TODO: xiw, use thread pool maybe?
    std::thread handleThread(&DFSChunkserver::handleBlockRequest, this, connfd);
    handleThread.detach();
  }
}

int DFSChunkserver::scanStoredBlocks() {
  struct dirent* blkFile;
  auto dir = opendir(dataDir.c_str());
  if (!dir) {
    cerr << "[DFSChunkserver] " << "Cannot open " << dataDir << std::endl;
    return -1;
  }

  while ((blkFile = readdir(dir)) != nullptr) {
    if (strcmp(blkFile->d_name, ".") == 0 || strcmp(blkFile->d_name, "..") == 0) {
      continue;
    }
    
    if (blkFile->d_type == DT_REG) {
      /// regular file

      /// the block name is stored in format of "blk_" + blockID
      auto blkname = string(blkFile->d_name);
      size_t idPos = blkname.find("blk_");

      if (idPos == blkname.size()) {
        continue;
      }

      idPos = idPos + 4;
      int blkid = atoi(blkname.substr(idPos).c_str());
      blksServed.emplace(blkid);
    }
  }
}

void DFSChunkserver::handleBlockRequest(int connfd) {
  char opcode = 0;
  if (recv(connfd, &opcode, 1, 0) == -1) {
    close(connfd);
    return;
  }

  switch (opcode) {
    case OpCode::OP_WRITE :
      recvBlock(connfd);
      break;
    case OpCode::OP_READ :
      sendBlock(connfd);
      break;
    default:
      cerr << "[DFSChunkserver] " << "Wrong OpCode\n";
      break;
  }
  close(connfd);
}

int DFSChunkserver::recvBlock(int connfd) {
  //
  // read header
  //
  uint16_t len = 0;
  /// read the length
  if (recv(connfd, &len, 2, 0) < 0) {
    return -1;
  }
  len = ntohs(len);

  std::vector<char> lbBuf(len, 0);
  /// read located block
  if (recv(connfd, lbBuf.data(), len, 0) < 0) {
    return -1;
  }
  LocatedBlock lb;
  lb.ParseFromArray(lbBuf.data(), len);

  //
  // read data from remote and write to local/another remote
  //
  uint64_t dataLen = 0;
  uint32_t halfLen = 0;
  /// read the first half
  if (recv(connfd, &halfLen, 4, 0) < 0) {
    return -1;
  }
  dataLen = ntohl(halfLen);
  dataLen <<= 32;
  /// the second half
  if (recv(connfd, &halfLen, 4, 0) < 0) {
    return -1;
  }
  dataLen += ntohl(halfLen);

  string folder("/tmp/");
  string blkFileName("blk_");
  int bID = lb.block().blockid();
  blkFileName += std::to_string(bID);
  std::fstream fOut(folder+blkFileName, std::ios::out | std::ios::app | std::ios::trunc | std::ios::binary);

  std::vector<char> dataBuffer(BUFFER_SIZE);
  long long byteLeft = dataLen;
  while (byteLeft > 0) {
    int nRead = byteLeft < BUFFER_SIZE ? byteLeft : BUFFER_SIZE;
    if ((nRead = recv(connfd, dataBuffer.data(), nRead, 0)) == -1) {
      return -1;
    }
    /// write to file
    fOut.write(dataBuffer.data(), nRead);
    /// TODO: xiw, forward the data to other chunkservers

    byteLeft -= nRead;
  }
  
  /// move to final folder
  rename((folder+blkFileName).c_str(), (dataDir+blkFileName).c_str());
  blksServed.emplace(bID);
  blksRecved.emplace(bID);

  /// send response
  char retOp = OpCode::OP_SUCCESS;
  int ret = send(connfd, &retOp, 1, 0);
  return ret;
}

int DFSChunkserver::sendBlock(int connfd) {
  //
  // read header
  //
  uint16_t len = 0;
  /// read the length
  if (recv(connfd, &len, 2, 0) < 0) {
    return -1;
  }
  len = ntohs(len);

  std::vector<char> bBuf(len, 0);
  /// read located block
  if (recv(connfd, bBuf.data(), len, 0) < 0) {
    return -1;
  }
  Block b;
  b.ParseFromArray(bBuf.data(), len);

  //
  // response: send data to client
  //
  char op = 0;
  int bID = b.blockid();
  /// send OpCode
  if (blksServed.find(bID) == blksServed.end()) {
    cerr << "[DFSChunkserver] " << "Invalid block: " << bID << std::endl;
    op = OpCode::OP_FAILURE;
    send(connfd, &op, 1, 0);
    return -1;
  }
  op = OpCode::OP_SUCCESS;
  send(connfd, &op, 1, 0);

  /// send block data
  if ( sendBlkData(connfd, bID) == -1) {
    cerr << "[DFSChunkserver] " << "Failed sending block: " << bID << std::endl;
    return -1;
  }
  
  return 0;
}

int DFSChunkserver::replicateBlock(LocatedBlock& locatedB) {
  int bID = locatedB.block().blockid();

  if (blksServed.find(bID) == blksServed.end()) {
    cerr << "[DFSChunkserver] " << "Invalid block: " << bID << std::endl;
    return -1;
  }

  //
  // connect target chunkserver
  //
  string serverIP = locatedB.chunkserverinfos(0).chunkserverip();
  int serverPort = locatedB.chunkserverinfos(0).chunkserverport();

  int sockfd = connectRemote(serverIP, serverPort);
  if (sockfd == -1) {
    return -1;
  }
  
  //
  // send data writing request
  //
  char op = OpCode::OP_WRITE;
  /// send OpCode
  send(sockfd, &op, 1, 0);
  
  string locatedBStr = locatedB.SerializeAsString();
  /// send the length of located block
  uint16_t lbLen = locatedBStr.size();
  lbLen = htons(lbLen);
  if ( send(sockfd, &lbLen, 2, 0) == -1) {
    return -1;
  }
  /// send located block
  if ( send(sockfd, locatedBStr.c_str(), 2, 0) == -1) {
    return -1;
  }
  /// send block data
  if ( sendBlkData(sockfd, bID) == -1) {
    cerr << "[DFSChunkserver] " << "Failed sending block: " << bID << std::endl;
    return -1;
  }

  /// response
  char opRet = 0;
  recv(sockfd, &opRet, 1, 0);
  if (opRet == OpCode::OP_SUCCESS) {
    return 0;
  } else {
    return -1;
  }
}


int DFSChunkserver::sendBlkData(int connfd, int bID) {
  string blkFileName("blk_");
  blkFileName += std::to_string(bID);
  std::fstream fIn(dataDir+blkFileName, std::ios::in | std::ios::binary);

  /// send datalen
  long long blkLen = - fIn.tellg();
  fIn.seekg(0, std::ios::end);
  blkLen += fIn.tellg();

  /// seek to start
  fIn.seekg(0, std::ios::beg);

  uint64_t dataLen = blkLen;
  uint32_t halfLen = dataLen >> 32;
  /// send the first half
  halfLen = htonl(halfLen);
  if (send(connfd, &halfLen, 4, 0) < 0) {
    return -1;
  }
  halfLen = dataLen;
  halfLen = htonl(halfLen);
  /// the second half
  if (recv(connfd, &halfLen, 4, 0) < 0) {
    return -1;
  }

  /// send data
  std::vector<char> dataBuffer(BUFFER_SIZE);
  long long byteLeft = dataLen;
  while (byteLeft > 0) {
    int nRead = byteLeft < BUFFER_SIZE ? byteLeft : BUFFER_SIZE;
    /// read from file
    fIn.read(dataBuffer.data(), nRead);
    if (send(connfd, dataBuffer.data(), nRead, 0) == -1) {
      return -1;
    }
    byteLeft -= nRead;
  }
  cerr << "[DFSChunkserver] " << "Succeed sending block: " << bID << std::endl;
  return 0;
}


int DFSChunkserver::connectRemote(string serverIP, int serverPort) {
  struct sockaddr_in serverAddr;
  /// set the server struct
  memset(&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(serverPort);
  if (inet_pton(AF_INET, serverIP.c_str(), &serverAddr.sin_addr) < 0) {
    cerr << "[DFSChunkserver] "  << "inet_pton() error for: " << serverIP << std::endl;
    return -1;
  }

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    cerr << "[DFSChunkserver] "  << "Failed to create socket\n" << strerror(errno) << " errno: " << errno << std::endl; 
    return -1;
  }

  if (connect(sockfd, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
    cerr << "[DFSChunkserver] "  << "Cannot connect to " << serverIP << std::endl;
    return -1;
  }
  cerr << "[DFSChunkserver] "  << "Succeed to connect Master:\n" << serverIP << ":" << serverPort << std::endl;
  return sockfd;
}

int DFSChunkserver::heartBeat() {
  return master->heartBeat(chunkserverInfo);
}

int DFSChunkserver::blkReport() {
  std::vector<int> blks(blksServed.begin(), blksServed.end());
  std::vector<int> blksDeleted;
  master->blkReport(chunkserverInfo, blks, blksDeleted);
}

} // namespace minidfs