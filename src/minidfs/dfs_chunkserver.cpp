/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <minidfs/dfs_chunkserver.hpp>


namespace minidfs {
DFSChunkserver::DFSChunkserver(const string& masterIP, int masterPort,
                               const string& serverIP, int serverPort,
                               const string& dataDir, long long blkSize,
                               int maxConnections, int BUFFER_SIZE,
                               size_t nThread,
                               long long HEART_BEAT_INTERVAL,
                               long long BLOCK_REPORT_INTERVAL,
                               long long BLK_TASK_STARTUP_INTERVAL)
    : master(new rpc::ChunkserverProtocolProxy(masterIP, masterPort)),
      serverIP(serverIP), serverPort(serverPort), dataDir(dataDir), blockSize(blkSize),
      maxConnections(maxConnections), BUFFER_SIZE(BUFFER_SIZE),
      threadPool(nThread),
      HEART_BEAT_INTERVAL(HEART_BEAT_INTERVAL), BLOCK_REPORT_INTERVAL(BLOCK_REPORT_INTERVAL),
      BLK_TASK_STARTUP_INTERVAL(BLK_TASK_STARTUP_INTERVAL){
}

void DFSChunkserver::run() {
  /// scan the block stored in local directory
  int op = scanStoredBlocks();
  if (op == OpCode::OP_FAILURE) {
    return;
  }
  cerr << "[DFSChunkserver] "  << "Succeed to scan data dir\n";

  /// start data service
  std::thread dataServiceThread(&DFSChunkserver::dataService, this);
  dataServiceThread.detach();

  //
  // Interact with master
  //
  using std::chrono::system_clock;
  auto lastHeartbeat = system_clock::now();
  auto lastBlkReport = lastHeartbeat;
  auto startup = lastHeartbeat;
  while(true) {
    auto now = system_clock::now();
    auto t = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastHeartbeat);
    /// heart beat
    if (t.count() > HEART_BEAT_INTERVAL) {
      heartBeat();
      lastHeartbeat = now;
    }

    /// block report
    //now = system_clock::now();
    t = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastBlkReport);
    if (t.count() > BLOCK_REPORT_INTERVAL) {
      blkReport();
      lastBlkReport = now;
    }

    /// inform the master about the received blocks
    if (blksRecved.empty() == false) {
      recvedBlks();
    }

    /// get block tasks
    //now = system_clock::now();
    t = std::chrono::duration_cast<std::chrono::milliseconds>(now - startup);
    if (t.count() > BLK_TASK_STARTUP_INTERVAL) {
      getBlkTask();
    }

    /// sleep until next heart beat
    now = system_clock::now();
    t = std::chrono::duration_cast<std::chrono::milliseconds>(lastHeartbeat - now);
    t += std::chrono::milliseconds(HEART_BEAT_INTERVAL);
    if (t.count() > 0) {
      std::this_thread::sleep_for(t);
    }
  }

  //dataServiceThread.join();
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
    std::function<void()> f = std::bind(&DFSChunkserver::handleBlockRequest, this, connfd);
    threadPool.enqueue(f);
  }
}

int DFSChunkserver::scanStoredBlocks() {
  struct dirent* blkFile;
  auto dir = opendir(dataDir.c_str());
  if (!dir) {
    cerr << "[DFSChunkserver] " << "Cannot open " << dataDir << std::endl;
    return OpCode::OP_FAILURE;
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
      std::lock_guard<std::mutex> lockBlksServed(mutexBlksServed);
      blksServed.emplace(blkid);
    }
  }
  return OpCode::OP_SUCCESS;
}

void DFSChunkserver::handleBlockRequest(int connfd) {
  char opcode = 0;
  if (recv(connfd, &opcode, 1, 0) == -1) {
    close(connfd);
    return;
  }

  cerr << "[DFSChunkserver] " << "One request " << (int)opcode << "\n";

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

  cerr << "[DFSChunkserver] " << "Succeed recving lb length: " << (int)len << std::endl;

  std::vector<char> lbBuf(len, 0);
  /// read located block
  if (recv(connfd, lbBuf.data(), len, 0) < 0) {
    return -1;
  }
  LocatedBlock lb;
  lb.ParseFromArray(lbBuf.data(), len);
  cerr << "[DFSChunkserver] " << "Succeed recving lb: " << lb.DebugString() << std::endl;

  //
  // read data from remote and write to local/another remote
  //
  uint64_t dataLen = 0;
  uint32_t halfLen = 0;
  /// read the first half
  if (recv(connfd, &halfLen, 4, 0) < 0) {
    cerr << "[DFSChunkserver] " << "Failed recving " << (int)halfLen;
    return -1;
  }
  dataLen = ntohl(halfLen);
  dataLen <<= 32;
  /// the second half
  if (recv(connfd, &halfLen, 4, 0) < 0) {
    cerr << "[DFSChunkserver] " << "Failed recving " << (int)halfLen;
    return -1;
  }
  dataLen += ntohl(halfLen);
  cerr << "[DFSChunkserver] " << "Succeed recving data length: " << (int)dataLen << std::endl;

  string folder("/tmp");
  string blkFileName("blk_");
  int bID = lb.block().blockid();
  blkFileName += std::to_string(bID);
  std::fstream fOut(folder+"/"+blkFileName, std::ios::out | std::ios::trunc | std::ios::binary);
  if (fOut.is_open() == false) {
    cerr << "[DFSChunkserver] " << "Failed to open " << folder+"/"+blkFileName << std::endl;
    fOut.clear();
    fOut.close();
    return -1;
  }

  std::vector<char> dataBuffer(BUFFER_SIZE);
  long long byteLeft = dataLen;
  while (byteLeft > 0) {
    cerr << "[DFSChunkserver] " << "Byte left: " << byteLeft << std::endl;

    int nRead = byteLeft < BUFFER_SIZE ? byteLeft : BUFFER_SIZE;
    if ((nRead = recv(connfd, dataBuffer.data(), nRead, 0)) == -1) {
      fOut.clear();
      fOut.close();
      return -1;
    }
    /// write to file
    fOut.write(dataBuffer.data(), nRead);
    /// TODO: xiw, forward the data to other chunkservers

    byteLeft -= nRead;
  }
  fOut.clear();
  fOut.close();
  
  /// move to final folder
  rename((folder+"/"+blkFileName).c_str(), (dataDir+"/"+blkFileName).c_str());
  {
    std::lock_guard<std::mutex> lockBlksServed(mutexBlksServed);
    std::lock_guard<std::mutex> lockBlksRecved(mutexBlksRecved);
    blksServed.emplace(bID);
    blksRecved.emplace(bID);
  }
  /// send response
  char retOp = OpCode::OP_SUCCESS;
  int ret = send(connfd, &retOp, 1, 0);
  cerr << "[DFSChunkserver] " << "Succeed recving block: " << bID << std::endl;
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
  cerr << "[DFSChunkserver] " << "Succeed recving b length: " << (int)len << std::endl;

  std::vector<char> bBuf(len, 0);
  /// read located block
  if (recv(connfd, bBuf.data(), len, 0) < 0) {
    return -1;
  }
  Block b;
  b.ParseFromArray(bBuf.data(), len);
  cerr << "[DFSChunkserver] " << "Succeed recving b: " << b.DebugString() << std::endl;
  //
  // response: send data to client
  //
  char op = 0;
  int bID = b.blockid();
  /// send OpCode
  {
    std::lock_guard<std::mutex> lockBlksServed(mutexBlksServed);
    if (blksServed.find(bID) == blksServed.end()) {
      cerr << "[DFSChunkserver] " << "Invalid block: " << bID << std::endl;
      op = OpCode::OP_FAILURE;
      send(connfd, &op, 1, 0);
      return -1;
    }
  }
  op = OpCode::OP_SUCCESS;
  send(connfd, &op, 1, 0);
  cerr << "[DFSChunkserver] " << "Succeed sending ret op " << std::endl;
  /// send block data
  if ( sendBlkData(connfd, bID) == -1) {
    cerr << "[DFSChunkserver] " << "Failed sending block: " << bID << std::endl;
    return -1;
  }
  cerr << "[DFSChunkserver] " << "Succeed sending block data "<< std::endl;
  return 0;
}

int DFSChunkserver::replicateBlock(const LocatedBlock& locatedB) {
  int bID = locatedB.block().blockid();
  {
    std::lock_guard<std::mutex> lockBlksServed(mutexBlksServed);
    if (blksServed.find(bID) == blksServed.end()) {
      cerr << "[DFSChunkserver] " << "Invalid block: " << bID << std::endl;
      return -1;
    }
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
  std::fstream fIn(dataDir+"/"+blkFileName, std::ios::in | std::ios::binary);
  if (fIn.is_open() == false) {
    cerr << "[DFSChunkserver] " << "Failed to open " << dataDir+"/"+blkFileName << std::endl;
    fIn.clear();
    fIn.close();
    return -1;
  }

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
    fIn.clear();
    fIn.close();
    return -1;
  }
  halfLen = dataLen;
  halfLen = htonl(halfLen);
  /// the second half
  if (send(connfd, &halfLen, 4, 0) < 0) {
    fIn.clear();
    fIn.close();
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
      fIn.clear();
      fIn.close();
      return -1;
    }
    byteLeft -= nRead;
  }
  fIn.clear();
  fIn.close();
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
  //cerr << "[DFSChunkserver] "  << "Succeed to connect Master:\n" << serverIP << ":" << serverPort << std::endl;
  return sockfd;
}

int DFSChunkserver::heartBeat() {
  cerr << "[DFSChunkserver] " << "Heartbeating\n";
  return master->heartBeat(chunkserverInfo);
}

int DFSChunkserver::blkReport() {
  //cerr << "[DFSChunkserver] " << "Reporting blocks\n";
  std::lock_guard<std::mutex> lockBlksServed(mutexBlksServed);
  std::vector<int> blks(blksServed.begin(), blksServed.end());
  std::vector<int> blksDeleted;
  int opRet = master->blkReport(chunkserverInfo, blks, blksDeleted);
  if (opRet == OpCode::OP_FAILURE) {
    return opRet;
  }
  for (int blkD : blksDeleted) {
    if (blksServed.find(blkD) != blksServed.end()) {
      blksServed.erase(blkD);

      string blkFileName("blk_");
      blkFileName += std::to_string(blkD);
      if (remove((dataDir+"/"+blkFileName).c_str()) == 0) {
        cerr << "[DFSChunkserver] " << "Succeed removing block: " << blkD << std::endl;
      } else {
        opRet = OpCode::OP_FAILURE;
        cerr << "[DFSChunkserver] " << "Failed removing block: " << blkD << std::endl;
      }
    }
  }
  return opRet;
}

int DFSChunkserver::getBlkTask() {
  //cerr << "[DFSChunkserver] " << "Getting block tasks\n";
  BlockTasks blockTasks;
  int opRet = master->getBlkTask(chunkserverInfo, &blockTasks);
  if (opRet == OpCode::OP_FAILURE) {
    return opRet;
  }

  for (int i = 0; i < blockTasks.blktasks_size(); ++i) {
    const auto& task = blockTasks.blktasks(i);
    if (task.operation() == OpCode::OP_COPY) {
      if (0 == replicateBlock(task.locatedblk())) {
        cerr << "[DFSChunkserver] " << "Succeed to replicate block: " << task.locatedblk().block().blockid() << std::endl;
      } else {
        opRet = OpCode::OP_FAILURE;
      }
    }
  }
  return opRet;
}

int DFSChunkserver::recvedBlks() {
  //cerr << "[DFSChunkserver] " << "Sending blocks received to master\n";
  std::lock_guard<std::mutex> lockBlksRecved(mutexBlksRecved);
  if (blksRecved.size() == 0) {
    return OpCode::OP_SUCCESS;
  }

  std::vector<int> blks(blksRecved.begin(), blksRecved.end());
  int opRet = master->recvedBlks(chunkserverInfo, blks);
  if (opRet == OpCode::OP_SUCCESS) {
    blksRecved.clear();  
  }
  return opRet;
}

} // namespace minidfs