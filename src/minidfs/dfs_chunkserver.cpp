/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <minidfs/dfs_chunkserver.hpp>


namespace minidfs {
DFSChunkserver::DFSChunkserver(const string& masterIP, int masterPort,
                               const string& serverIP, int serverPort,
                               const string& dataDir, long long blkSize,
                               int maxConnections)
    : master(new rpc::ChunkserverProtocolProxy(masterIP, masterPort)),
      serverIP(serverIP), serverPort(serverPort), dataDir(dataDir), blockSize(blkSize),
      maxConnections(maxConnections) {
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
      servedBlks.emplace(blkid);
    }
  }
}

void DFSChunkserver::handleBlockRequest(int connfd) {
  
}

} // namespace minidfs