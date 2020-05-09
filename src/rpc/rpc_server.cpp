/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include <rpc/rpc_server.hpp>
#include <minidfs/dfs_master.hpp>


namespace rpc {
/// TODO: xiw, isSafe should be true. False here is used to test rpc b/w client and master.
RPCServer::RPCServer(int serverPort, int maxConns, minidfs::DFSMaster* master)
    : serverPort(serverPort), maxConnections(maxConns), master(master), isSafeMode(false) {
}

RPCServer::~RPCServer() {
}

int RPCServer::init() {
  if (bindRPCCalls() < 0) {
    cerr << "[RPCServer] "  << "Failed to bind RPC calls\n";
    return -1;
  }
  if (initServer() < 0) {
    cerr << "[RPCServer] "  << "Failed to init server\n";
    return -1;
  }
  return 0;
}

void RPCServer::run() {
  while(true) {
    struct sockaddr_in clientAddr;
    socklen_t clientAddrSize = sizeof(clientAddr);
    int connfd = accept(listenSockfd, (struct sockaddr*)&clientAddr, &clientAddrSize);
    if (connfd == -1) {
      cerr << "[RPCServer] "  << "Failed to accept socket " << strerror(errno) << std::endl;
      continue;
    }
    /// TODO: xiw, use thread pool maybe?
    std::thread handleThread(&RPCServer::handleRequest, this, connfd);
    handleThread.detach();
  }
}

int RPCServer::initServer() {
  listenSockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (listenSockfd == -1) {
    cerr << "[RPCServer] "  << "Failed to create a socket: " << strerror(errno) << std::endl;
    return -1;
  }

  memset(&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddr.sin_port = htons(serverPort);

  if (bind(listenSockfd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == -1) {
    cerr << "[RPCServer] "  << "Failed to bind the socket: " << strerror(errno) << std::endl;
    return -1;
  }

  if (listen(listenSockfd, maxConnections) == -1) {
    cerr << "[RPCServer] "  << "Failed to listen to the socket: " << strerror(errno) << std::endl;
    return -1;
  }
  std::cout << "Start listening: \n";
  return 0;
}

int RPCServer::bindRPCCalls() {
  /// Client protocol
  rpcBindings[1] = std::bind(&RPCServer::getBlockLocations, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[2] = std::bind(&RPCServer::create, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[3] = std::bind(&RPCServer::addBlock, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[4] = std::bind(&RPCServer::blockAck, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[5] = std::bind(&RPCServer::complete, this, std::placeholders::_1, std::placeholders::_2);
  
  rpcBindings[11] = std::bind(&RPCServer::remove, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[12] = std::bind(&RPCServer::exists, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[13] = std::bind(&RPCServer::makeDir, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[14] = std::bind(&RPCServer::listDir, this, std::placeholders::_1, std::placeholders::_2);
  

  /// Chunkserver protocol
  rpcBindings[101] = std::bind(&RPCServer::heartBeat, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[102] = std::bind(&RPCServer::blkReport, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[103] = std::bind(&RPCServer::getBlkTask, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[104] = std::bind(&RPCServer::recvedBlks, this, std::placeholders::_1, std::placeholders::_2);
  return 0;
}

void RPCServer::handleRequest(int connfd) {
  //
  // recv request
  //
  int methodID = -1;
  string request;
  if (recvRequest(connfd, methodID, request) == -1) {
    close(connfd);
    return;
  }

  /// In safe mode, rpc calls from clients are ignored!
  if (isSafeMode == true && methodID <= 100) {
    int status = OpCode::OP_SAFE_MODE;
    string response;
    sendResponse(connfd, status, response);

    cerr << "[RPCServer] "  << "In safe mode\n";
    close(connfd);
    
    isSafeMode = !master->isSafe();
    return;
  }

  auto func = rpcBindings[methodID];
  if (func(connfd, request) < 0) {
    cerr << "[RPCServer] "  << "Failed to send response to " << connfd << std::endl;
    close(connfd);
    return;
  }
  cerr << "[RPCServer] "  << "Succeed to process one request\n";
  /// close client sock
  close(connfd);
}

/// Format of response: len(4 Byte) : status(1 Byte) : response
int RPCServer::getBlockLocations(int connfd, const string& request) {
  minidfs::LocatedBlocks locatedBlks;
  int status = master->getBlockLocations(request, &locatedBlks);
  string response = locatedBlks.SerializeAsString();

  /// reply to the client
  return sendResponse(connfd, status, response);
}

int RPCServer::create(int connfd, const string& request) {
  minidfs::LocatedBlock locatedBlk;
  int status = master->create(request, &locatedBlk);
  string response = locatedBlk.SerializeAsString();

  return sendResponse(connfd, status, response);
}

int RPCServer::addBlock(int connfd, const string& request) {
  minidfs::LocatedBlock locatedBlk;
  int status = master->addBlock(request, &locatedBlk);
  string response = locatedBlk.SerializeAsString();

  return sendResponse(connfd, status, response);
}

int RPCServer::blockAck(int connfd, const string& request) {
  minidfs::LocatedBlock locatedBlk;
  locatedBlk.ParseFromString(request);
  int status = master->blockAck(locatedBlk);
  string response;

  return sendResponse(connfd, status, response);
}


int RPCServer::complete(int connfd, const string& request) {
  int status = master->complete(request);
  string response;

  return sendResponse(connfd, status, response);
}

int RPCServer::remove(int connfd, const string& request) {
  int status = master->remove(request);
  string response;

  return sendResponse(connfd, status, response);
}

int RPCServer::exists(int connfd, const string& request) {
  int status = master->exists(request);
  string response;

  return sendResponse(connfd, status, response);
}

int RPCServer::makeDir(int connfd, const string& request) {
  int status = master->makeDir(request);
  string response;

  return sendResponse(connfd, status, response);
}

int RPCServer::listDir(int connfd, const string& request) {
  minidfs::FileInfos items;
  int status = master->listDir(request, items);
  string response = items.SerializeAsString();

  return sendResponse(connfd, status, response);
}


int RPCServer::heartBeat(int connfd, const string& request) {
  minidfs::ChunkserverInfo chunkserverInfo;
  chunkserverInfo.ParseFromString(request);
  int status = master->heartBeat(chunkserverInfo);
  /// No response
  string response;
  return sendResponse(connfd, status, response);
}

int RPCServer::blkReport(int connfd, const string& request) {
  minidfs::BlockReport report;
  report.ParseFromString(request);

  std::vector<int> blkIDs;
  for (int i = 0; i < report.blkids_size(); ++i) {
    blkIDs.push_back(report.blkids(i));
  }

  std::vector<int> deletedBlks;
  int status = master->blkReport(report.chunkserverinfo(), blkIDs, deletedBlks);
  /// construct the response
  minidfs::BlkIDs response;
  for (int i : deletedBlks) {
    response.add_blkids(i);
  }
  return sendResponse(connfd, status, response.SerializeAsString());
}

int RPCServer::getBlkTask(int connfd, const string& request) {
  minidfs::ChunkserverInfo chunkserverInfo;
  chunkserverInfo.ParseFromString(request);

  minidfs::BlockTasks blkTasks;
  int status = master->getBlkTask(chunkserverInfo, &blkTasks);
  
  return sendResponse(connfd, status, blkTasks.SerializeAsString());
}

int RPCServer::recvedBlks(int connfd, const string& request) {
  minidfs::BlockReport report;
  report.ParseFromString(request);

  std::vector<int> blkIDs;
  for (int i = 0; i < report.blkids_size(); ++i) {
    blkIDs.push_back(report.blkids(i));
  }

  int status = master->recvedBlks(report.chunkserverinfo(), blkIDs);
  /// No response
  string response;
  return sendResponse(connfd, status, response);
}


int RPCServer::recvRequest(int connfd, int& methodID, string& request) {
  int32_t len = 0;
  /// read the length
  if (recv(connfd, &len, 4, 0) < 0) {
    return -1;
  }
  len = ntohl(len);

  /// read methodID
  char mID = methodID;
  if (recv(connfd, &mID, 1, 0) == -1) {
    return -1;
  }
  methodID = mID;

  /// recv request
  std::vector<char> buf(len-5);
  int ret = recv(connfd, buf.data(), buf.size(), 0);
  if (ret == -1) {
    cerr << "[RPCServer] " << "Failed to recv request\n";
    return -1;
  }
  request = string(buf.begin(), buf.end());
  cerr << "[RPCServer] " << "Succeed to recv request\n";
  return 0;
}

int RPCServer::sendResponse(int connfd, int status, const string& response) {
  /// Send len
  uint32_t len = htonl(4 + 1 + response.size());
  if (send(connfd, &len, 4, 0) == -1) {
    return -1;
  }

  /// send the status
  char statusCh = status;
  if (send(connfd, &statusCh, 1, 0) < 0) {
    return -1;
  }
  
  /// send response
  if (send(connfd, response.data(), response.size(), 0) < 0) {
    return -1;
  }
  cerr << "[RPCServer] " << "Succeed to send response\n";
  return 0;
}



} // namespace rpc
