/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include <rpc/rpc_server.hpp>
#include <minidfs/dfs_master.hpp>


namespace rpc {

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
  rpcBindings[1] = std::bind(&RPCServer::getBlockLocations, this, std::placeholders::_1, std::placeholders::_2);
  rpcBindings[2] = std::bind(&RPCServer::create, this, std::placeholders::_1, std::placeholders::_2);
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

  /// In safe mode
  if (isSafeMode == true && methodID != 1) {
    int status = 100;
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
  // TODO: use SerializeAsString or SerializeToString?
  string response = locatedBlks.SerializeAsString();

  /// reply to the client
  return sendResponse(connfd, status, response);
}

int RPCServer::create(int connfd, const string& request) {
  minidfs::LocatedBlock locatedBlk;
  int status = master->create(request, &locatedBlk);
  // TODO: use SerializeAsString or SerializeToString?
  string response = locatedBlk.SerializeAsString();

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
