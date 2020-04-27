/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi


#include <rpc/rpc_server.hpp>
#include <minidfs/dfs_master.hpp>


namespace rpc {

RPCServer::RPCServer(int serverPort, int maxConns, minidfs::DFSMaster* master)
    : serverPort(serverPort), maxConnections(maxConns), master(master), isSafeMode(true) {
}

RPCServer::~RPCServer() {
}

int RPCServer::init() {
  if (bindRPCCalls() < 0) {
    cerr << "Failed to bind RPC calls\n";
    return -1;
  }
  if (initServer() < 0) {
    cerr << "Failed to init server\n";
    return -1;
  }
  return 0;
}

void RPCServer::run() {
  while(true) {
    int connfd = accept(listenSockfd, (struct sockaddr*)NULL, NULL);
    if (connfd == -1) {
      cerr << "Failed to accept socket " << strerror(errno) << std::endl;
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
    cerr << "Failed to create a socket: " << strerror(errno) << std::endl;
    return -1;
  }

  memset(&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddr.sin_port = htons(serverPort);

  if (bind(listenSockfd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == -1) {
    cerr << "Failed to bind the socket: " << strerror(errno) << std::endl;
    return -1;
  }

  if (listen(listenSockfd, maxConnections) == -1) {
    cerr << "Failed to listen to the socket: " << strerror(errno) << std::endl;
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
  int32_t len = 0;
  /// read the length
  if (recv(connfd, &len, 4, 0) < 0) {
    cerr << "Failed to recv data length from " << connfd << std::endl;
    close(connfd);
    return;
  }
  
  string buf(len-4, 0);
  if (recv(connfd, &buf, len-4, 0) < 0) {
    cerr << "Failed to recv data from " << connfd << std::endl;
    close(connfd);
    return;
  }

  int methodID = buf[0];

  /// In safe mode
  if (isSafeMode == true && methodID != 1) {
    int32_t len = 4 + 1;
    string buf(5, 0);
    memcpy(&buf, &len, 4);
    /// Safe mode code = 100
    buf[4] = 100;
    send(connfd, &buf, len, 0);

    cerr << "In safe mode\n";
    close(connfd);
    
    isSafeMode = !master->isSafe();
    return;
  }

  auto func = rpcBindings[methodID];
  if (func(connfd, buf.substr(1, len-5)) < 0) {
    cerr << "Failed to send response to " << connfd << std::endl;
    close(connfd);
    return;
  }
  cerr << "Succeed to process one request\n";
  /// close client sock
  close(connfd);
}

/// Format of response: len(4 Byte) : status(1 Byte) : response
int RPCServer::getBlockLocations(int connfd, const string& request) {
  minidfs::LocatedBlocks locatedBlks;
  int status = master->getBlockLocations(request, &locatedBlks);
  // TODO: use SerializeAsString or SerializeToString?
  string response = locatedBlks.SerializeAsString();

  int32_t len = 4 + 1 + response.size();
  string buf(5, 0);
  memcpy(&buf, &len, 4);
  buf[4] = status;
  buf += request;

  /// reply to the client
  return send(connfd, &buf, len, 0);
}

int RPCServer::create(int connfd, const string& request) {
  minidfs::LocatedBlock locatedBlk;
  int status = master->create(request, &locatedBlk);
  // TODO: use SerializeAsString or SerializeToString?
  string response = locatedBlk.SerializeAsString();

  int32_t len = 4 + 1 + response.size();
  string buf(5, 0);
  memcpy(&buf, &len, 4);
  buf[4] = status;
  buf += request;

  return send(connfd, &buf, len, 0);
}

} // namespace rpc
