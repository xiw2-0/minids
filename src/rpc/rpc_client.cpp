/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <rpc/rpc_client.hpp>

namespace rpc {

RPCClient::RPCClient(const string& serverIP, int serverPort):
  serverIP(serverIP), serverPort(serverPort){
  sockfd = -1;
  
}

RPCClient::~RPCClient() {
}

int RPCClient::connectMaster() {
  /// set the server struct
  memset(&serverAddr, 0, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(serverPort);
  if (inet_pton(AF_INET, serverIP.c_str(), &serverAddr.sin_addr) < 0) {
    cerr << "inet_pton() error for: " << serverIP << std::endl;
    return -1;
  }

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    cerr << "Failed to create socket\n" << strerror(errno) << " errno: " << errno << std::endl; 
    return -1;
  }

  if (connect(sockfd, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
    cerr << "Cannot connect to " << serverIP << std::endl;
    return -1;
  }
  return 0;
}

void RPCClient::closeConnection() {
  close(sockfd);
}

int RPCClient::sendRequest(int methodID, const string& request) {
  int32_t len = 4 + 1 + request.size();

  string buf(5, 0);
  memcpy(&buf, &len, 4);
  buf[4] = methodID;
  buf += request;

  return send(sockfd, &buf, len, 0);
}

int RPCClient::recvResponse(int* status, string* response) {
  int32_t len = 0;
  /// read the length
  if (recv(sockfd, &len, 4, 0) < 0) {
    return -1;
  }
  
  string buf(len-4, 0);
  if (recv(sockfd, &buf, len-4, 0) < 0) {
    return -1;
  }
  
  *status = buf[0];
  *response = buf.substr(1, len-5);
  return 0;
}

} // namespace rpc
