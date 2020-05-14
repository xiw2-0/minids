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
    cerr << "[RPCClient] "  << "inet_pton() error for: " << serverIP << std::endl;
    return -1;
  }

  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    cerr << "[RPCClient] "  << "Failed to create socket\n" << strerror(errno) << " errno: " << errno << std::endl; 
    return -1;
  }

  if (connect(sockfd, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
    cerr << "[RPCClient] "  << "Cannot connect to " << serverIP << std::endl;
    return -1;
  }
  //cerr << "[RPCClient] "  << "Succeed to connect Master:\n" << serverIP << ":" << serverPort << std::endl;
  return 0;
}

void RPCClient::closeConnection() {
  close(sockfd);
}

int RPCClient::sendRequest(int methodID, const string& request) {
  /// Send len
  uint32_t len = htonl(4 + 1 + request.size());
  if (send(sockfd, &len, 4, 0) == -1) {
    return -1;
  }

  /// Send methodID
  char mID = methodID;
  if (send(sockfd, &mID, 1, 0) == -1) {
    return -1;
  }

  /// Send request
  int ret = send(sockfd, request.c_str(), request.size(), 0);
  if (ret == 0) {
    //cerr << "[RPCClient] " << "Succeed to send request\n";
  }
  return ret;
}

int RPCClient::recvResponse(int* status, string* response) {
  int32_t len = 0;
  /// read the length
  if (recv(sockfd, &len, 4, 0) < 0) {
    return -1;
  }
  len = ntohl(len);

  /// read the status
  char statusCh = 0;
  if (recv(sockfd, &statusCh, 1, 0) < 0) {
    return -1;
  }
  *status = statusCh;
  
  /// read response
  std::vector<char> buf(len-5);

  if (recv(sockfd, buf.data(), len-5, 0) < 0) {
    return -1;
  }
  *response = string(buf.begin(), buf.end());

  return 0;
}

} // namespace rpc
