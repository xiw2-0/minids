/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <rpc/client_protocol_proxy.hpp>

namespace rpc {

ClientProtocolProxy::ClientProtocolProxy(const string& serverIP, int serverPort)
    : client(RPCClient(serverIP, serverPort)) {
}

ClientProtocolProxy::~ClientProtocolProxy() {
}


int ClientProtocolProxy::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks){
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(file, 1)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  locatedBlks->ParseFromString(response);

  /// close the connection
  client.closeConnection();

  return status;
}

int ClientProtocolProxy::create(const string& file, minidfs::LocatedBlock* locatedBlk) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(file, 2)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  locatedBlk->ParseFromString(response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::addBlock(const string& file, minidfs::LocatedBlock* locatedBlk) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(file, 3)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  locatedBlk->ParseFromString(response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::blockAck(const minidfs::LocatedBlock& locatedBlk) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(locatedBlk.SerializeAsString(), 4)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::complete(const string& file) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(file, 5)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::remove(const string& file) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(file, 11)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::exists(const string& file) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(file, 12)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::makeDir(const string& dirName) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(dirName, 13)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::listDir(const string& dirName, minidfs::FileInfos& items) {
  /// send request
  int connOp = -1;
  if ((connOp = connectAndRequest(dirName, 14)) != OpCode::OP_SUCCESS) {
    return connOp;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  items.ParseFromString(response);

  /// close the connection
  client.closeConnection();
  
  return status;
}

int ClientProtocolProxy::connectAndRequest(const string& requst, int methodID) {
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "[ClientProtocolProxy] "  << "Connection failed\n";
    return OpCode::OP_FAILURE;
  }

  /// send the request
  if (client.sendRequest(methodID, requst) < 0) {
    cerr << "[ClientProtocolProxy] "  << "Failed to send request\n";
    return OpCode::OP_FAILURE;
  }
  return OpCode::OP_SUCCESS;
}

} // namespace rpc
