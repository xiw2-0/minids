/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include <rpc/client_protocol_proxy.hpp>

namespace rpc {

ClientProtocolProxy::ClientProtocolProxy(const string& serverIP, int serverPort):
  client(RPCClient(serverIP, serverPort)) {
}

ClientProtocolProxy::~ClientProtocolProxy() {
}


int ClientProtocolProxy::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks){
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "Connection failed\n";
    return -1;
  }

  /// send the request
  if (client.sendRequest(1, file) < 0) {
    cerr << "Failed to send request\n";
    return -1;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  locatedBlks->ParseFromString(response);

  /// close the connection
  client.closeConnection();

  return success;
}

int ClientProtocolProxy::create(const string& file, minidfs::LocatedBlock* locatedBlk) {
  /// connect Master
  if (client.connectMaster() < 0) {
    cerr << "Connection failed\n";
    return -1;
  }

  /// send the request
  if (client.sendRequest(2, file) < 0) {
    cerr << "Failed to send request\n";
    return -1;
  }

  /// recv the response
  int status;
  string response;
  int success = client.recvResponse(&status, &response);
  locatedBlk->ParseFromString(response);

  /// close the connection
  client.closeConnection();
  
  return success;
}


} // namespace rpc
