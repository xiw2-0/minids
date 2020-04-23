/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RPCClient.

#ifndef RPC_CLIENT_
#define RPC_CLIENT_

#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
//#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <string>
#include <proto/minidfs.pb.h>

using std::cout;
using std::cerr;
using std::string;

namespace rpc {

/// \brief RPCClient uses tcp socket to communicate with master.
/// This class is used by client proxy and chunkserver proxy.
class RPCClient {
 private:
  string serverIP;
  int serverPort;
  struct sockaddr_in serverAddr;

  int sockfd;

 public:
  /// \brief Construct the RPCClient.
  ///
  /// \param serverIP the Master's IP address 
  /// \param serverPort Master's serving port
  RPCClient(const string& serverIP, int serverPort);

  ~RPCClient();

  /// \brief Connect the Master.
  ///
  /// \return return 0 on success, -1 for errors.
  int connectMaster();

  /// \brief Close the connection.
  void closeConnection();

  /// \brief Send rpc request to the Master.
  ///
  /// The format of request is:
  /// len(4 Byte) : methodID(1 Byte) : request
  ///
  /// \param methodID remote procedure call ID
  /// \param request the serialized parameters
  /// \return return 0 on success, -1 for errors.
  int sendRequest(int methodID, const string& request);

  /// \brief Recv rpc response from the Master.
  ///
  /// The format of reponse is:
  /// len(4 Byte) : status(1 Byte) : response
  /// \return return 0 on success, -1 for errors.
  int recvResponse(int* status, string* response);
};

} // namespace rpc



#endif