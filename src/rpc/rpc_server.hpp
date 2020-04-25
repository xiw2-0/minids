/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RPCServer.

#ifndef RPC_SERVER_
#define RPC_SERVER_

#include <memory>
#include <string>
#include <iostream>
#include <functional>

#include <sys/socket.h>
#include <errno.h>
#include <netinet/in.h>
#include <thread>

#include <proto/minidfs.pb.h>

using std::string;
using std::cerr;

namespace minidfs {
class DFSMaster;
} // namespace minidfs



namespace rpc {

/// \brief RPCServer waits for the calls and forwards
/// the call to \class DFSMaster.
///
/// RPC server binds method name to member functions.
/// Uses a map to transform string to member function calls.
/// It uses while(1) loop to wait for requests from clients and chunkservers.
/// In the member function call, it parses from the request and
/// and call the coresponding method in client protocol handler
/// or chunkserver handler.
/// After return from handlers, it serializes the return values to
/// socket. And return the rpc calls.  
/// TODO: xiw, how to stop this server? 
class RPCServer {
 private:
  /// \brief Master is responsible for dealing with the requests.
  ///
  /// When \class RPCServer receives a rpc call, it forwards the 
  /// call to master.
  /// Changed from unique_ptr to normal ptr.
  minidfs::DFSMaster* master;

  int serverPort;
  struct sockaddr_in serverAddr;
  int listenSockfd;
  int maxConnections;

  std::map<int, std::function<int(int, const string&)>> rpcBindings;

 public:

  /// \brief Construct the server.
  ///
  /// \param serverPort Master's serving port
  /// \param maxConns maximum number of connections
  RPCServer(int serverPort, int maxConns, minidfs::DFSMaster* master);

  ~RPCServer();

  /// \brief Initialize the server and bind the rpc method ID
  /// to member function. Call this method before calling run().
  int init();

  /// \brief Wait for the requests from clients and chunkservers.
  /// It runs endlessly. Call init() before run()!
  void run();

 private:
  /// \brief Bind the ip and port using socket.
  ///
  /// \return return 0 on success, -1 for errors.
  int initServer();

  /// \brief Bind the rpc method IDs with member functions. The Method ID starts
  /// from 1.
  ///
  /// \return return 0 on success, -1 for errors.
  int bindRPCCalls();

  /// \brief Handle the request.
  ///
  /// \param connfd the accepted socket fd.
  void handleRequest(int connfd);


  /// \brief Get a file's block location information from Master. MethodID = 1.
  /// This method forwards the request to master and fetches the response.
  /// Then it sends the response back to client.
  ///
  /// \param connfd the connected sockfd
  /// \param request the file name stored in minidfs
  /// \return return 0 on success, -1 for errors.
  int getBlockLocations(int connfd, const string& request);

  /// \brief Create a file. MethodID = 2.
  /// This operation informs Master to create the meta info for the first block.
  /// This method forwards the request to master and fetches the response.
  /// Then it sends the response back to client.
  ///
  /// \param connfd the connected sockfd
  /// \param request the file name to be stored in minidfs.
  /// \return return 0 on success, -1 for errors.
  int create(int connfd, const string& request);
};


} // namespace rpc





#endif