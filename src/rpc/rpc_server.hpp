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
#include <unistd.h>

#include <proto/minidfs.pb.h>
#include <minidfs/op_code.hpp>

using std::string;
using std::cerr;
using minidfs::OpCode;

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

  std::unordered_map<int, std::function<int(int, const string&)>> rpcBindings;

 public:
  /// \brief When in safemode, only recv the block-report rpc calls from chunkserver.
  bool isSafeMode;

 public:

  /// \brief Construct the server.
  ///
  /// \param serverPort Master's serving port
  /// \param maxConns maximum number of connections
  RPCServer(int serverPort, int maxConns, minidfs::DFSMaster* master);

  ~RPCServer();

  /// \brief Initialize the server and bind the rpc method ID
  /// to member function. Call this method before calling run().
  ///
  /// \return return 0 on success, -1 for errors.
  int init();

  /// \brief Wait for the requests from clients and chunkservers.
  /// It runs endlessly. Call init() before run()!
  ///
  /// The first stage of run() is in safe mode.
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


  ////////////////////////
  /// ClientProtocol
  ////////////////////////


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


  ////////////////////////
  /// ChunkserverProtocol
  ////////////////////////


  /// \brief Send heartbeat information to Master. MethodID = 101.
  ///
  /// \param connfd the connected sockfd
  /// \param request serialized chunkserverInfo which contains the ip and port of the chunkserver
  /// \return return 0 on success, -1 for errors.
  int heartBeat(int connfd, const string& request);
  
  /// \brief Send block report to Master. MethodID = 102.
  ///
  /// The chunkserver informs Master about all the blocks it has
  /// \param connfd the connected sockfd
  /// \param request serialized BlockReport
  /// \return return 0 on success, -1 for errors.
  int blkReport(int connfd, const string& request);

  /// \brief Get block task from Master. MethodID = 103.
  ///
  /// \param connfd the connected sockfd
  /// \param request serialized chunkserverInfo which contains the ip and port of the chunkserver
  /// \return return 0 on success, -1 for errors.  
  int getBlkTask(int connfd, const string& request);

  /// \brief Inform Master about the received blocks. MethodID = 104.
  ///
  /// \param connfd the connected sockfd
  /// \param request serialized BlkIDs that contains all the block ids it received
  /// \return return 0 on success, -1 for errors.  
  int recvedBlks(int connfd, const string& request);



  /// \brief Recv rpc request from the RPCClient.
  ///
  /// The format of request is:
  /// len(4 Byte) : methodID(1 Byte) : request
  ///
  /// \param connfd the accepted socket fd.
  /// \param methodID remote procedure call ID
  /// \param request the serialized parameters
  /// \return return 0 on success, -1 for errors.
  int recvRequest(int connfd, int& methodID, string& request);

  /// \brief Send rpc response to the RPCClient.
  ///
  /// The format of reponse is:
  /// len(4 Byte) : status(1 Byte) : response
  /// \param connfd the accepted socket fd.
  /// \return return 0 on success, -1 for errors.
  int sendResponse(int connfd, int status, const string& response);
};


} // namespace rpc





#endif