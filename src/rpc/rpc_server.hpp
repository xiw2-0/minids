/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class RPCServer.

#ifndef RPC_SERVER_
#define RPC_SERVER_

namespace rpc {

/// RPC server binds method name to member functions.
/// Uses a map to transform string to member function calls.
/// It uses while(1) loop to wait for 
/// In the member function call, it parses from the request and
/// and call the coresponding method in client protocol handler
/// or chunkserver handler.
/// After return from handlers, it serializes the return values to
/// socket. And return the rpc calls.   
class RPCServer {
 private:
  /* data */
 public:
  RPCServer(/* args */);
  ~RPCServer();
};

RPCServer::RPCServer(/* args */)
{
}

RPCServer::~RPCServer()
{
}


} // namespace rpc





#endif