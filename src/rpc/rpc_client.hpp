/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class ClientProtocolProxy.

#ifndef RPC_CLIENT_
#define RPC_CLIENT_

#include <proto/minidfs.pb.h>

namespace rpc {

/// Use socket to communicate with master.
/// This class is used by client proxy and chunkserver proxy.
class RPCClient {
  private:
    /* data */
  public:
    RPCClient(/* args */);
    ~RPCClient();
};

RPCClient::RPCClient(/* args */)
{
}

RPCClient::~RPCClient()
{
}




} // namespace rpc





#endif