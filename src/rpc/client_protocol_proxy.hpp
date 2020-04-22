/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class ClientProtocolProxy.

#ifndef CLIENT_PROTOCOL_PROXY_
#define CLIENT_PROTOCOL_PROXY_

#include <minidfs/client_protocol.hpp>
#include <proto/minidfs.pb.h>
#include <rpc/rpc_client.hpp>

namespace rpc {

/// Uses rpc_client to communicate with master.
/// Serializes the method calls' parameters and uses rpc
/// client to send the serialized function call to master.
class ClientProtocolProxy: public minidfs:: ClientProtocol {
  private:
    RPCClient client;

  public:
    ClientProtocolProxy(/* args */);
    ~ClientProtocolProxy();

      public:
    /// \brief Get a file's block location information from Master.
    ///
    /// \param file the file name stored in minidfs.
    /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
    ///        It is the returning parameter. 
    /// \return returning status. 0: success; 1: fail.
    virtual int getBlockLocations(const string& file, minidfs:: LocatedBlocks* locatedBlks) override;

    /// \brief Create a file.
    ///
    /// This operation informs Master to create the meta info for the first block
    /// \param file the file name stored in minidfs.
    /// \param locatedBlk contains chunkservers' information.
    ///        It is the returning parameter. 
    /// \return returning status. 0 for success; 1 for fail.
    virtual int create(const string& file, minidfs:: LocatedBlock* locatedBlk) override;
};



ClientProtocolProxy::ClientProtocolProxy(/* args */)
{
}

ClientProtocolProxy::~ClientProtocolProxy()
{
}



} // namespace rpc





#endif