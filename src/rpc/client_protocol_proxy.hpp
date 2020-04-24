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

/// \brief ClientProtocolProxy uses rpc client to communicate with master.
///
/// It serializes the method calls' parameters and uses rpc
/// client to send the serialized function call to master.
/// Make sure to call startRPCClient() before rpc communication.
class ClientProtocolProxy: public minidfs::ClientProtocol {
 private:
  RPCClient client;

 public:
  /// \brief Construct a RPCClient in this constructor. 
  ClientProtocolProxy(const string& serverIP, int serverPort);

  ~ClientProtocolProxy();

  /// \brief Get a file's block location information from Master. MethodID = 1.
  ///
  /// \param file the file name stored in minidfs.
  /// \param locatedBlks a list of locatedBlocks which maps from a block ID to chunkservers.
  ///        It is the returning parameter. 
  /// \return returning status. 0 on success, 1 for errors.
  virtual int getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks) override;

  /// \brief Create a file. MethodID = 2.
  ///
  /// This operation informs Master to create the meta info for the first block
  /// \param file the file name stored in minidfs.
  /// \param locatedBlk contains chunkservers' information.
  ///        It is the returning parameter. 
  /// \return returning status. 0 on success, 1 for errors.
  virtual int create(const string& file, minidfs::LocatedBlock* locatedBlk) override;
};



} // namespace rpc





#endif