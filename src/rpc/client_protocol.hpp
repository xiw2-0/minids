/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for Protocol b/w client and master.

#ifndef CLIENT_PROTOCOL_H_
#define CLIENT_PROTOCOL_H_

#include <string>

using std::string;

namespace rpc {

/// \brief ClientProtocol is the communicate protocol between DFSClient with Master.
///
/// It communicates with Master node to get/set the metadata of files/directories.
/// This class is just an interface.
class ClientProtocol {
  public:
    /// \brief Get a file's block location information from Master.
    ///
    /// \param file the file name stored in minidfs.
    /// \return void
    virtual void getBlockLocations(string file) = 0;

    /// \brief Create a file.
    ///
    /// This operation informs Master to create the meta info for the first block
    /// \param file the file name stored in minidfs.
    /// \return void
    virtual void create(string file) = 0;
};

} // namespace rpc
    
#endif