/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for class DFSClient.

#ifndef DFS_CLIENT_H_
#define DFS_CLIENT_H_

#include <string>

using std::string;

namespace minidfs {

/// \brief DFSClient is used to communicate with Master and Chunkserver.
///
/// It communicates with Master node to get/set the metadata of files/directories.
/// It interacts with Chunkserver to send/recv chunk data.
class DFSClient {
  public:
    DFSClient(/* args */);
    ~DFSClient();

    /// \brief Get a file's block location information from Master.
    ///
    /// \param file the file name stored in minidfs.
    /// \return void
    void getBlockLocations(string file);

    /// \brief Create a file.
    ///
    /// This operation informs Master to create the meta info for the first block
    /// \param file the file name stored in minidfs.
    /// \return void
    void create(string file);


  private:
    /// 
    
};





} // namespace minidfs

#endif