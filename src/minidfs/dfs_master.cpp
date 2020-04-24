/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implementation for class DFSMaster.


#include <minidfs/dfs_master.hpp>

namespace minidfs {

DFSMaster::DFSMaster(const string& dfIDFile, const string& dentryFile,
                     const string& inodeFile, int serverPort, int maxConns)
    : dfIDFile(dfIDFile), dentryFile(dentryFile), inodeFile(inodeFile),
      server(serverPort, maxConns) {
}

DFSMaster::~DFSMaster() {
}

void DFSMaster::startRun() {
  
}

int DFSMaster::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks) {

}

} // namespace minidfs
