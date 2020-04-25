/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implementation for class DFSMaster.


#include <minidfs/dfs_master.hpp>

namespace minidfs {

DFSMaster::DFSMaster(const string& nameSysFile,
                     int serverPort, int maxConns)
    : nameSysFile(nameSysFile),
      server(serverPort, maxConns, this) {
}

DFSMaster::~DFSMaster() {
}

void DFSMaster::startRun() {
  
}

int DFSMaster::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks) {

}


int DFSMaster::parseNameSystem() {
  std::ifstream fs(nameSysFile, std::ios::in|std::ios::binary);
  if (fs.is_open()) {
    cerr << "Failed to open Name system file: " << nameSysFile << std::endl;
    return -1;
  }

  NameSystem namesys;
  namesys.ParseFromIstream(&fs);
  auto inodeSection = namesys.inodesection();
  auto dentrySection = namesys.dentrysection();

  //currentMaxDfID = namesys.maxdfID();


  /// INODE SECTION
  for (int i = 0; i < inodeSection.inodes_size(); ++i) {
    const auto inode = inodeSection.inodes(i);
    int inodeID = inode.id();
    /// dfID
    dfIDs[inode.name()] = inodeID;

    if (inode.isdir() == true) {
      continue;
    }

    inodes[inodeID] = std::vector<int>();
    for (int j = 0; j < inode.blks_size(); ++j) {
      int blkID = inode.blks(j).blockid();
      /// blks
      blks[blkID] = inode.blks(j);
      /// inodes
      inodes[inodeID].push_back(blkID);
    }
    
  }

  /// DENTRY SECTION
  for (int i = 0; i < dentrySection.dentries_size(); ++i) {
    const auto dentry = dentrySection.dentries(i);
    dentries[dentry.id()] = std::vector<int>();
    /// dentries
    for (int j = 0; j < dentry.subdentries_size(); ++j) {
      dentries[j].push_back(dentry.subdentries(j));
    }
  }

  fs.clear();
  fs.close();
}

} // namespace minidfs
