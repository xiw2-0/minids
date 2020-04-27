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
  if (initMater() < 0) {
    cerr << "Init master failure\n";
    return;
  }

  server.run();
}

int DFSMaster::checkpoint() {
  return serializeNameSystem();
}

bool DFSMaster::isSafe() {
  if (blkLocs.size() < blks.size()) {
    return false;
  }
  return true;
}

int DFSMaster::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks) {
  if (dfIDs.find(file) == dfIDs.end()) {
    cerr << "No such a file/dir\n";
    return 1;
  }

  int dfid = dfIDs[file];
  if (inodes.find(dfid) == inodes.end()) {
    cerr << "No such a file\n";
    return 1;
  }

  /// Set the return value
  for (int inodeid : inodes[dfid]) {
    auto locatedblk = locatedBlks->add_locatedblks();
    auto blk = locatedblk->mutable_block();
    *blk = blks[inodeid];

    /// block related chunkserver info
    for(int chkserverID : blkLocs[inodeid]) {
      auto chunkserverinfo = locatedblk->add_chunkserverinfos();
      *chunkserverinfo = chunkservers[chkserverID];
    }
  }
  return 0;
}

int DFSMaster::create(const string& file, LocatedBlock* locatedBlk) {
  
}



int DFSMaster::serializeNameSystem() {
  std::ofstream fs(nameSysFile, std::ios::out|std::ios::binary);
  if (fs.is_open()) {
    cerr << "Failed to open Name system file: " << nameSysFile << std::endl;
    return -1;
  }

  NameSystem namesys;
  
  namesys.set_maxdfid(currentMaxDfID);
  namesys.set_maxblkid(currentMaxBlkID);
  
  auto inodeSection = namesys.mutable_inodesection();
  auto dentrySection = namesys.mutable_dentrysection();

  /// INODE SECTION
  for (auto i = dfIDs.cbegin(); i != dfIDs.cend(); ++i) {
    auto inode = inodeSection->add_inodes();

    inode->set_name(i->first);
    int dfid = i->second;
    inode->set_id(dfid);

    /// dir
    if (inodes.find(dfid) == inodes.end()) {
      inode->set_isdir(true);
    }

    inode->set_isdir(false);
    for (const auto& blkid : inodes[dfid]) {
      /// blks
      auto blk = inode->add_blks();
      *blk = blks[blkid];
    }
  }

  /// DENTRY SECTION
  for (auto i = dentries.cbegin(); i != dentries.cend(); ++i) {
    auto dentry = dentrySection->add_dentries();
    /// dentries
    dentry->set_id(i->first);
    for (const auto& j : i->second) {
      dentry->add_subdentries(j);
    }
  }
  
  namesys.SerializePartialToOstream(&fs);
  fs.clear();
  fs.close();
  cerr << "Succeed to serialize the name system to file: " << nameSysFile << std::endl;
  return 0;
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

  currentMaxDfID = namesys.maxdfid();
  currentMaxBlkID = namesys.maxblkid();


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
  cerr << "Succeed to parse the name system from file: " << nameSysFile << std::endl;
  return 0;
}

int DFSMaster::initMater() {
  blkLocs = std::map<int, std::vector<int>>();
  chunkservers = std::map<int, ChunkserverInfo>();
  return parseNameSystem();
}

} // namespace minidfs
