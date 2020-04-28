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

int DFSMaster::format() {
  dfIDs.clear();
  dfIDs["/"] = 0;
  currentMaxDfID = 0;

  dentries.clear();
  dentries[0] = std::vector<int>();

  inodes.clear();
  /// block id starts from 1
  currentMaxBlkID = 0;

  blks.clear();
  blkLocs.clear();

  if (serializeNameSystem() == -1) {
    cerr << "[DFSMaster] " << "Failed to format name system!\n";
    return -1;
  }
  cerr << "[DFSMaster] "  << "Formated!\n";
  return 0;
}

void DFSMaster::startRun() {
  if (initMater() < 0) {
    cerr << "[DFSMaster] "  << "Init master failure\n";
    return;
  }
  if (server.init() == -1) {
    cerr << "[DFSMaster] "  << "Init server of master failure\n";
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
    cerr << "[DFSMaster] "  << "No such a file/dir\n";
    return 1;
  }

  int dfid = dfIDs[file];
  if (inodes.find(dfid) == inodes.end()) {
    cerr << "[DFSMaster] "  << "No such a file\n";
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
  if (dfIDs.find(file) != dfIDs.end()) {
    cerr << "[DFSMaster] "  << file << " existed!\n";
    return 1;
  }

  string dir;
  splitPath(file, dir);

  if (dfIDs.find(dir) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << "Dir " << dir << " does not exist!\n";
    return 1;
  }

  /// TODO: xiw, add a lock to guard currentMaxDfID
  int newDfID = ++currentMaxDfID;
  dfIDs[file] = newDfID;

  dentries[dfIDs[dir]].push_back(newDfID);

  int newBlkid = ++currentMaxBlkID;
  inodes[newDfID].push_back(newBlkid);

  /// just in test
  auto retblock = locatedBlk->mutable_block();
  retblock->set_blockid(newBlkid);
  retblock->set_blocklen(10);

  auto chunkserverinfo = locatedBlk->add_chunkserverinfos();
  chunkserverinfo->set_chunkserverip("ip_test");
  chunkserverinfo->set_chunkserverport(18000);

  cerr << "[DFSMaster] "  << "Created a file\n";
  return 0;
}



int DFSMaster::serializeNameSystem() {
  std::ofstream fs(nameSysFile, std::ios::out|std::ios::binary);
  if (!fs.is_open()) {
    cerr << "[DFSMaster] "  << "Failed to open Name system file: " << nameSysFile << std::endl;
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
  cerr << "[DFSMaster] "  << "Succeed to serialize the name system to file: " << nameSysFile << std::endl;
  return 0;
}

int DFSMaster::parseNameSystem() {
  std::ifstream fs(nameSysFile, std::ios::in|std::ios::binary);
  if (!fs.is_open()) {
    cerr << "[DFSMaster] "  << "Failed to open Name system file: " << nameSysFile << std::endl;
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
  cerr << "[DFSMaster] "  << "Succeed to parse the name system from file: " << nameSysFile << std::endl;
  return 0;
}

int DFSMaster::initMater() {
  blkLocs = std::map<int, std::vector<int>>();
  chunkservers = std::map<int, ChunkserverInfo>();
  return parseNameSystem();
}

void DFSMaster::splitPath(const string& path, string& dir) {
  int index = path.find_last_of('/');
  if (index == -1) {
    dir = "/home";
  } else if (index == 0) {
    dir = "/";
  } else {
    dir = path.substr(0, index);
  }
}



} // namespace minidfs
