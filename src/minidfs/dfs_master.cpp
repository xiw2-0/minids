/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implementation for class DFSMaster.


#include <minidfs/dfs_master.hpp>

namespace minidfs {

DFSMaster::DFSMaster(const string& nameSysFile,
                     int serverPort, int maxConns, int replicationFactor)
    : nameSysFile(nameSysFile),
      server(serverPort, maxConns, this), replicationFactor(replicationFactor) {
}

DFSMaster::~DFSMaster() {
}

int DFSMaster::format() {
  dfIDs.clear();
  dfIDs["/"] = 0;
  currentMaxDfID = 0;

  dfNames.clear();
  dfNames[0] = "/";

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

void DFSMaster::checkHeartbeat() {
  for (auto i = aliveChunkservers.begin(); i != aliveChunkservers.end();) {
    if (i->second == true) {
      i->second = false;
      ++i;
    } else {
      findBlksToBeReplicated(i->first);
      i = aliveChunkservers.erase(i);
    }
  }
}

int DFSMaster::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks) {
  if (dfIDs.find(file) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << "No such a file/dir\n";
    return OpCode::OP_NO_SUCH_FILE;
  }

  int dfid = dfIDs[file];
  if (inodes.find(dfid) == inodes.end()) {
    cerr << "[DFSMaster] "  << "No such a file\n";
    return OpCode::OP_NO_SUCH_FILE;
  }

  /// Set the return value
  for (int inodeid : inodes[dfid]) {
    auto locatedblk = locatedBlks->add_locatedblks();
    auto blk = locatedblk->mutable_block();
    *blk = blks[inodeid];

    /// block related chunkserver info
    for(const auto& cs : blkLocs[inodeid]) {
      auto chunkserverinfo = locatedblk->add_chunkserverinfos();
      *chunkserverinfo = cs;
    }
  }
  return OpCode::OP_SUCCESS;
}

int DFSMaster::create(const string& file, LocatedBlock* locatedBlk) {
  if (dfIDs.find(file) != dfIDs.end()) {
    cerr << "[DFSMaster] "  << file << " existed!\n";
    return OpCode::OP_FILE_ALREADY_EXISTED;
  }

  string dir;
  splitPath(file, dir);

  if (dfIDs.find(dir) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << "Dir " << dir << " does not exist!\n";
    return OpCode::OP_NO_SUCH_FILE;
  }

  /// if some other client is creating a file with the same name
  if (filesInCreating.find(file) != filesInCreating.end()) {
    cerr << "[DFSMaster] "  << "File is in creating\n";
    return OpCode::OP_FILE_IN_CREATING;
  }

  /// When the client creates a file,
  /// the master should add it to filesInCreating first.
  /// When the master receives the complete() RPC call, it
  /// finalizes the file into the namesystem and assigns a new
  /// DfID to the newly created file.
  
  /// TODO: xiw, add lock to protect currentMaxBlkID
  int newBlkid = ++currentMaxBlkID;
  filesInCreating[file] = std::vector<int>{newBlkid};

  auto retblock = locatedBlk->mutable_block();
  retblock->set_blockid(newBlkid);
  retblock->set_blocklen(0);

  std::vector<ChunkserverInfo> allocatedCS;
  if (-1 == allocateChunkservers(allocatedCS)) {
    cerr << "[DFSMaster] "  << "Chunkservers alive are fewer than replication factor\n";
    return OpCode::OP_FAILURE;
  }

  for (const auto& cs : allocatedCS) {
    *locatedBlk->add_chunkserverinfos() = cs;
  }

  cerr << "[DFSMaster] "  << "A file is in creating\n";
  return OpCode::OP_SUCCESS;
}

int DFSMaster::addBlock(const string& file, LocatedBlock* locatedBlk) {
  /// if the file isn't in creating process
  if (filesInCreating.find(file) == filesInCreating.end()) {
    cerr << "[DFSMaster] "  << file << " isn't in creating\n";
    return OpCode::OP_NO_SUCH_FILE;
  }
  
  /// TODO: xiw, add lock to protect currentMaxBlkID
  int newBlkid = ++currentMaxBlkID;
  filesInCreating[file] = std::vector<int>{newBlkid};

  auto retblock = locatedBlk->mutable_block();
  retblock->set_blockid(newBlkid);
  retblock->set_blocklen(0);

  std::vector<ChunkserverInfo> allocatedCS;
  if (-1 == allocateChunkservers(allocatedCS)) {
    cerr << "[DFSMaster] "  << "Chunkservers alive are fewer than replication factor\n";
    return OpCode::OP_FAILURE;
  }

  for (const auto& cs : allocatedCS) {
    *locatedBlk->add_chunkserverinfos() = cs;
  }

  cerr << "[DFSMaster] "  << "A block is to be added to " << file << std::endl;
  return OpCode::OP_SUCCESS;
}

int DFSMaster::blockAck(const LocatedBlock& locatedBlk) {
  /// add the confirmed block into blocksInCreating
  int blockID = locatedBlk.block().blockid();
  blocksInCreating[blockID] = locatedBlk;

  return OpCode::OP_SUCCESS;
}

int DFSMaster::complete(const string& file) {
  /// if the file isn't in creating process
  if (filesInCreating.find(file) == filesInCreating.end()) {
    cerr << "[DFSMaster] "  << file << " isn't in creating\n";
    return OpCode::OP_NO_SUCH_FILE;
  }

  /// assign dfID to this newly created file
  int newDfID = ++currentMaxDfID;
  dfIDs[file] = newDfID;
  /// add it dfNames at the same time
  dfNames[newDfID] = file;

  /// add it to dentries
  string dir;
  splitPath(file, dir);
  int dirID = dfIDs[dir];
  dentries[dirID].push_back(newDfID);

  /// add inode
  const auto fileBlks = filesInCreating[file];
  for (int b : fileBlks) {
    if (blocksInCreating.find(b) == blocksInCreating.end()) {
      continue;
    }
    inodes[newDfID].push_back(b);
    const auto lb = blocksInCreating[b];
    blks[b] = lb.block();    
    /// blkLocs are reported by chunkservers

    if (lb.chunkserverinfos_size() < replicationFactor) {
      blksToBeReplicated[b] = replicationFactor - lb.chunkserverinfos_size();
    }
    /// the block is created successfully and is removed from
    /// blocksInCreating
    blocksInCreating.erase(b);
  }

  /// the file is created successfully and is removed from
  /// filesInCreating
  filesInCreating.erase(file);

  return OpCode::OP_SUCCESS;
}

int DFSMaster::remove(const string& file) {
  if (dfIDs.find(file) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << file << " doesn't exist!\n";
    return OpCode::OP_NO_SUCH_FILE;
  }

  /// get dfID
  int dfid = dfIDs[file];
  dfIDs.erase(file);
  /// remove dfNames at the same time
  dfNames.erase(dfid);

  /// remove it from its parent directory
  string dir;
  splitPath(file, dir);
  int dirID = dfIDs[dir];

  auto& vec = dentries[dirID];
  for (auto ite = vec.begin(); ite != vec.end(); ++ite) {
    if (*ite == dfid) {
      vec.erase(ite);
      break;
    }
  }

  /// delete the corresponding inode
  auto& blockvec = inodes[dfid];
  for (int b : blockvec) {
    blks.erase(b);
  }
  inodes.erase(dfid);

  return OpCode::OP_SUCCESS;
}

int DFSMaster::exists(const string& file) {
  if (dfIDs.find(file) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << file << " doesn't exist!\n";
    return OpCode::OP_NOT_EXIST;
  }
  return OpCode::OP_EXIST;
}

int DFSMaster::makeDir(const string& dirName) {
  if (dfIDs.find(dirName) != dfIDs.end()) {
    cerr << "[DFSMaster] "  << dirName << " existed!\n";
    return OpCode::OP_FILE_ALREADY_EXISTED;
  }

  string dir;
  splitPath(dirName, dir);

  if (dfIDs.find(dir) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << "Dir " << dir << " does not exist!\n";
    return OpCode::OP_NO_SUCH_FILE;
  }
  
  /// assign dfID to this newly created folder
  int newDfID = ++currentMaxDfID;
  dfIDs[dirName] = newDfID;
  /// add it to dfNames
  dfNames[newDfID] = dirName;

  /// add it to dentries
  int dirID = dfIDs[dir];
  dentries[dirID].push_back(newDfID);
  dentries[newDfID] = std::vector<int>();

  return OpCode::OP_SUCCESS;
}

int DFSMaster::listDir(const string& dirName, FileInfos& items) {
  if (dfIDs.find(dirName) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << dirName << " doesn't exist!\n";
    return OpCode::OP_NO_SUCH_FILE;
  }
  int id = dfIDs[dirName];
  if (dentries.find(id) == dentries.end()) {
    cerr << "[DFSMaster] "  << dirName << " isn't a directory!\n";
    return OpCode::OP_NO_SUCH_FILE;
  }
  const auto& fileVec = dentries[id];
  for (int f : fileVec) {
    auto finfo = items.add_fileinfos();
    string name = dfNames[f];
    name = name.substr(name.find_last_of('/'));
    finfo->set_name(name);
    
    int isDir = 0;
    if (dentries.find(f) != dentries.end()) {
      isDir = 1;
    }
    finfo->set_isdir(isDir);

    finfo->set_filelen(getFileLength(f));
  }
  return OpCode::OP_SUCCESS;

}

int DFSMaster::heartBeat(const ChunkserverInfo& chunkserverInfo) {
  //int id = getChunkserverID(chunkserverInfo);

  aliveChunkservers[chunkserverInfo] = true;
  /// TODO: xiw, is returning value here useless?
  return OpCode::OP_SUCCESS;
}

int DFSMaster::blkReport(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) {
  //int id = getChunkserverID(chunkserverInfo);
  aliveChunkservers[chunkserverInfo] = true;
  for (int blockid : blkIDs) {
    /// all valid blocks should appear in blks
    if (blks.find(blockid) == blks.end()) {
      deletedBlks.push_back(blockid);
      continue;
    }
    /// confirm the blkLocs
    if (blkLocs.find(blockid) == blkLocs.end()) {
      blkLocs[blockid].push_back(chunkserverInfo);
    } else {
      int contained = 0;
      auto csEqualTo = ChunkserverInfoEqualTo();
      for (const auto& cs : blkLocs[blockid]) {
        if (csEqualTo(cs, chunkserverInfo)) {
          contained = 1;
          break;
        }
      }
      if (contained == 0) {
        blkLocs[blockid].push_back(chunkserverInfo);
      }
    }

  }
  return OpCode::OP_SUCCESS;
}

int DFSMaster::getBlkTask(const ChunkserverInfo& chunkserverInfo, BlockTasks* blkTasks) {
  //int id = getChunkserverID(chunkserverInfo);
  auto csEqualTo = ChunkserverInfoEqualTo();
  bool hasTask = false;
  for (auto& b : blksToBeReplicated) {
    for (auto& node : blkLocs[b.first]) {
      if ( csEqualTo(node, chunkserverInfo) ) {
        distributeBlkTask(b.first, b.second, blkTasks->add_blktasks());
        hasTask = true;
        break;
      }
    }
  }
  return hasTask ? OpCode::OP_SUCCESS : OpCode::OP_NO_BLK_TASK;
}

int DFSMaster::recvedBlks(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs) {
  //int id = getChunkserverID(chunkserverInfo);
  auto csEqualTo = ChunkserverInfoEqualTo();
  aliveChunkservers[chunkserverInfo] = true;
  for (int blockid : blkIDs) {
    /// all valid blocks should appear in blks
    if (blks.find(blockid) == blks.end()) {
      cerr << "[DFSMaster] " << chunkserverInfo.chunkserverip() << " received invalid block " << blockid << std::endl;
      continue;
    }
    if (blkLocs.find(blockid) == blkLocs.end()) {
      blkLocs[blockid] = std::vector<ChunkserverInfo>();
    }
    int contained = false;
    for(const auto& chunkserver : blkLocs[blockid]) {
      if ( csEqualTo(chunkserver, chunkserverInfo) ) {
        contained = true;
        break;
      }
    }
    if (contained == false)
      blkLocs[blockid].push_back(chunkserverInfo);
  }
  return OpCode::OP_SUCCESS;
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
    /// dfName
    dfNames[inodeID] = inode.name();

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
  /// chunkserver
  blkLocs.clear();
  //chunkservers = std::unordered_map<int, ChunkserverInfo>();
  //chunkserverIDs = std::unordered_map<ChunkserverInfo, int>();
  //currentMaxChunkserverID = 0;
  aliveChunkservers.clear();

  blksToBeReplicated.clear();
  
  /// name system
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

/*
int DFSMaster::getChunkserverID(const ChunkserverInfo& chunkserverInfo) {
  int id = -1;
  if (chunkserverIDs.find(chunkserverInfo) != chunkserverIDs.end()) {
    id = chunkserverIDs[chunkserverInfo];
  } else {
    id = ++currentMaxChunkserverID;

    chunkserverIDs[chunkserverInfo] = id;
    chunkservers[id] = chunkserverInfo;
  }
  return id;
}
*/

void DFSMaster::findBlksToBeReplicated(const ChunkserverInfo& chunkserver) {
  auto csEqualTo = ChunkserverInfoEqualTo();
  for (auto& bl : blkLocs) {
    for (const auto& blChunkserver : bl.second) {
      if ( csEqualTo(blChunkserver, chunkserver) ) {
        if (blksToBeReplicated.find(bl.first) == blksToBeReplicated.end()) {
          blksToBeReplicated[bl.first] = 0;
        }
        blksToBeReplicated[bl.first]++;
        break;
      }
    }
  }
}

void DFSMaster::distributeBlkTask(int blockID, int repFactor, BlockTask* blkTask) {
  blkTask->set_operation(OpCode::OP_COPY);

  auto csEqualTo = ChunkserverInfoEqualTo();

  auto locatedBlk = blkTask->mutable_locatedblk();
  *locatedBlk->mutable_block() = blks[blockID];
  int numFound = 0;
  for (const auto& i : aliveChunkservers) {
    bool contained = false;
    for (const auto& backup : blkLocs[blockID]) {
      if ( csEqualTo(backup, i.first) ) {
        contained = true;
        break;
      }
    }
    /// don't contain the target block and is alive
    if (contained == false && i.second == true) {
      *locatedBlk->add_chunkserverinfos() = i.first;
      ++numFound;
    }
    if (numFound >= repFactor) {
      break;
    }
  }
  if (numFound < repFactor) {
    cerr << "[DFSMaster] " << "Required " << repFactor << "repair nodes; "
         << "only " << numFound << " found\n";
  }
  blksToBeReplicated.erase(blockID);
}

int DFSMaster::allocateChunkservers(std::vector<ChunkserverInfo>& cs) {
  int nServers = aliveChunkservers.size();
  if (nServers < replicationFactor) {
    return -1;
  }
  std::vector<ChunkserverInfo> shuffleVec(nServers);

  auto ics = aliveChunkservers.cbegin();
  for (int i = 0; i < nServers && ics != aliveChunkservers.cend(); ++i, ++ics) {
    shuffleVec[i] = ics->first;
  }
  std::random_shuffle(shuffleVec.begin(), shuffleVec.end());
  for (int i = 0; i < replicationFactor; ++i) {
    cs.emplace_back(shuffleVec[i]);
  }
  return 0;
}

long long DFSMaster::getFileLength(int fileID) {
  if (inodes.find(fileID) == inodes.end()) {
    return -1;
  }

  const auto& blkVec = inodes[fileID];
  long long len = 0;
  for (int b : blkVec) {
    len += blks[b].blocklen();
  }

  return len;
}


} // namespace minidfs
