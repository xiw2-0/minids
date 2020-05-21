/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Implementation for class DFSMaster.


#include <minidfs/dfs_master.hpp>

namespace minidfs {

DFSMaster::DFSMaster(const string& nameSysFile, const string& editLogFile,
                     int serverPort, int maxConns, int replicationFactor, size_t nThread)
    : nameSysFile(nameSysFile), editLogFile(editLogFile),
      server(serverPort, maxConns, this, nThread), replicationFactor(replicationFactor) {
  editlogID = 0;
}

DFSMaster::~DFSMaster() {
}

int DFSMaster::format() {
  {
    std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys, std::defer_lock);
    std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

    std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
    std::lock(lockFileNameSys, lockMemoryNameSys, lockChunkserverBlock);

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

    /// clear editlog
    editlogID = 0;
    std::ofstream f(editLogFile, std::ios::trunc | std::ios::out | std::ios::binary);
    if (f.is_open() == false){
      cerr << "[DFSMaster] " << "Failed to open edit log!\n";
      return -1;
    }
    f.clear();
    f.close();
  }


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
  std::thread checkerThread(&DFSMaster::statusChecker, this);
  checkerThread.detach();

  server.run();
}

int DFSMaster::checkpoint() {
  {
    std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys);
    /// clear editlog
    editlogID = 0;
    std::ofstream f(editLogFile, std::ios::trunc | std::ios::out | std::ios::binary);
    f.clear();
    f.close();  
  }

  return serializeNameSystem();
}

bool DFSMaster::isSafe() {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::lock(lockMemoryNameSys, lockChunkserverBlock);

  if (blkLocs.size() < blks.size()) {
    return false;
  }
  return true;
}

void DFSMaster::statusChecker() {
  auto lastCheck = std::chrono::system_clock::now();
  while(true) {
    auto now = std::chrono::system_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastCheck);
    if (dur.count() >= STATUS_CHECK_INTERVAL) {
      lastCheck = std::chrono::system_clock::now();
      /// check whether a chunkserver is still alive
      {
        cerr << "[DFSMaster] " << "Checking active chunkservers\n";
        std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock);
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

      /// check the edit log size
      {
        cerr << "[DFSMaster] " << "Checking edit log length " << editlogID << " " << maxEditLogEntry << std::endl;
        if (editlogID > maxEditLogEntry) {
          if (-1 == checkpoint()){
            cerr << "[DFSMaster] " << "Failed to take a checkpoint\n";
          } else {
            cerr << "[DFSMaster] " << "Succeeded to take a checkpoint\n";
          }
        }
      }
    }
    now = std::chrono::system_clock::now();
    dur = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastCheck);
    std::this_thread::sleep_for(std::chrono::milliseconds(STATUS_CHECK_INTERVAL)-dur);
  }
  
}

int DFSMaster::getBlockLocations(const string& file, minidfs::LocatedBlocks* locatedBlks) {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::lock(lockMemoryNameSys, lockChunkserverBlock);

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
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockInCreating(mutexInCreating, std::defer_lock);

  std::lock(lockMemoryNameSys, lockChunkserverBlock, lockInCreating);
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
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockInCreating(mutexInCreating, std::defer_lock);

  std::lock(lockChunkserverBlock, lockInCreating);

  /// if the file isn't in creating process
  if (filesInCreating.find(file) == filesInCreating.end()) {
    cerr << "[DFSMaster] "  << file << " isn't in creating\n";
    return OpCode::OP_NO_SUCH_FILE;
  }
  
  int newBlkid = ++currentMaxBlkID;
  filesInCreating[file].push_back(newBlkid);

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
  std::unique_lock<std::recursive_mutex> lockInCreating(mutexInCreating);

  /// add the confirmed block into blocksInCreating
  int blockID = locatedBlk.block().blockid();
  blocksInCreating[blockID] = locatedBlk;

  return OpCode::OP_SUCCESS;
}

int DFSMaster::complete(const string& file) {
  std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);
  
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockInCreating(mutexInCreating, std::defer_lock);

  std::lock(lockFileNameSys, lockMemoryNameSys, lockChunkserverBlock, lockInCreating);
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

  /// log the edit to disk
  EditLog editlog;
  editlog.set_op(OpCode::OP_CREATE);
  editlog.set_src(file);
  editlog.set_dfid(newDfID);
  for (int b : inodes[newDfID]) {
    *editlog.add_blks() = blks[b];
  }
  if (-1 == logEdit(editlog.SerializeAsString())) {
    cerr << "[DFSMaster] " << "Failed to create " << file << std::endl;
    return OpCode::OP_LOG_FAILURE;
  }
  editlogID++;
  cerr << "[DFSMaster] "  << file << " created\n";
  return OpCode::OP_SUCCESS;
}

int DFSMaster::remove(const string& file) {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys);

  if (dfIDs.find(file) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << file << " doesn't exist!\n";
    return OpCode::OP_NO_SUCH_FILE;
  }

  /// get dfID
  int dfid = dfIDs[file];

  /// check whether it is a file or dir
  /// currently, minidfs doesn't support remove a directory.
  if (dentries.find(dfid) != dentries.end()) {
    cerr << "[DFSMaster] "  << file << " is a directory!\n";
    return OpCode::OP_FAILURE;
  }

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

  /// log the edit to disk
  EditLog editlog;
  editlog.set_op(OpCode::OP_REMOVE);
  editlog.set_src(file);
  editlog.set_dfid(dirID);

  if (-1 == logEdit(editlog.SerializeAsString())) {
    cerr << "[DFSMaster] " << "Failed to remove " << file << std::endl;
    return OpCode::OP_LOG_FAILURE;
  }
  editlogID++;
  return OpCode::OP_SUCCESS;
}

int DFSMaster::exists(const string& file) {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys);

  if (dfIDs.find(file) == dfIDs.end()) {
    cerr << "[DFSMaster] "  << file << " doesn't exist!\n";
    return OpCode::OP_NOT_EXIST;
  }
  return OpCode::OP_EXIST;
}

int DFSMaster::makeDir(const string& dirName) {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys);

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

  /// log the edit to disk
  EditLog editlog;
  editlog.set_op(OpCode::OP_MKDIR);
  editlog.set_src(dirName);
  editlog.set_dfid(newDfID);
  if (-1 == logEdit(editlog.SerializeAsString())) {
    cerr << "[DFSMaster] " << "Failed to mkdir " << dirName << std::endl;
    return OpCode::OP_LOG_FAILURE;
  }
  editlogID++;
  return OpCode::OP_SUCCESS;
}

int DFSMaster::listDir(const string& dirName, FileInfos& items) {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys);

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
    name = name.substr(name.find_last_of('/')+1);
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
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock);
  //int id = getChunkserverID(chunkserverInfo);

  aliveChunkservers[chunkserverInfo] = true;
  /// TODO: xiw, is returning value here useless?
  return OpCode::OP_SUCCESS;
}

int DFSMaster::blkReport(const ChunkserverInfo& chunkserverInfo, const std::vector<int>& blkIDs, std::vector<int>& deletedBlks) {
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::lock(lockMemoryNameSys, lockChunkserverBlock);

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
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock);
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
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::lock(lockMemoryNameSys, lockChunkserverBlock);

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
  std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

  std::lock(lockFileNameSys, lockMemoryNameSys);

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
      continue;
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
  std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

  std::lock(lockFileNameSys, lockMemoryNameSys);

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
    dentries[dentry.id()].clear();
    /// dentries
    for (int j = 0; j < dentry.subdentries_size(); ++j) {
      dentries[dentry.id()].push_back(dentry.subdentries(j));
    }
  }

  fs.clear();
  fs.close();
  cerr << "[DFSMaster] "  << "Succeed to parse the name system from file: " << nameSysFile << std::endl;
  return 0;
}

int DFSMaster::initMater() {
  std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);

  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock, std::defer_lock);
  std::lock(lockFileNameSys, lockMemoryNameSys, lockChunkserverBlock);

  /// chunkserver
  blkLocs.clear();
  //chunkservers = std::unordered_map<int, ChunkserverInfo>();
  //chunkserverIDs = std::unordered_map<ChunkserverInfo, int>();
  //currentMaxChunkserverID = 0;
  aliveChunkservers.clear();

  blksToBeReplicated.clear();
  
  /// name system
  if(-1 == parseNameSystem()){
    return -1;
  }
  /// replay the edit log
  if (-1 == replayEditLog()){
    return -1;
  }
  return 0;
}

void DFSMaster::splitPath(const string& path, string& dir) {
  if(path == "/") {
    dir = "";
    return;
  }

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
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock);

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
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock);

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
  std::unique_lock<std::recursive_mutex> lockChunkserverBlock(mutexChunkserverBlock);

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
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys);
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

int DFSMaster::logEdit(const string& editString) {
  cerr << "[DFSMaster] " << editString << std::endl;
  std::unique_lock<std::recursive_mutex> lockEdit(mutexFileNameSys);
  int fd = ::open(editLogFile.c_str(), O_WRONLY | O_APPEND);
  if (fd < 0) {
    cerr << "[DFSMaster] " << "Failed to open editlog file\n";
    return -1;
  }
  {
    google::protobuf::io::FileOutputStream outRaw(fd);
    google::protobuf::io::CodedOutputStream outCoded(&outRaw);

    outCoded.WriteLittleEndian32(editlogMagicCode);
    outCoded.WriteVarint32(editString.size());
    outCoded.WriteString(editString);
  }
  
  ::close(fd);
  cerr << "[DFSMaster] " << "Succeeded to append to editlog file\n";
  return 0;
}

int DFSMaster::replayEditLog(){
  std::unique_lock<std::recursive_mutex> lockFileNameSys(mutexFileNameSys, std::defer_lock);
  std::unique_lock<std::recursive_mutex> lockMemoryNameSys(mutexMemoryNameSys, std::defer_lock);
  std::lock(lockFileNameSys, lockMemoryNameSys);

  int fd = ::open(editLogFile.c_str(), O_RDONLY);
  if (fd < 0) {
    cerr << "[DFSMaster] " << "Failed to open editlog file\n";
    return -1;
  }
  {
    google::protobuf::io::FileInputStream inRaw(fd);
    google::protobuf::io::CodedInputStream inCoded(&inRaw);

    u_int32_t magic = 0;
    editlogID = 0;
    uint len = 0;
    while (true == inCoded.ReadLittleEndian32(&magic) && magic == editlogMagicCode) {
      if (false == inCoded.ReadVarint32(&len)){
        cerr << "[DFSMaster] " << "Failed to read editlog file\n";
        ::close(fd);
        return -1;
      }
      std::vector<char> buf(len, 0);
      if (false == inCoded.ReadRaw(buf.data(), len)) {
        cerr << "[DFSMaster] " << "Failed to read editlog file\n";
        ::close(fd);
        return -1;
      }
      EditLog editlog;
      editlog.ParseFromArray(buf.data(), len);
      if (editlog.op() == OpCode::OP_CREATE) {
        int newDfID = editlog.dfid();
        if (currentMaxDfID < newDfID){
          currentMaxDfID = newDfID;
        }
        string file = editlog.src();
        
        /// add an entry to dfIDs
        dfIDs[file] = newDfID;
        /// add it dfNames at the same time
        dfNames[newDfID] = file;

        /// add it to dentries
        string dir;
        splitPath(file, dir);
        int dirID = dfIDs[dir];
        dentries[dirID].push_back(newDfID);

        /// add it to inode
        for (int i = 0; i < editlog.blks_size(); ++i) {
          int newBlkID = editlog.blks(i).blockid();
          if (currentMaxBlkID < newBlkID) {
            currentMaxBlkID = newBlkID;
          }
          inodes[newDfID].push_back(newBlkID);
          blks[newBlkID] = editlog.blks(i);
        }
      } else if (editlog.op() == OpCode::OP_MKDIR) {
        string dirName = editlog.src();
        string dir;
        splitPath(dirName, dir);

        /// assign dfID
        int newDfID = editlog.dfid();
        if (currentMaxDfID < newDfID){
          currentMaxDfID = newDfID;
        }
        /// add it to dfIDs
        dfIDs[dirName] = newDfID;
        /// add it to dfNames
        dfNames[newDfID] = dirName;

        /// add it to dentries
        int dirID = dfIDs[dir];
        dentries[dirID].push_back(newDfID);
        dentries[newDfID] = std::vector<int>();
      } else if (editlog.op() == OpCode::OP_REMOVE) {
        string file = editlog.src();

        /// assign dfID
        int newDfID = editlog.dfid();
        if (currentMaxDfID < newDfID){
          currentMaxDfID = newDfID;
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
      } else{
        cerr << "[DFSMaster] " << "Invalid opcode in editlog file\n";
        ::close(fd);
        return -1;
      }
      editlogID++;
    }
  }
  
  ::close(fd);
  return 0;
}

} // namespace minidfs
