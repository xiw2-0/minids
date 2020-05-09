/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Header file for operation code class which contains all the operation codes.


#ifndef OP_CODE_H_
#define OP_CODE_H_

namespace minidfs {

class OpCode {
 public:
  /// Generally indicate operation success.
  static constexpr char OP_SUCCESS = 0;

  /// Generally indicate operation failed.
  static constexpr char OP_FAILURE = 1;

  /////////////////////////////////
  /// Op code for client protocol
  /////////////////////////////////

  static constexpr char OP_NO_SUCH_FILE = 20;
  static constexpr char OP_FILE_ALREADY_EXISTED = 21;
  static constexpr char OP_FILE_IN_CREATING = 22;
  static constexpr char OP_SAFE_MODE = 30;

  /////////////////////////////////
  /// Op code for chunkserver protocol
  /////////////////////////////////

  static constexpr char OP_COPY = 40;
  static constexpr char OP_NO_BLK_TASK = 41;

  /////////////////////////////////
  /// Op code for data transfer
  /////////////////////////////////
  
  static constexpr char OP_READ = 60;
  static constexpr char OP_WRITE = 61;

};


} // namespace minidfs










#endif
