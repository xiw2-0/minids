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
  static const char OP_SUCCESS;

  /// Generally indicate operation failed.
  static const char OP_FAILURE;

  /////////////////////////////////
  /// Op code for client protocol
  /////////////////////////////////

  static const char OP_NO_SUCH_FILE;
  static const char OP_FILE_ALREADY_EXISTED;
  static const char OP_SAFE_MODE;

  /////////////////////////////////
  /// Op code for chunkserver protocol
  /////////////////////////////////

  static const char OP_COPY;
  
};

const char OpCode::OP_SUCCESS = 0;
const char OpCode::OP_FAILURE = 1;

const char OpCode::OP_NO_SUCH_FILE = 20;
const char OpCode::OP_FILE_ALREADY_EXISTED = 21;
const char OpCode::OP_SAFE_MODE = 30;

const char OpCode::OP_COPY = 40;

} // namespace minidfs










#endif
