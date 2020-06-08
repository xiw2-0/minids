/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief Test for Logger.


#include "logging/logger.h"

#include <cassert>
#include <cstring>


void TestLogger() {
  ::printf("Test Logger...\n");
  
  LOG_DEBUG << "nothing should be printed" << ' ' << 9;

  logging::Logger::set_log_level(logging::DEBUG);
  LOG_TRACE << "nothing should be printed" << " " << 9.9;
  LOG_DEBUG << "i am debug " << ' ' << -9;
  LOG_INFO << "i am info log " << 'a' << ' ' << 93;
  LOG_WARN << "i am warn log " << 9.93;
  LOG_ERROR << "i am error log " << false;


  ::printf("\n");
}