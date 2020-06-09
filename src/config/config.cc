/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi

#include "config/config.h"
#include <fcntl.h>

namespace config {


Config::Config(const char* file)
    : Config(file, '=', '#') {
}

Config::Config(const char* file, char delimiter, char comment)
    : file_(file), delimiter_(delimiter), comment_(comment) {
}

bool Config::parse() {
  auto f = ::fopen(file_.c_str(), "r");
  if (!f) {
    printf("Failed to open file: %s\n", file_.c_str());
    return false;
  }
  
  char buf[128] = {0};
  while( nullptr != ::fgets(buf, sizeof(buf), f)) {
    if(!parseLine(string(buf))) {
      ::fclose(f);
      return false;
    }
  }
  ::fclose(f);
  return true;
}



bool Config::parseLine(const string& line) {
  // remove comments
  auto comment_index = line.find_first_of(comment_);
  auto line_ = line.substr(0, comment_index);

  // remove spaces
  removeWhiteSpace(&line_);

  if (line_.empty() || line_[0] == comment_) {
    return true;
  }
  

  auto delim_index = line_.find_first_of(delimiter_);
  if (delim_index == string::npos) {
    return false;
  }

  

  string key(line_.substr(0, delim_index));
  removeWhiteSpace(&key);

  string value(line_.substr(delim_index + 1));
  removeWhiteSpace(&value);

  // put it into map
  configurations_[key] = value;
  return true;
}

void Config::removeWhiteSpace(string* str) const{
  char null_characters[] = " \n\r\t\v\f";
  // remove null characters at the begining
  str->erase(0, str->find_first_not_of(null_characters));

  // remove null char at the end
  str->erase(str->find_last_not_of(null_characters) + 1);
}

} // namespace config



