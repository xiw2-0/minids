/// Copyright (c) 2020 xiw
///
/// MIT License
/// \author Wang Xi
/// \brief A simple configuration class

#ifndef CONFIG_H_
#define CONFIG_H_

#include <string>
#include <map>
#include <sstream>

using std::string;



namespace config {



class Config {
 public:
  /// Construct the config class using default delimiter and comment symbol.
  ///
  /// \param file config file name
  Config(const char* file);

  /// Construct with specified delimiter and comment symbols.
  ///
  /// \param file config file name
  /// \param delimiter ':' or '=' is recommended
  /// \param comment '#' is recommended
  Config(const char* file, char delimiter, char comment);

  ~Config() = default;

  /// Parse the configuration file
  bool parse();

  /// Get the value of the coresponding key
  ///
  /// \param key key of configuration
  /// \param value returning value
  /// \return return true if key exists, otherwise false.
  template<typename T>
  bool get(const string& key, T* value) const {
    // no such a key
    if (configurations_.find(key) == configurations_.end()) {
      return false;
    }
    string v = configurations_.at(key);

    *value = string2T<T>(v);
    return true;
  }




 protected:
  /// Parse a line. If there are repeated keys, we only keep the last one.
  bool parseLine(const string& line);

  /// Translate from string to T
  template<typename T>
  static T string2T(const string& value);



 private:
  /// remove white space at the begining and the end.
  ///
  /// \param str string that needs to be trimmed
  void removeWhiteSpace(string* str) const;

  /// config file name
  const string file_;
  /// delimiter is used to separate key and value, default is =
  char delimiter_ = '=';
  /// default comment sign is #
  char comment_ = '#';

  /// a map is used to store the contents of configurations
  std::map<string, string> configurations_;

};

template<>
inline bool Config::string2T<bool>(const string& value){
  if (value == "false" || value == "False" || value == "0") {
    return false;
  }

  return true;
}

template<>
inline string Config::string2T<string>(const string& value){
  return value;
}

template<typename T>
T Config::string2T(const string& value){
  std::istringstream iss(value);
  T v;
  iss >> v;
  return v;
}




} // namespace config












#endif