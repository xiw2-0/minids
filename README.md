# minids
A mini distributed file system. Inspired by Hadoop and GFS. 


## How to build minidfs?

Make sure you have installed [protobuf](https://github.com/protocolbuffers/protobuf)!

### install protobuf

If you haven't installed yet, use the following commands to install. Or you can refer to the official repo to install [protobuf](https://github.com/protocolbuffers/protobuf).

```
# change working directory to appropriate path
sudo apt-get install autoconf automake libtool curl make g++ unzip
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.12.3/protobuf-cpp-3.12.3.zip
unzip protobuf-cpp-3.12.3.zip
cd protobuf*
./configure --prefix=/usr
make
#make check
sudo make install
sudo ldconfig # refresh shared library cache.
```

### build minidfs

Use the following commands to build. Just one line. And the command lines in build.sh is very simple too. 

```
./build.sh
```

## How to use minidfs?

Before running the master, chunkserver and dfs_shell, revise configurations in ./config
You need to configure at least the master IP address/port, chunkserver IP/port and replication factor. Other configurations are self-explainable by their names.

### pseudo-cluster

It's just fine to use the default configuration.
```
# format master first
./bin/master -format
# run master
./bin/master
# run a chunkserver
./bin/chunkserver
```
Make a new folder:
```
./bin/dfs_shell -mkdir /doc
```
Put a file in local file system to mini-dfs:
```
./bin/dfs_shell -put ./up.txt /doc/up.txt
```
List a folder:
```
./bin/dfs_shell -ls /doc
```
Get a file from mini-dfs to local file system:
```
./bin/dfs_shell -get /doc/up.txt down.txt
```
Judge whether a file exists:
```
./bin/dfs_shell -exists /doc/unknown.txt
```
