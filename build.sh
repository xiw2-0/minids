# make sure you have installed protobuf!
# if you have installed protobuf, uncomment the first part
# install protobuf
sudo apt-get install autoconf automake libtool curl make g++ unzip
cd ..
git clone https://github.com/google/protobuf.git
cd protobuf
git submodule update --init --recursive
./autogen.sh
./configure
make
make check
sudo make install
sudo ldconfig # refresh shared library cache.
# change workding directory to minidfs
cd ../minids


# build minidfs
mkdir -p build/config build/logging build/minidfs build/proto build/rpc build/threadpool
mkdir -p bin data/client data/chunkserver
make proto
make