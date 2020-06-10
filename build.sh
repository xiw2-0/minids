# build minidfs
mkdir -p build/config build/logging build/minidfs build/proto build/rpc build/threadpool build/test
mkdir -p bin data/client data/chunkserver
make proto
make