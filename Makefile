SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin
TEST_DIR = ./test

protosrc = ${wildcard ${SRC_DIR}/proto/*.proto}

cppsrc = ${wildcard ${SRC_DIR}/minidfs/*.cpp} \
				 ${wildcard ${SRC_DIR}/rpc/*.cpp} \
				 ${wildcard ${SRC_DIR}/threadpool/*.cpp}
ccsrc = ${wildcard ${SRC_DIR}/proto/*.cc} \
				${wildcard ${SRC_DIR}/logging/*.cc}
				


cppobj = ${patsubst ${SRC_DIR}%, ${BUILD_DIR}%, ${cppsrc:.cpp=.o}}
ccobj = ${patsubst ${SRC_DIR}%, ${BUILD_DIR}%, ${ccsrc:.cc=.o}}
mainobj = %fs_shell.o %/master.o %/chunkserver.o %test_main.o
obj = ${filter-out ${mainobj}, ${cppobj} ${ccobj}}


DFS_SHELL = dfs_shell
dfs_shell_obj = ${BUILD_DIR}/minidfs/fs_shell.o
BIN_DFS_SHELL = ${BIN_DIR}/${DFS_SHELL}

MASTER = master
master_obj = ${BUILD_DIR}/minidfs/master.o
BIN_MASTER = ${BIN_DIR}/${MASTER}

CHUNKSERVER = chunkserver
chunkserver_obj = ${BUILD_DIR}/minidfs/chunkserver.o
BIN_CHUNKSERVER = ${BIN_DIR}/${CHUNKSERVER}

TEST = ${BUILD_DIR}/unit_test
test_obj = ${BUILD_DIR}/test/test_main.o


INC_DIR = -I${SRC_DIR} -I${SRC_DIR}/proto
CCFLAGS = ${INC_DIR} -std=c++11 -g
LDFLAGS = `pkg-config --cflags --libs protobuf` -lpthread


all: ${BIN_MASTER} ${BIN_DFS_SHELL} ${BIN_CHUNKSERVER} ${TEST}
.PHONY: all

${BIN_DFS_SHELL}: ${dfs_shell_obj} $(obj)
	$(CXX) $^ -o $@  $(LDFLAGS)

${BIN_MASTER}: ${master_obj} $(obj)
	$(CXX) $^ -o $@  $(LDFLAGS)

${BIN_CHUNKSERVER}: ${chunkserver_obj} $(obj)
	$(CXX) $^ -o $@  $(LDFLAGS)

${TEST}: ${test_obj} ${obj}
	$(CXX) $^ -o $@  $(LDFLAGS)

${BUILD_DIR}/minidfs/%.o: ${SRC_DIR}/minidfs/%.cpp
	$(CXX) -c $< -o $@ ${CCFLAGS}

${BUILD_DIR}/rpc/%.o: ${SRC_DIR}/rpc/%.cpp
	$(CXX) -c $< -o $@ ${CCFLAGS}

${BUILD_DIR}/threadpool/%.o: ${SRC_DIR}/threadpool/%.cpp
	$(CXX) -c $< -o $@ ${CCFLAGS}

${BUILD_DIR}/%.o: ${SRC_DIR}/%.cc
	$(CXX) -c $< -o $@ ${CCFLAGS}

${BUILD_DIR}/test/%.o: ${TEST_DIR}/%.cc
	$(CXX) -c $< -o $@ ${CCFLAGS}

.PHONY: clean
clean:
	rm -f $(obj) ${dfs_shell_obj} ${master_obj} ${chunkserver_obj} \
				${BIN_DFS_SHELL} ${BIN_MASTER} ${BIN_CHUNKSERVER}

.PHONY: proto
proto: ${protosrc}
	protoc -I=${SRC_DIR}/proto --cpp_out=${SRC_DIR}/proto $^