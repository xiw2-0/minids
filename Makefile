SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

protosrc = ${wildcard ${SRC_DIR}/proto/*.proto}

cppsrc = ${wildcard ${SRC_DIR}/minidfs/*.cpp}
ccsrc = ${wildcard ${SRC_DIR}/proto/*.cc}

cppobj = ${patsubst ${SRC_DIR}%, ${BUILD_DIR}%, ${cppsrc:.cpp=.o}}
ccobj = ${patsubst ${SRC_DIR}%, ${BUILD_DIR}%, ${ccsrc:.cc=.o}}
mainobj = %fs_shell.o
obj = ${filter-out ${mainobj}, ${cppobj} ${ccobj}}


DFS_SHELL = dfs_shell
dfs_shell_obj = ${BUILD_DIR}/minidfs/fs_shell.o
BIN_DFS_SHELL = ${BIN_DIR}/${DFS_SHELL}

INC_DIR = -I${SRC_DIR} -I${SRC_DIR}/proto
CCFLAGS = ${INC_DIR}
LDFLAGS = `pkg-config --cflags --libs protobuf`


all: ${BIN_DFS_SHELL}
.PHONY: all

${BIN_DFS_SHELL}: ${dfs_shell_obj} $(obj)
	$(CXX) $^ -o $@  $(LDFLAGS)

${BUILD_DIR}/minidfs/%.o: ${SRC_DIR}/minidfs/%.cpp
	$(CXX) -c $< -o $@ ${CCFLAGS}

${BUILD_DIR}/proto/%.o: ${SRC_DIR}/proto/%.cc
	$(CXX) -c $< -o $@ ${CCFLAGS}

.PHONY: clean
clean:
	rm -f $(obj) ${BIN_DFS_SHELL}

.PHONY: proto
proto: ${protosrc}
	protoc -I=${SRC_DIR}/proto --cpp_out=${SRC_DIR}/proto $^