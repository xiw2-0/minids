SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

cppsrc = ${wildcard ${SRC_DIR}/minidfs/*.cpp}
obj = ${patsubst %.cpp, ${BUILD_DIR}/%.o, ${notdir ${cppsrc}}}

TARGET = dfs_shell
BIN_TARGET = ${BIN_DIR}/${TARGET}

LDFLAGS =

${BIN_TARGET}: $(obj)
	$(CXX) $^ -o $@  $(LDFLAGS)

${BUILD_DIR}/%.o: ${SRC_DIR}/minidfs/%.cpp
	$(CXX) -c $< -o $@

.PHONY: clean
clean:
	rm -f $(obj) ${BIN_TARGET}