CXX ?= clang++
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -pthread -Iinclude
SRC_DIR = src
INC_DIR = include
TEST_DIR = tests
OBJ_DIR = obj
BIN_DIR = bin

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(OBJ_DIR)/%.o, $(SRCS))

TARGET_LIB = $(BIN_DIR)/libp2p_core.a
TARGET_TEST = $(BIN_DIR)/test_socket_comm
TARGET_SERVER = $(BIN_DIR)/peer_server
TARGET_CLIENT = $(BIN_DIR)/peer_client

.PHONY: all clean test

all: $(TARGET_LIB) $(TARGET_TEST) $(TARGET_SERVER) $(TARGET_CLIENT)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp | $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TARGET_LIB): $(OBJS) | $(BIN_DIR)
	ar rcs $@ $^

$(TARGET_TEST): $(TEST_DIR)/test_socket_comm.cpp $(TARGET_LIB) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $< -L$(BIN_DIR) -lp2p_core -o $@

$(TARGET_SERVER): $(TEST_DIR)/peer_server_cli.cpp $(TARGET_LIB) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $< -L$(BIN_DIR) -lp2p_core -o $@

$(TARGET_CLIENT): $(TEST_DIR)/peer_client_cli.cpp $(TARGET_LIB) | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $< -L$(BIN_DIR) -lp2p_core -o $@

test: $(TARGET_TEST)
	@echo "Running Bare-Metal C++ Raw Socket-to-Socket Tests..."
	@./$(TARGET_TEST)

clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)
