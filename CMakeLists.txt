cmake_minimum_required(VERSION 3.10)
project(AetherRPC CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# ZooKeeper C client (multi-threaded flavor) - Phase 2 dependency.
find_path(ZOOKEEPER_INCLUDE_DIR zookeeper/zookeeper.h REQUIRED)
find_library(ZOOKEEPER_MT_LIB NAMES zookeeper_mt REQUIRED)
message(STATUS "ZooKeeper client: ${ZOOKEEPER_MT_LIB}")

# ---------------------------------------------------------------------------
# protobuf code generation
# ---------------------------------------------------------------------------
set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

set(PROTO_SRCS ${PROTO_GEN_DIR}/rpc_common.pb.cc)
set(PROTO_HDRS ${PROTO_GEN_DIR}/rpc_common.pb.h
               ${PROTO_GEN_DIR}/echo.pb.cc
               ${PROTO_GEN_DIR}/echo.pb.h)
list(APPEND PROTO_SRCS ${PROTO_GEN_DIR}/echo.pb.cc)

add_custom_command(
    OUTPUT ${PROTO_GEN_DIR}/rpc_common.pb.cc ${PROTO_GEN_DIR}/rpc_common.pb.h
           ${PROTO_GEN_DIR}/echo.pb.cc ${PROTO_GEN_DIR}/echo.pb.h
    COMMAND protobuf::protoc
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
            --cpp_out=${PROTO_GEN_DIR}
            rpc_common.proto echo.proto
    DEPENDS proto/rpc_common.proto proto/echo.proto
    COMMENT "Generating protobuf sources"
    VERBATIM)

# ---------------------------------------------------------------------------
# aetherrpc core library: Aether net/base kernel + rpc layer + protos
# ---------------------------------------------------------------------------
file(GLOB BASE_SRCS src/base/*.cpp)
file(GLOB NET_SRCS src/net/*.cpp)
file(GLOB RPC_SRCS src/rpc/*.cpp)
file(GLOB ZK_SRCS src/zk/*.cpp)

add_library(aetherrpc STATIC
    ${BASE_SRCS}
    ${NET_SRCS}
    ${RPC_SRCS}
    ${ZK_SRCS}
    ${PROTO_SRCS}
    ${PROTO_HDRS})

target_include_directories(aetherrpc
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
           ${PROTO_GEN_DIR})
target_link_libraries(aetherrpc
    PUBLIC protobuf::libprotobuf Threads::Threads
            ${ZOOKEEPER_MT_LIB})
target_include_directories(aetherrpc PUBLIC ${ZOOKEEPER_INCLUDE_DIR})

# ---------------------------------------------------------------------------
# examples
# ---------------------------------------------------------------------------
add_executable(echo_server examples/echo_server.cpp)
target_link_libraries(echo_server PRIVATE aetherrpc)

add_executable(echo_client examples/echo_client.cpp)
target_link_libraries(echo_client PRIVATE aetherrpc)

# Phase 2: ZooKeeper-backed distributed examples.
add_executable(dist_echo_server examples/dist_echo_server.cpp)
target_link_libraries(dist_echo_server PRIVATE aetherrpc)

add_executable(dist_echo_client examples/dist_echo_client.cpp)
target_link_libraries(dist_echo_client PRIVATE aetherrpc)

# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------
enable_testing()
add_subdirectory(test)

# ---------------------------------------------------------------------------
# optional benchmark suite (AetherRPC vs gRPC); skipped silently when no
# gRPC dev installation is present - the library and tests never need it
# ---------------------------------------------------------------------------
find_path(GRPCPP_INCLUDE_DIR grpcpp/grpcpp.h)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
find_library(GRPCPP_LIB grpc++)
find_library(GRPC_LIB grpc)
if(GRPCPP_INCLUDE_DIR AND GRPC_CPP_PLUGIN_EXECUTABLE AND GRPCPP_LIB AND GRPC_LIB)
    message(STATUS "gRPC found: benchmark suite enabled")
    add_subdirectory(bench)
else()
    message(STATUS "gRPC not found: benchmark suite skipped")
endif()
