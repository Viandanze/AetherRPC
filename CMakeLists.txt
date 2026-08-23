cmake_minimum_required(VERSION 3.10)
project(AetherRPC CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

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

add_library(aetherrpc STATIC
    ${BASE_SRCS}
    ${NET_SRCS}
    ${RPC_SRCS}
    ${PROTO_SRCS}
    ${PROTO_HDRS})

target_include_directories(aetherrpc
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
           ${PROTO_GEN_DIR})
target_link_libraries(aetherrpc
    PUBLIC protobuf::libprotobuf Threads::Threads)

# ---------------------------------------------------------------------------
# examples
# ---------------------------------------------------------------------------
add_executable(echo_server examples/echo_server.cpp)
target_link_libraries(echo_server PRIVATE aetherrpc)

add_executable(echo_client examples/echo_client.cpp)
target_link_libraries(echo_client PRIVATE aetherrpc)

# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------
enable_testing()
add_subdirectory(test)
