# Optional benchmark suite: AetherRPC vs gRPC unary echo. Built only
# when a gRPC dev installation is found (see the guard in the top-level
# CMakeLists); the core library and tests never depend on it.

find_package(Protobuf REQUIRED)

set(BENCH_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/gen)
file(MAKE_DIRECTORY ${BENCH_GEN_DIR})

set(GRPC_PROTO_SRCS ${BENCH_GEN_DIR}/bench_grpc.pb.cc)
set(GRPC_PROTO_HDRS ${BENCH_GEN_DIR}/bench_grpc.pb.h
                    ${BENCH_GEN_DIR}/bench_grpc.grpc.pb.cc
                    ${BENCH_GEN_DIR}/bench_grpc.grpc.pb.h)
list(APPEND GRPC_PROTO_SRCS ${BENCH_GEN_DIR}/bench_grpc.grpc.pb.cc)

add_custom_command(
    OUTPUT ${BENCH_GEN_DIR}/bench_grpc.pb.cc ${BENCH_GEN_DIR}/bench_grpc.pb.h
           ${BENCH_GEN_DIR}/bench_grpc.grpc.pb.cc
           ${BENCH_GEN_DIR}/bench_grpc.grpc.pb.h
    COMMAND protobuf::protoc
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --cpp_out=${BENCH_GEN_DIR}
            --grpc_out=${BENCH_GEN_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
            bench_grpc.proto
    DEPENDS bench_grpc.proto
    COMMENT "Generating gRPC benchmark sources"
    VERBATIM)

set(AETHER_PROTO_SRCS ${BENCH_GEN_DIR}/bench.pb.cc)
set(AETHER_PROTO_HDRS ${BENCH_GEN_DIR}/bench.pb.h)
add_custom_command(
    OUTPUT ${BENCH_GEN_DIR}/bench.pb.cc ${BENCH_GEN_DIR}/bench.pb.h
    COMMAND protobuf::protoc
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/../proto
            --cpp_out=${BENCH_GEN_DIR}
            bench.proto
    DEPENDS ../proto/bench.proto
    COMMENT "Generating AetherRPC benchmark sources"
    VERBATIM)

add_executable(bench_rpc bench_rpc.cpp
               ${GRPC_PROTO_SRCS} ${GRPC_PROTO_HDRS}
               ${AETHER_PROTO_SRCS} ${AETHER_PROTO_HDRS})
target_include_directories(bench_rpc PRIVATE ${BENCH_GEN_DIR})
target_link_libraries(bench_rpc
    PRIVATE aetherrpc
            protobuf::libprotobuf
            ${GRPCPP_LIB} ${GRPC_LIB})
