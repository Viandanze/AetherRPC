# Unit tests. Each suite is a standalone binary using the header-only
# test framework (no external dependency), mirroring the Aether layout.

# --- network kernel regression (inherited from Aether) ---------------------
add_executable(test_buffer test_buffer.cpp)
target_link_libraries(test_buffer PRIVATE aetherrpc)

add_executable(test_inet_address test_inet_address.cpp)
target_link_libraries(test_inet_address PRIVATE aetherrpc)

add_executable(test_timestamp test_timestamp.cpp)
target_link_libraries(test_timestamp PRIVATE aetherrpc)

# --- rpc layer --------------------------------------------------------------
add_executable(test_rpc_codec test_rpc_codec.cpp)
target_link_libraries(test_rpc_codec PRIVATE aetherrpc)

add_executable(test_rpc_e2e test_rpc_e2e.cpp)
target_link_libraries(test_rpc_e2e PRIVATE aetherrpc)

add_executable(test_load_balancer test_load_balancer.cpp)
target_link_libraries(test_load_balancer PRIVATE aetherrpc)

add_executable(test_zk_dist test_zk_dist.cpp)
target_link_libraries(test_zk_dist PRIVATE aetherrpc)

foreach(t
        test_buffer
        test_inet_address
        test_timestamp
        test_rpc_codec
        test_rpc_e2e
        test_load_balancer
        test_zk_dist)
    add_test(NAME ${t} COMMAND ${t})
endforeach()
