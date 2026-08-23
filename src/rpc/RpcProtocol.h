#ifndef AETHER_RPC_PROTOCOL_H
#define AETHER_RPC_PROTOCOL_H
#pragma once

#include <cstddef>
#include <cstdint>

namespace aether {
namespace rpc {

// Wire format of one AetherRPC frame (all fields little-endian on the wire):
//
//   +--------+---------+---------+------------+---------+---------------+
//   | magic  | version | msgType | requestId  | bodyLen |   body (pb)   |
//   |  2B    |   1B    |   1B    |    4B      |   4B    |   bodyLen B   |
//   +--------+---------+---------+------------+---------+---------------+
//
// The 12-byte fixed header is followed by a protobuf-serialized RpcRequest
// (for REQUEST frames) or RpcResponse (for RESPONSE frames).

constexpr uint16_t kMagic   = 0xAE50;  // "AE" + "P": AetherRPC protocol
constexpr uint8_t  kVersion = 1;

enum MsgType : uint8_t {
    kRequest   = 1,
    kResponse  = 2,
    kHeartbeat = 3,  // reserved for connection keepalive (Phase 3)
};

// Error codes carried by RpcResponse.error_code.
enum RpcErrorCode : int32_t {
    kOk               = 0,
    kServiceNotFound  = 1,
    kMethodNotFound   = 2,
    kParseError       = 3,   // request/response body failed to parse
    kTimeout          = 4,   // caller-side timeout (Phase 3)
    kConnectionClosed = 5,
    kInternalError    = 6,
};

struct RpcHeader {
    uint16_t magic;
    uint8_t  version;
    uint8_t  msgType;
    uint32_t requestId;
    uint32_t bodyLen;
};

constexpr size_t kHeaderLen = sizeof(uint16_t) + sizeof(uint8_t) * 2 +
                              sizeof(uint32_t) * 2;  // == 12

// Sanity limit: a frame larger than this is treated as a protocol violation.
constexpr uint32_t kMaxBodyLen = 64u * 1024 * 1024;  // 64 MB

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_PROTOCOL_H
