#ifndef AETHER_RPC_CODEC_H
#define AETHER_RPC_CODEC_H
#pragma once

#include <string>

#include "net/Buffer.h"
#include "rpc/RpcProtocol.h"

namespace aether {
namespace rpc {

/// RpcCodec: encode/decode AetherRPC frames on top of the net::Buffer.
///
/// Encoding is header-only bookkeeping; decoding has to deal with TCP
/// stream semantics: a frame may arrive in pieces (half package) or
/// glued to its neighbours (sticky package), so tryParse() only consumes
/// a frame when it is fully buffered.
class RpcCodec {
public:
    /// Serialize `body` and append a complete frame to `out`.
    /// Both overloads keep the request id so responses can be matched.
    static void appendFrame(std::string* out, MsgType type, uint32_t requestId,
                            const std::string& body);

    /// Try to extract one complete frame from `buf`.
    /// Returns true and fills `header`/`body` when a full frame is buffered
    /// (the frame bytes are consumed from `buf`); returns false when more
    /// bytes are needed. Throws nothing; protocol violations are reported
    /// through `err` so the caller can decide to drop the connection.
    static bool tryParse(Buffer* buf, RpcHeader* header, std::string* body,
                         bool* err);

    /// Encode/decode helpers for the fixed 12-byte header (little-endian,
    /// host-endianness independent).
    static void encodeHeader(char* dst, const RpcHeader& h);
    static RpcHeader decodeHeader(const char* src);
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CODEC_H
