#include "rpc/RpcCodec.h"

#include <cstring>

namespace aether {
namespace rpc {

void RpcCodec::encodeHeader(char* dst, const RpcHeader& h) {
    dst[0] = static_cast<char>(h.magic & 0xFF);
    dst[1] = static_cast<char>((h.magic >> 8) & 0xFF);
    dst[2] = static_cast<char>(h.version);
    dst[3] = static_cast<char>(h.msgType);
    dst[4] = static_cast<char>(h.requestId & 0xFF);
    dst[5] = static_cast<char>((h.requestId >> 8) & 0xFF);
    dst[6] = static_cast<char>((h.requestId >> 16) & 0xFF);
    dst[7] = static_cast<char>((h.requestId >> 24) & 0xFF);
    dst[8] = static_cast<char>(h.bodyLen & 0xFF);
    dst[9] = static_cast<char>((h.bodyLen >> 8) & 0xFF);
    dst[10] = static_cast<char>((h.bodyLen >> 16) & 0xFF);
    dst[11] = static_cast<char>((h.bodyLen >> 24) & 0xFF);
}

RpcHeader RpcCodec::decodeHeader(const char* src) {
    RpcHeader h;
    h.magic = static_cast<uint16_t>(
        static_cast<uint8_t>(src[0]) |
        (static_cast<uint32_t>(static_cast<uint8_t>(src[1])) << 8));
    h.version = static_cast<uint8_t>(src[2]);
    h.msgType = static_cast<uint8_t>(src[3]);
    h.requestId = static_cast<uint32_t>(static_cast<uint8_t>(src[4])) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(src[5])) << 8) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(src[6])) << 16) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(src[7])) << 24);
    h.bodyLen = static_cast<uint32_t>(static_cast<uint8_t>(src[8])) |
                (static_cast<uint32_t>(static_cast<uint8_t>(src[9])) << 8) |
                (static_cast<uint32_t>(static_cast<uint8_t>(src[10])) << 16) |
                (static_cast<uint32_t>(static_cast<uint8_t>(src[11])) << 24);
    return h;
}

void RpcCodec::appendFrame(std::string* out, MsgType type, uint32_t requestId,
                           const std::string& body) {
    RpcHeader h;
    h.magic = kMagic;
    h.version = kVersion;
    h.msgType = static_cast<uint8_t>(type);
    h.requestId = requestId;
    h.bodyLen = static_cast<uint32_t>(body.size());

    char header[kHeaderLen];
    encodeHeader(header, h);
    out->append(header, kHeaderLen);
    out->append(body);
}

bool RpcCodec::tryParse(Buffer* buf, RpcHeader* header, std::string* body,
                        bool* err) {
    *err = false;
    if (buf->readableBytes() < kHeaderLen) {
        return false;  // half package: header not complete yet
    }

    RpcHeader h = decodeHeader(buf->peek());
    if (h.magic != kMagic || h.version != kVersion ||
        h.bodyLen > kMaxBodyLen) {
        *err = true;  // protocol violation: caller should drop the connection
        return false;
    }
    if (buf->readableBytes() < kHeaderLen + h.bodyLen) {
        return false;  // half package: body not complete yet
    }

    buf->retrieve(kHeaderLen);
    *body = buf->retrieveAsString(h.bodyLen);
    *header = h;
    return true;
}

}  // namespace rpc
}  // namespace aether
