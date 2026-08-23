// Unit tests for the AetherRPC wire protocol codec.
// Tests: header encode/decode round-trip, frame layout, half packages
// (header/body), sticky packages, protocol violations.
#include <cstring>
#include <string>

#include "net/Buffer.h"
#include "rpc/RpcCodec.h"
#include "rpc/RpcProtocol.h"
#include "test_framework.h"

using namespace aether::rpc;

void test_header_roundtrip() {
    TEST_SUITE("Header Encode/Decode Round-trip");
    RpcHeader in;
    in.magic = kMagic;
    in.version = kVersion;
    in.msgType = kRequest;
    in.requestId = 0xDEADBEEF;
    in.bodyLen = 0x01020304;

    char wire[kHeaderLen];
    RpcCodec::encodeHeader(wire, in);
    RpcHeader out = RpcCodec::decodeHeader(wire);

    ASSERT_EQ(out.magic, in.magic);
    ASSERT_EQ(out.version, in.version);
    ASSERT_EQ(out.msgType, in.msgType);
    ASSERT_EQ(out.requestId, in.requestId);
    ASSERT_EQ(out.bodyLen, in.bodyLen);
}

void test_header_is_little_endian() {
    TEST_SUITE("Wire Format Is Little-endian");
    RpcHeader in;
    in.magic = kMagic;
    in.version = kVersion;
    in.msgType = kResponse;
    in.requestId = 0x0102;
    in.bodyLen = 0x03;

    char wire[kHeaderLen];
    RpcCodec::encodeHeader(wire, in);
    // 0x0102 little-endian -> bytes 02 01
    ASSERT_EQ(static_cast<uint8_t>(wire[4]), 0x02);
    ASSERT_EQ(static_cast<uint8_t>(wire[5]), 0x01);
    ASSERT_EQ(static_cast<uint8_t>(wire[8]), 0x03);
}

void test_append_frame_layout() {
    TEST_SUITE("appendFrame Layout");
    std::string body = "protobuf-payload";
    std::string frame;
    RpcCodec::appendFrame(&frame, kRequest, 42, body);

    ASSERT_EQ(frame.size(), kHeaderLen + body.size());
    RpcHeader h = RpcCodec::decodeHeader(frame.data());
    ASSERT_EQ(h.magic, kMagic);
    ASSERT_EQ(h.version, kVersion);
    ASSERT_EQ(h.msgType, static_cast<uint8_t>(kRequest));
    ASSERT_EQ(h.requestId, 42u);
    ASSERT_EQ(h.bodyLen, static_cast<uint32_t>(body.size()));
    ASSERT_STREQ(frame.substr(kHeaderLen), body);
}

void test_parse_complete_frame() {
    TEST_SUITE("Parse Complete Frame");
    std::string body(64, 'x');
    std::string frame;
    RpcCodec::appendFrame(&frame, kResponse, 7, body);

    Buffer buf;
    buf.append(frame);

    RpcHeader h;
    std::string got;
    bool err = false;
    ASSERT_TRUE(RpcCodec::tryParse(&buf, &h, &got, &err));
    ASSERT_FALSE(err);
    ASSERT_EQ(h.requestId, 7u);
    ASSERT_EQ(h.msgType, static_cast<uint8_t>(kResponse));
    ASSERT_STREQ(got, body);
    ASSERT_EQ(buf.readableBytes(), 0u);  // frame fully consumed
}

void test_half_package() {
    TEST_SUITE("Half Package");
    std::string body(32, 'y');
    std::string frame;
    RpcCodec::appendFrame(&frame, kRequest, 99, body);

    // feed only part of the header
    {
        Buffer buf;
        buf.append(frame.data(), 5);
        RpcHeader h;
        std::string got;
        bool err = false;
        ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &got, &err));
        ASSERT_FALSE(err);
    }
    // header complete, body partial
    {
        Buffer buf;
        buf.append(frame.data(), kHeaderLen + 10);
        RpcHeader h;
        std::string got;
        bool err = false;
        ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &got, &err));
        ASSERT_FALSE(err);
        ASSERT_EQ(buf.readableBytes(), kHeaderLen + 10);  // nothing consumed
    }
    // exactly one byte short, then complete
    {
        Buffer buf;
        buf.append(frame.data(), frame.size() - 1);
        RpcHeader h;
        std::string got;
        bool err = false;
        ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &got, &err));
        buf.append(std::string(1, frame.back()));
        ASSERT_TRUE(RpcCodec::tryParse(&buf, &h, &got, &err));
        ASSERT_STREQ(got, body);
    }
}

void test_sticky_package() {
    TEST_SUITE("Sticky Package");
    std::string f1, f2, f3;
    RpcCodec::appendFrame(&f1, kRequest, 1, std::string(10, 'a'));
    RpcCodec::appendFrame(&f2, kRequest, 2, std::string(20, 'b'));
    RpcCodec::appendFrame(&f3, kResponse, 3, std::string(30, 'c'));

    Buffer buf;
    buf.append(f1 + f2 + f3);

    RpcHeader h;
    std::string body;
    bool err = false;

    ASSERT_TRUE(RpcCodec::tryParse(&buf, &h, &body, &err));
    ASSERT_EQ(h.requestId, 1u);
    ASSERT_EQ(body.size(), 10u);

    ASSERT_TRUE(RpcCodec::tryParse(&buf, &h, &body, &err));
    ASSERT_EQ(h.requestId, 2u);
    ASSERT_EQ(body.size(), 20u);

    ASSERT_TRUE(RpcCodec::tryParse(&buf, &h, &body, &err));
    ASSERT_EQ(h.requestId, 3u);
    ASSERT_EQ(body.size(), 30u);

    ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &body, &err));  // drained
    ASSERT_EQ(buf.readableBytes(), 0u);
}

void test_protocol_violations() {
    TEST_SUITE("Protocol Violations");
    // bad magic
    {
        std::string body(8, 'z');
        std::string frame;
        RpcCodec::appendFrame(&frame, kRequest, 5, body);
        frame[0] = static_cast<char>(0xFF);  // corrupt magic

        Buffer buf;
        buf.append(frame);
        RpcHeader h;
        std::string got;
        bool err = false;
        ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &got, &err));
        ASSERT_TRUE(err);
    }
    // bad version
    {
        std::string body(8, 'z');
        std::string frame;
        RpcCodec::appendFrame(&frame, kRequest, 5, body);
        frame[2] = static_cast<char>(0x7F);  // corrupt version

        Buffer buf;
        buf.append(frame);
        RpcHeader h;
        std::string got;
        bool err = false;
        ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &got, &err));
        ASSERT_TRUE(err);
    }
    // oversized bodyLen
    {
        std::string body(8, 'z');
        std::string frame;
        RpcCodec::appendFrame(&frame, kRequest, 5, body);
        uint32_t huge = kMaxBodyLen + 1;
        memcpy(&frame[8], &huge, sizeof(huge));  // host order is fine here:
                                                 // x86 is little-endian
        Buffer buf;
        buf.append(frame);
        RpcHeader h;
        std::string got;
        bool err = false;
        ASSERT_FALSE(RpcCodec::tryParse(&buf, &h, &got, &err));
        ASSERT_TRUE(err);
    }
}

int main() {
    test_header_roundtrip();
    test_header_is_little_endian();
    test_append_frame_layout();
    test_parse_complete_frame();
    test_half_package();
    test_sticky_package();
    test_protocol_violations();

    printf("\n%d assertions, %d passed, %d failed\n", g_test_count,
           g_test_passed, g_test_failed);
    return g_test_failed == 0 ? 0 : 1;
}
