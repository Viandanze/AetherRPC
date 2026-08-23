// Unit tests for Buffer class
// Tests: construction, append, retrieve, prepend, swap, shrink, findCRLF
#include "test_framework.h"
#include "net/Buffer.h"

void test_buffer_construction() {
    TEST_SUITE("Buffer Construction");
    Buffer buf;
    ASSERT_EQ(buf.readableBytes(), 0u);
    ASSERT_EQ(buf.writableBytes(), Buffer::kInitialSize);
    ASSERT_EQ(buf.prependableBytes(), Buffer::kCheapPrepend);
}

void test_buffer_append_retrieve() {
    TEST_SUITE("Buffer Append & Retrieve");
    Buffer buf;
    std::string data = "Hello, Aether!";
    buf.append(data);
    ASSERT_EQ(buf.readableBytes(), data.size());
    ASSERT_EQ(buf.writableBytes(), Buffer::kInitialSize - data.size());

    std::string retrieved = buf.retrieveAsString(5);
    ASSERT_STREQ(retrieved, "Hello");
    ASSERT_EQ(buf.readableBytes(), data.size() - 5);

    std::string rest = buf.retrieveAllAsString();
    ASSERT_STREQ(rest, ", Aether!");
    ASSERT_EQ(buf.readableBytes(), 0u);
}

void test_buffer_auto_grow() {
    TEST_SUITE("Buffer Auto-Grow");
    Buffer buf(100);  // 100 bytes initial
    std::string big(250, 'X');
    buf.append(big);
    ASSERT_EQ(buf.readableBytes(), big.size());
}

void test_buffer_prepend() {
    TEST_SUITE("Buffer Prepend");
    Buffer buf;
    buf.append("world");
    int32_t len = 5;
    buf.prependInt32(len);
    ASSERT_EQ(buf.readableBytes(), 4u + 5u);
    // Read back the int32
    int32_t recovered;
    memcpy(&recovered, buf.peek(), 4);
    ASSERT_EQ(ntohl(recovered), 5);
    buf.retrieve(4);
    std::string body = buf.retrieveAllAsString();
    ASSERT_STREQ(body, "world");
}

void test_buffer_find_crlf() {
    TEST_SUITE("Buffer findCRLF");
    Buffer buf;
    buf.append("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    const char* crlf = buf.findCRLF();
    ASSERT_TRUE(crlf != nullptr);
    ASSERT_EQ(crlf - buf.peek(), 14);  // "GET / HTTP/1.1" is 14 chars

    // Find next CRLF after first
    const char* second = buf.findCRLF(crlf + 2);
    ASSERT_TRUE(second != nullptr);
    ASSERT_EQ(second - buf.peek(), 31);  // after "Host: localhost" (16+15=31)
}

void test_buffer_swap() {
    TEST_SUITE("Buffer Swap");
    Buffer a;
    Buffer b(200);
    a.append("AAA");
    b.append("BBBB");
    size_t a_readable = a.readableBytes();
    size_t b_readable = b.readableBytes();
    a.swap(b);
    ASSERT_EQ(a.readableBytes(), b_readable);
    ASSERT_EQ(b.readableBytes(), a_readable);
    ASSERT_STREQ(a.retrieveAllAsString(), "BBBB");
    ASSERT_STREQ(b.retrieveAllAsString(), "AAA");
}

void test_buffer_retrieve_all() {
    TEST_SUITE("Buffer RetrieveAll");
    Buffer buf;
    buf.append("test data");
    buf.retrieveAll();
    ASSERT_EQ(buf.readableBytes(), 0u);
    ASSERT_EQ(buf.prependableBytes(), Buffer::kCheapPrepend);
}

void test_buffer_shrink() {
    TEST_SUITE("Buffer Shrink");
    Buffer buf(1000);
    buf.append("small");
    buf.shrink();
    ASSERT_EQ(buf.readableBytes(), 5u);
    ASSERT_STREQ(buf.retrieveAllAsString(), "small");
}

void test_buffer_unwrite() {
    TEST_SUITE("Buffer Unwrite");
    Buffer buf;
    buf.append("hello world");
    buf.unwrite(6);  // Remove " world"
    ASSERT_EQ(buf.readableBytes(), 5u);
    ASSERT_STREQ(buf.retrieveAllAsString(), "hello");
}

void test_buffer_retrieve_until() {
    TEST_SUITE("Buffer RetrieveUntil");
    Buffer buf;
    buf.append("GET /path HTTP/1.1\r\nrest");
    const char* crlf = buf.findCRLF();
    ASSERT_TRUE(crlf != nullptr);
    buf.retrieveUntil(crlf);
    ASSERT_EQ(buf.readableBytes(), 6u);  // "\r\nrest" but we retrieved up to (not including) crlf
    // Actually retrieveUntil retrieves up to 'end' pointer
    // After retrieveUntil(crlf), readable = from crlf to end = "\r\nrest"
    ASSERT_STREQ(buf.retrieveAllAsString(), "\r\nrest");
}

int main() {
    test_buffer_construction();
    test_buffer_append_retrieve();
    test_buffer_auto_grow();
    test_buffer_prepend();
    test_buffer_find_crlf();
    test_buffer_swap();
    test_buffer_retrieve_all();
    test_buffer_shrink();
    test_buffer_unwrite();
    test_buffer_retrieve_until();
    TEST_SUMMARY();
}
