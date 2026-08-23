// Unit tests for InetAddress class
// Tests: construction (port only, loopback, IP+port), toIp, toPort, toIpPort
#include "test_framework.h"
#include "net/InetAddress.h"
#include <cstring>
#include <arpa/inet.h>

void test_inet_construction_port_only() {
    TEST_SUITE("InetAddress Port-Only (INADDR_ANY)");
    InetAddress addr(8080);
    ASSERT_STREQ(addr.toIp(), "0.0.0.0");
    ASSERT_EQ(addr.toPort(), 8080);
    ASSERT_STREQ(addr.toIpPort(), "0.0.0.0:8080");
}

void test_inet_construction_loopback() {
    TEST_SUITE("InetAddress Loopback");
    InetAddress addr(8080, true);
    ASSERT_STREQ(addr.toIp(), "127.0.0.1");
    ASSERT_EQ(addr.toPort(), 8080);
    ASSERT_STREQ(addr.toIpPort(), "127.0.0.1:8080");
}

void test_inet_construction_ip_port() {
    TEST_SUITE("InetAddress IP+Port");
    InetAddress addr("192.168.1.100", 3000);
    ASSERT_STREQ(addr.toIp(), "192.168.1.100");
    ASSERT_EQ(addr.toPort(), 3000);
    ASSERT_STREQ(addr.toIpPort(), "192.168.1.100:3000");
}

void test_inet_construction_localhost() {
    TEST_SUITE("InetAddress Localhost");
    InetAddress addr("127.0.0.1", 6379);
    ASSERT_STREQ(addr.toIp(), "127.0.0.1");
    ASSERT_EQ(addr.toPort(), 6379);
}

void test_inet_port_zero() {
    TEST_SUITE("InetAddress Port Zero");
    InetAddress addr(0);
    ASSERT_EQ(addr.toPort(), 0);
    ASSERT_STREQ(addr.toIp(), "0.0.0.0");
}

void test_inet_large_port() {
    TEST_SUITE("InetAddress Max Port");
    InetAddress addr(65535);
    ASSERT_EQ(addr.toPort(), 65535);
}

void test_inet_set_sockaddr() {
    TEST_SUITE("InetAddress setSockAddr");
    InetAddress addr(8080);
    struct sockaddr_in sa = addr.getSockAddr();
    ASSERT_EQ(sa.sin_family, AF_INET);
    ASSERT_EQ(ntohs(sa.sin_port), 8080);

    // Modify via setSockAddr
    struct sockaddr_in new_sa;
    memset(&new_sa, 0, sizeof(new_sa));
    new_sa.sin_family = AF_INET;
    new_sa.sin_port = htons(9090);
    inet_pton(AF_INET, "10.0.0.1", &new_sa.sin_addr);
    addr.setSockAddr(new_sa);
    ASSERT_EQ(addr.toPort(), 9090);
    ASSERT_STREQ(addr.toIp(), "10.0.0.1");
}

int main() {
    test_inet_construction_port_only();
    test_inet_construction_loopback();
    test_inet_construction_ip_port();
    test_inet_construction_localhost();
    test_inet_port_zero();
    test_inet_large_port();
    test_inet_set_sockaddr();
    TEST_SUMMARY();
}
