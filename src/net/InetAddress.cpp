#include "InetAddress.h"
#include <cstring>
#include <arpa/inet.h>
#include <netinet/in.h>

InetAddress::InetAddress(uint16_t port, bool loopbackOnly) {
    memset(&addr_, 0, sizeof(addr_));
    addr_.sin_family = AF_INET;
    addr_.sin_addr.s_addr = htonl(loopbackOnly ? INADDR_LOOPBACK : INADDR_ANY);
    addr_.sin_port = htons(port);
}

InetAddress::InetAddress(const std::string& ip, uint16_t port) {
    memset(&addr_, 0, sizeof(addr_));
    addr_.sin_family = AF_INET;
    addr_.sin_port = htons(port);
    ::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
}

std::string InetAddress::toIp() const {
    char buf[64];
    ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof(buf));
    return buf;
}

std::string InetAddress::toIpPort() const {
    return toIp() + ":" + std::to_string(toPort());
}

uint16_t InetAddress::toPort() const {
    return ntohs(addr_.sin_port);
}
