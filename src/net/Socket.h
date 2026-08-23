#ifndef AETHER_NET_SOCKET_H
#define AETHER_NET_SOCKET_H
#pragma once
#include "base/noncopyable.h"

// RAII wrapper around a socket fd
class Socket : noncopyable {
public:
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket();

    int fd() const { return fd_; }

    // server-side socket operations
    void bindAddress(const class InetAddress& localAddr);
    void listen();
    int  accept(class InetAddress* peerAddr);

    // socket options
    void setReuseAddr(bool on);
    void setReusePort(bool on);
    void setTcpNoDelay(bool on);
    void setKeepAlive(bool on);

    // shut down the write side
    void shutdownWrite();

private:
    const int fd_;
};
#endif // AETHER_NET_SOCKET_H
