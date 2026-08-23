#include "Socket.h"
#include "InetAddress.h"
#include "base/Logger.h"
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <cstring>

Socket::~Socket() { ::close(fd_); }

void Socket::bindAddress(const InetAddress& localAddr) {
    if (::bind(fd_, reinterpret_cast<const sockaddr*>(&localAddr.getSockAddr()),
               sizeof(localAddr.getSockAddr())) < 0) {
        LOG_FATAL("bind failed: %s", strerror(errno));
    }
}

void Socket::listen() {
    if (::listen(fd_, SOMAXCONN) < 0) {
        LOG_FATAL("listen failed: %s", strerror(errno));
    }
}

int Socket::accept(InetAddress* peerAddr) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    socklen_t len = sizeof(addr);
    int connFd = ::accept4(fd_, reinterpret_cast<sockaddr*>(&addr), &len,
                           SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connFd >= 0) {
        peerAddr->setSockAddr(addr);
    } else {
        int savedErrno = errno;
        switch (savedErrno) {
            case EAGAIN:
            case ECONNABORTED:
            case EINTR:
            case EPERM:
            case EMFILE:
                errno = savedErrno;
                break;
            case EBADF:
            case EFAULT:
            case EINVAL:
            case ENFILE:
            case ENOMEM:
                LOG_FATAL("accept fatal error: %s", strerror(savedErrno));
                break;
            default:
                LOG_FATAL("accept unknown error: %s", strerror(savedErrno));
                break;
        }
    }
    return connFd;
}

void Socket::setReuseAddr(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

void Socket::setReusePort(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
}

void Socket::setTcpNoDelay(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

void Socket::setKeepAlive(bool on) {
    int opt = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
}

void Socket::shutdownWrite() {
    ::shutdown(fd_, SHUT_WR);
}
