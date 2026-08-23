#include "Acceptor.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Socket.h"
#include "Channel.h"
#include "base/Logger.h"
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>

Acceptor::Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reusePort)
    : loop_(loop),
      acceptSocket_(new Socket(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0))),
      acceptChannel_(new Channel(loop, acceptSocket_->fd())),
      listening_(false),
      idleFd_(::open("/dev/null", O_RDONLY | O_CLOEXEC)) {
    acceptSocket_->setReuseAddr(true);
    acceptSocket_->setReusePort(reusePort);
    acceptSocket_->bindAddress(listenAddr);
    acceptChannel_->setReadCallback([this]() { handleRead(); });
}

Acceptor::~Acceptor() {
    // When stopListening() already closed the socket and removed the
    // channel, touching the channel again would re-register a closed fd
    // in the poller (EBADF).
    if (acceptSocket_ != nullptr) {
        acceptChannel_->disableAll();
        acceptChannel_->remove();
    }
    ::close(idleFd_);
}

void Acceptor::listen() {
    loop_->assertInLoopThread();
    if (acceptSocket_ == nullptr) {
        // re-listening after stopListening() is not supported
        LOG_ERROR("Acceptor::listen() after stopListening(): socket closed");
        return;
    }
    listening_ = true;
    acceptSocket_->listen();
    acceptChannel_->enableReading();
}

void Acceptor::stopListening() {
    loop_->assertInLoopThread();
    if (!listening_) return;
    listening_ = false;
    // A "stopped" server must refuse new connections: merely disabling
    // the epoll watch leaves the listening fd open, so fresh connects
    // still complete the TCP handshake into the accept backlog and then
    // black-hole (nobody ever accept()s them). During graceful shutdown
    // that traps clients into waiting out their full call deadline. Close
    // the socket so connects fail fast with ECONNREFUSED.
    acceptChannel_->disableAll();
    acceptChannel_->remove();
    acceptSocket_.reset();
    LOG_INFO("Acceptor: stopped listening (socket closed)");
}

void Acceptor::handleRead() {
    // drain the listen socket in a loop (works for both LT and ET)
    for (;;) {
        InetAddress peerAddr;
        int connFd = acceptSocket_->accept(&peerAddr);
        if (connFd >= 0) {
            if (newConnectionCallback_) {
                newConnectionCallback_(connFd, peerAddr);
            } else {
                ::close(connFd);
            }
        } else {
            int savedErrno = errno;
            if (savedErrno == EMFILE) {
                // fd exhausted: free the spare fd, take and drop one pending
                // connection so the client sees a clean close, then re-arm
                ::close(idleFd_);
                idleFd_ = ::accept(acceptSocket_->fd(), nullptr, nullptr);
                ::close(idleFd_);
                idleFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
                LOG_WARN("Acceptor: EMFILE hit, used idleFd trick");
            } else if (savedErrno == EAGAIN || savedErrno == EWOULDBLOCK) {
                break;  // no more pending connections
            } else if (savedErrno == ECONNABORTED || savedErrno == EINTR) {
                continue;
            } else {
                LOG_ERROR("Acceptor::handleRead accept error: %s", strerror(savedErrno));
                break;
            }
        }
    }
}
