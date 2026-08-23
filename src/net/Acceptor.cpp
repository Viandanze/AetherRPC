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
    acceptChannel_->disableAll();
    acceptChannel_->remove();
    ::close(idleFd_);
}

void Acceptor::listen() {
    loop_->assertInLoopThread();
    listening_ = true;
    acceptSocket_->listen();
    acceptChannel_->enableReading();
}

void Acceptor::stopListening() {
    loop_->assertInLoopThread();
    listening_ = false;
    acceptChannel_->disableReading();
    LOG_INFO("Acceptor: stopped listening");
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
