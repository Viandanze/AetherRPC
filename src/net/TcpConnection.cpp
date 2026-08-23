#include "TcpConnection.h"
#include "EventLoop.h"
#include "Socket.h"
#include "Channel.h"
#include "InetAddress.h"
#include "base/Logger.h"
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <vector>
#include <sys/sendfile.h>

TcpConnection::TcpConnection(EventLoop* loop, const std::string& name, int fd,
                             const InetAddress& localAddr, const InetAddress& peerAddr)
    : loop_(loop),
      name_(name),
      state_(kConnecting),
      socket_(new Socket(fd)),
      channel_(new Channel(loop, fd)),
      localAddr_(localAddr),
      peerAddr_(peerAddr),
      highWaterMark_(64 * 1024 * 1024),  // 64MB
      idleTimerGeneration_(0) {
    channel_->setReadCallback([this]() { handleRead(); });
    channel_->setWriteCallback([this]() { handleWrite(); });
    channel_->setCloseCallback([this]() { handleClose(); });
    channel_->setErrorCallback([this]() { handleError(); });
}

TcpConnection::~TcpConnection() {}

void TcpConnection::connectEstablished() {
    setState(kConnected);
    channel_->enableReading();
    if (connectionCallback_) connectionCallback_(shared_from_this());
}

void TcpConnection::connectDestroyed() {
    // On the normal close path handleClose() has already fired connectionCallback_;
    // this only covers connections destroyed without going through the close callback (e.g. still alive at server shutdown),
    // the state check keeps the upper-layer callback from firing twice (duplicate logs / duplicate cleanup).
    if (state_ == kConnected) {
        setState(kDisconnected);
        channel_->disableAll();
        if (connectionCallback_) connectionCallback_(shared_from_this());
    }
    channel_->remove();
}

// --- sending ---

void TcpConnection::send(const std::string& message) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendInLoop(message);
        } else {
            loop_->runInLoop([this, message]() {
                sendInLoop(message);
            });
        }
    }
}

void TcpConnection::send(const char* data, size_t len) {
    send(std::string(data, len));
}

void TcpConnection::send(Buffer* buf) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendInLoop(buf->retrieveAllAsString());
        } else {
            loop_->runInLoop([this, str = buf->retrieveAllAsString()]() {
                sendInLoop(str);
            });
        }
    }
}

void TcpConnection::sendInLoop(const std::string& message) {
    sendInLoop(message.data(), message.size());
}

void TcpConnection::sendInLoop(const char* data, size_t len) {
    ssize_t nwrote = 0;
    size_t remaining = len;

    // if the output buffer is empty and no write event is pending, try writing directly
    if (!channel_->isWriting() && outputBuf_.readableBytes() == 0) {
        nwrote = ::write(channel_->fd(), data, len);
        if (nwrote >= 0) {
            remaining = len - nwrote;
            if (remaining == 0 && writeCompleteCallback_) {
                loop_->queueInLoop([this]() { writeCompleteCallback_(shared_from_this()); });
            }
        } else {
            nwrote = 0;
            if (errno != EWOULDBLOCK) {
                LOG_ERROR("TcpConnection::sendInLoop write error: %s", strerror(errno));
            }
        }
    }

    // whatever doesn't fit goes to outputBuf_ and is flushed when the fd is writable
    if (remaining > 0) {
        // high-water mark check
        size_t oldLen = outputBuf_.readableBytes();
        if (oldLen + remaining >= highWaterMark_
            && oldLen < highWaterMark_
            && highWaterMarkCallback_) {
            size_t highLen = oldLen + remaining;
            loop_->queueInLoop([this, highLen]() {
                highWaterMarkCallback_(shared_from_this(), highLen);
            });
        }
        outputBuf_.append(data + nwrote, remaining);
        if (!channel_->isWriting()) {
            channel_->enableWriting();
        }
    }
}

// --- zero-copy file sending ---

void TcpConnection::sendFile(int fileFd, off_t offset, size_t count) {
    if (state_ == kConnected) {
        if (loop_->isInLoopThread()) {
            sendFileInLoop(fileFd, offset, count);
        } else {
            loop_->runInLoop([this, fileFd, offset, count]() {
                sendFileInLoop(fileFd, offset, count);
            });
        }
    } else {
        ::close(fileFd);  // connection is no longer Connected; fd ownership ends here
    }
}

// Fast path: while outputBuf_ is empty and no write event is pending, loop sendfile(2) so data stays in kernel space;
// fallback: when sendfile hits EAGAIN (socket buffer full) or the header hasn't fully flushed,
// pread the rest into outputBuf_ and continue via the normal write event, so the body strictly follows the header.
// fd ownership ends inside this function: no member state, and a mid-transfer close can't leak the fd.
void TcpConnection::sendFileInLoop(int fileFd, off_t offset, size_t count) {
    const bool fastPath =
        (outputBuf_.readableBytes() == 0 && !channel_->isWriting());

    off_t off = offset;
    size_t left = count;

    if (fastPath) {
        while (left > 0) {
            size_t chunk = left > (1u << 20) ? (1u << 20) : left;  // 1MB per call
            ssize_t n = ::sendfile(channel_->fd(), fileFd, &off, chunk);
            if (n > 0) {
                left -= static_cast<size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;  // buffer full -> fall back
            if (n == 0) {
                // file shrank since stat (truncated mid-send): give up the rest, deliver a short response
                LOG_WARN("TcpConnection::sendFileInLoop file truncated during send");
                left = 0;
                break;
            }
            // real error (connection dead, etc.): drop the rest, let handleClose finish up
            LOG_ERROR("TcpConnection::sendFileInLoop sendfile error: %s", strerror(errno));
            left = 0;
            break;
        }
    }

    if (left > 0) {
        std::vector<char> tmp(left);
        ssize_t n = ::pread(fileFd, tmp.data(), left, off);
        if (n > 0) {
            outputBuf_.append(tmp.data(), static_cast<size_t>(n));
            channel_->enableWriting();
        }
        // n<=0: file deleted mid-send; deliver only what got out
    }

    ::close(fileFd);

    // non-keep-alive ordering note: if the layer above then calls shutdown(), shutdownInLoop checks
    // isWriting() -- in the fallback path write events are enabled, so we half-close after handleWrite drains;
    // in the fast path everything is out, so shutdownWrite fires immediately. Nothing more to do here.
}

// --- closing ---

void TcpConnection::shutdown() {
    if (state_ == kConnected) {
        setState(kDisconnecting);
        loop_->runInLoop([this]() { shutdownInLoop(); });
    }
}

void TcpConnection::shutdownInLoop() {
    if (!channel_->isWriting()) {
        socket_->shutdownWrite();
    }
}

void TcpConnection::forceClose() {
    if (state_ == kConnected || state_ == kDisconnecting) {
        setState(kDisconnecting);
        loop_->runInLoop([this]() { forceCloseInLoop(); });
    }
}

void TcpConnection::forceCloseWithDelay(double seconds) {
    if (state_ == kConnected || state_ == kDisconnecting) {
        setState(kDisconnecting);
        auto self = shared_from_this();
        loop_->runAfter(seconds, [self]() { self->forceClose(); });
    }
}

void TcpConnection::forceCloseInLoop() {
    if (state_ == kConnected || state_ == kDisconnecting) {
        handleClose();
    }
}

void TcpConnection::setTcpNoDelay(bool on) {
    socket_->setTcpNoDelay(on);
}

// --- event handling ---

void TcpConnection::handleRead() {
    // ET mode must read in a loop until EAGAIN
    int savedErrno = 0;
    ssize_t n = inputBuf_.readFd(channel_->fd(), &savedErrno);

    while (n > 0) {
        if (messageCallback_) {
            messageCallback_(shared_from_this(), &inputBuf_);
        }
        // keep reading; in ET mode there may be more data
        n = inputBuf_.readFd(channel_->fd(), &savedErrno);
    }

    if (n == 0) {
        handleClose();
    } else if (n < 0) {
        if (savedErrno != EAGAIN && savedErrno != EWOULDBLOCK) {
            LOG_ERROR("TcpConnection::handleRead error: %s", strerror(savedErrno));
            handleError();
        }
        // EAGAIN is the normal ET exit condition, not an error
    }
}

void TcpConnection::handleWrite() {
    if (channel_->isWriting()) {
        ssize_t n = ::write(channel_->fd(),
                            outputBuf_.peek(),
                            outputBuf_.readableBytes());
        if (n > 0) {
            outputBuf_.retrieve(n);
            if (outputBuf_.readableBytes() == 0) {
                channel_->disableWriting();
                if (writeCompleteCallback_) {
                    loop_->queueInLoop([this]() { writeCompleteCallback_(shared_from_this()); });
                }
                if (state_ == kDisconnecting) {
                    shutdownInLoop();
                }
            }
        } else {
            LOG_ERROR("TcpConnection::handleWrite write error: %s", strerror(errno));
        }
    }
}

void TcpConnection::handleClose() {
    setState(kDisconnected);
    channel_->disableAll();

    // notify the upper layer that the connection closed
    if (closeCallback_) closeCallback_(shared_from_this());
    if (connectionCallback_) connectionCallback_(shared_from_this());
}

void TcpConnection::handleError() {
    int opt = 0;
    socklen_t len = sizeof(opt);
    ::getsockopt(channel_->fd(), SOL_SOCKET, SO_ERROR, &opt, &len);
    LOG_ERROR("TcpConnection::handleError SO_ERROR=%d (%s)", opt, strerror(opt));
}
