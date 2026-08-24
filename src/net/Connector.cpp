#include "Connector.h"

#include <cerrno>
#include <cstring>

#include <sys/socket.h>
#include <unistd.h>

#include "Channel.h"
#include "EventLoop.h"
#include "base/Logger.h"

namespace {

const int kInitRetryDelayMs = 500;
const int kMaxRetryDelayMs  = 30000;

int getSocketError(int fd) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        return errno;
    }
    return err;
}

}  // namespace

Connector::Connector(EventLoop* loop, const InetAddress& serverAddr)
    : loop_(loop), serverAddr_(serverAddr), retryDelayMs_(kInitRetryDelayMs) {}

Connector::~Connector() {
    // The owner is responsible for stop() before dropping the last
    // reference (RpcClient::disconnect() does). Nothing to clean up
    // here: a handed-off fd belongs to the connection, and a connecting
    // fd is closed synchronously in stopInLoop().
}

void Connector::start() {
    connect_.store(true);
    // Capture shared_from_this: the queued functor must not touch a
    // destroyed Connector if the owner drops the last reference before
    // the loop gets around to running it.
    auto self = shared_from_this();
    loop_->runInLoop([self] { self->startInLoop(); });
}

void Connector::startInLoop() {
    loop_->assertInLoopThread();
    if (state_ == kDisconnected && connect_.load()) {
        connect();
    }
    // kConnecting: attempt already in flight; kConnected: fd handed off.
}

void Connector::stop() {
    connect_.store(false);
    auto self = shared_from_this();
    loop_->runInLoop([self] { self->stopInLoop(); });
}

void Connector::stopInLoop() {
    loop_->assertInLoopThread();
    if (state_ == kConnecting) {
        // Abandon the in-flight attempt: deregister its channel and
        // close the half-open socket.
        setState(kDisconnected);
        int sockfd = removeChannel();
        ::close(sockfd);
    }
    if (retryTimerArmed_) {
        loop_->cancel(retryTimer_);
        retryTimerArmed_ = false;
    }
}

void Connector::restart() {
    // Loop thread only - called from the owner's connection-close
    // callback, which runs on the loop.
    loop_->assertInLoopThread();
    if (!connect_.load() || !retry_.load()) return;
    if (state_.load() == kConnected) {
        // The handed-off connection is gone: we are back at
        // kDisconnected and a fresh attempt is legal.
        setState(kDisconnected);
        retryDelayMs_ = kInitRetryDelayMs;
        connect();
    }
}

void Connector::connect() {
    loop_->assertInLoopThread();
    int sockfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (sockfd < 0) {
        LOG_ERROR("Connector[%s]: socket() failed: %s",
                  serverAddr_.toIpPort().c_str(), strerror(errno));
        retry(-1);
        return;
    }

    const struct sockaddr_in& addr = serverAddr_.getSockAddr();
    int ret = ::connect(sockfd,
                        reinterpret_cast<const struct sockaddr*>(&addr),
                        sizeof(addr));
    int savedErrno = (ret == 0) ? 0 : errno;
    switch (savedErrno) {
        case 0:
        case EINPROGRESS:
        case EINTR:
        case EISCONN:
            connecting(sockfd);
            break;

        case EAGAIN:
        case EADDRINUSE:
        case EADDRNOTAVAIL:
        case ECONNREFUSED:
        case ENETUNREACH:
        case ETIMEDOUT:
            LOG_DEBUG("Connector[%s]: connect failed: %s",
                      serverAddr_.toIpPort().c_str(), strerror(savedErrno));
            retry(sockfd);
            break;

        default:
            LOG_ERROR("Connector[%s]: unexpected connect error: %s",
                      serverAddr_.toIpPort().c_str(), strerror(savedErrno));
            ::close(sockfd);
            break;
    }
}

void Connector::connecting(int sockfd) {
    loop_->assertInLoopThread();
    setState(kConnecting);
    channel_.reset(new Channel(loop_, sockfd));
    auto self = shared_from_this();
    channel_->setWriteCallback([self] { self->handleWrite(); });
    channel_->setErrorCallback([self] { self->handleError(); });
    channel_->enableWriting();
}

int Connector::removeChannel() {
    loop_->assertInLoopThread();
    channel_->disableAll();
    channel_->remove();
    int sockfd = channel_->fd();
    // channel_ itself is reset from a queued functor: we may currently be
    // inside Channel::handleEvent() on this very channel. The functor
    // holds shared_from_this, so the Connector stays alive until it runs.
    auto self = shared_from_this();
    loop_->queueInLoop([self] { self->resetChannel(); });
    return sockfd;
}

void Connector::resetChannel() {
    loop_->assertInLoopThread();
    channel_.reset();
}

void Connector::handleWrite() {
    loop_->assertInLoopThread();
    if (state_ != kConnecting) return;  // stale event (e.g. stop() raced us)

    int sockfd = removeChannel();
    int err = getSocketError(sockfd);
    if (err == 0) {
        setState(kConnected);
        if (connect_.load() && newConnectionCallback_) {
            // fd ownership transfers to the owner.
            newConnectionCallback_(sockfd);
        } else {
            // stop() arrived between writability and here.
            ::close(sockfd);
        }
        // Consecutive-failure backoff resets after a success.
        retryDelayMs_ = kInitRetryDelayMs;
    } else {
        LOG_DEBUG("Connector[%s]: connect failed: %s",
                  serverAddr_.toIpPort().c_str(), strerror(err));
        ::close(sockfd);
        retry(sockfd);
    }
}

void Connector::handleError() {
    loop_->assertInLoopThread();
    if (state_ != kConnecting) return;  // handleWrite already handled it

    int sockfd = removeChannel();
    int err = getSocketError(sockfd);
    LOG_DEBUG("Connector[%s]: connect error event: %s",
              serverAddr_.toIpPort().c_str(),
              strerror(err != 0 ? err : errno));
    ::close(sockfd);
    retry(sockfd);
}

void Connector::retry(int sockfd) {
    loop_->assertInLoopThread();
    if (sockfd >= 0) ::close(sockfd);
    setState(kDisconnected);

    if (connect_.load() && retry_.load()) {
        retryDelayMs_ = std::min(retryDelayMs_ * 2, kMaxRetryDelayMs);
        LOG_INFO("Connector[%s]: retrying in %d ms",
                 serverAddr_.toIpPort().c_str(), retryDelayMs_);
        auto self = shared_from_this();
        retryTimer_ = loop_->runAfter(retryDelayMs_ / 1000.0,
                                      [self] { self->retryInLoop(); });
        retryTimerArmed_ = true;
    } else {
        // Terminal failure with retry disabled: report and stay idle.
        if (connectFailedCallback_) connectFailedCallback_();
    }
}

void Connector::retryInLoop() {
    loop_->assertInLoopThread();
    retryTimerArmed_ = false;
    if (connect_.load() && state_ == kDisconnected) {
        connect();
    }
}
