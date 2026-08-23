#include "rpc/RpcClient.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <cstdio>
#include <cstring>
#include <future>

#include <google/protobuf/service.h>

#include "base/Logger.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
#include "rpc/RpcChannel.h"
#include "rpc/RpcController.h"

namespace aether {
namespace rpc {

namespace {

/// Self-deleting closure that fulfils a promise owned through a
/// shared_ptr. Even if the reply arrives after syncCall has already
/// timed out, calling set_value() on an abandoned promise is safe.
class PromiseDone : public google::protobuf::Closure {
public:
    explicit PromiseDone(std::shared_ptr<std::promise<void>> promise)
        : promise_(std::move(promise)) {}

    void Run() override {
        if (promise_ != nullptr) promise_->set_value();
        delete this;
    }

private:
    std::shared_ptr<std::promise<void>> promise_;
};

int getSocketError(int fd) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        return errno;
    }
    return err;
}

}  // namespace

RpcClient::RpcClient(EventLoop* loop, const InetAddress& serverAddr,
                     const std::string& name)
    : loop_(loop), serverAddr_(serverAddr), name_(name) {}

RpcClient::~RpcClient() { disconnect(); }

bool RpcClient::connect(int timeoutMs) {
    if (connected()) return true;

    const struct sockaddr_in& addr = serverAddr_.getSockAddr();

    // Phase 1: non-blocking connect + poll, then hand the socket to the
    // EventLoop. The connection itself is fully non-blocking afterwards.
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        LOG_ERROR("RpcClient[%s]: socket() failed: %s", name_.c_str(),
                  strerror(errno));
        return false;
    }

    int ret = ::connect(fd,
                        reinterpret_cast<const struct sockaddr*>(&addr),
                        sizeof(addr));
    if (ret < 0 && errno != EINPROGRESS) {
        LOG_ERROR("RpcClient[%s]: connect() failed: %s", name_.c_str(),
                  strerror(errno));
        ::close(fd);
        return false;
    }
    if (ret != 0) {  // EINPROGRESS: wait for writability
        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = POLLOUT;
        pfd.revents = 0;
        int pr = ::poll(&pfd, 1, timeoutMs);
        if (pr <= 0) {
            LOG_ERROR("RpcClient[%s]: connect timeout to %s", name_.c_str(),
                      serverAddr_.toIpPort().c_str());
            ::close(fd);
            return false;
        }
        int err = getSocketError(fd);
        if (err != 0) {
            LOG_ERROR("RpcClient[%s]: connect failed: %s", name_.c_str(),
                      strerror(err));
            ::close(fd);
            return false;
        }
    }

    InetAddress localAddr;
    struct sockaddr_in localRaw;
    socklen_t localLen = sizeof(localRaw);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&localRaw), &localLen) == 0) {
        localAddr.setSockAddr(localRaw);
    }

    auto conn = std::make_shared<TcpConnection>(loop_, name_, fd, localAddr,
                                                serverAddr_);
    conn->setTcpNoDelay(true);
    channel_ = std::make_shared<RpcChannel>(conn);
    conn->setMessageCallback(
        [this](const std::shared_ptr<TcpConnection>&, Buffer* buf) {
            channel_->onMessage(buf);
        });
    conn->setCloseCallback(
        [this](const std::shared_ptr<TcpConnection>& c) { closeConnection(c); });

    conn_ = conn;
    {
        std::lock_guard<std::mutex> lock(closeMtx_);
        closed_ = false;
    }

    // connectEstablished() runs asynchronously on the IO thread; wait for
    // it so that connect() returning true implies a usable connection
    // (otherwise a follow-up syncCall could see state == kConnecting).
    auto established = std::make_shared<std::promise<void>>();
    auto satisfied = std::make_shared<std::atomic<bool>>(false);
    auto future = established->get_future();
    // TcpConnection fires connectionCallback_ both on establish AND on
    // close; guard so the promise is satisfied exactly once.
    conn->setConnectionCallback(
        [established, satisfied](const std::shared_ptr<TcpConnection>&) {
            if (!satisfied->exchange(true)) established->set_value();
        });
    loop_->runInLoop([conn] { conn->connectEstablished(); });
    future.wait_for(std::chrono::milliseconds(1000));

    LOG_INFO("RpcClient[%s]: connected to %s", name_.c_str(),
             serverAddr_.toIpPort().c_str());
    return true;
}

void RpcClient::closeConnection(const std::shared_ptr<TcpConnection>& conn) {
    // Runs on the IO thread from the close callback.
    if (channel_ != nullptr) channel_->onDisconnect();
    loop_->queueInLoop([conn] { conn->connectDestroyed(); });
    if (conn_ == conn) {
        conn_.reset();
    }
    {
        std::lock_guard<std::mutex> lock(closeMtx_);
        closed_ = true;
    }
    closeCv_.notify_all();
}

void RpcClient::disconnect() {
    if (conn_ != nullptr) {
        std::shared_ptr<TcpConnection> conn = conn_;
        conn->forceClose();  // triggers the close callback on the IO thread
        // Wait for the close callback to complete on the IO thread: the
        // channel/poller bookkeeping must finish before this object (and
        // the connection it references) goes away.
        std::unique_lock<std::mutex> lock(closeMtx_);
        closeCv_.wait_for(lock, std::chrono::seconds(3),
                          [this] { return closed_; });
    }
}

google::protobuf::RpcChannel* RpcClient::channel() {
    return channel_.get();
}

bool RpcClient::syncCall(const std::string& serviceName,
                         const std::string& methodName,
                         const google::protobuf::Message& request,
                         google::protobuf::Message* response, int timeoutMs,
                         std::string* error) {
    if (!connected()) {
        if (error != nullptr) *error = "not connected";
        return false;
    }

    std::string args;
    if (!request.SerializeToString(&args)) {
        if (error != nullptr) *error = "failed to serialize request";
        return false;
    }

    auto promise = std::make_shared<std::promise<void>>();
    std::future<void> future = promise->get_future();
    RpcController controller;

    channel_->sendRequest(serviceName, methodName, args, response, &controller,
                          new PromiseDone(promise));

    auto status = future.wait_for(std::chrono::milliseconds(timeoutMs));
    if (status != std::future_status::ready) {
        // The pending entry is left in the map; a late reply will find an
        // abandoned promise (harmless set_value) and be discarded.
        if (error != nullptr) *error = "timeout";
        return false;
    }

    if (controller.Failed()) {
        if (error != nullptr) *error = controller.ErrorText();
        return false;
    }
    return true;
}

}  // namespace rpc
}  // namespace aether
