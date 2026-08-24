#include "rpc/RpcClient.h"

#include <sys/socket.h>

#include <chrono>
#include <cstring>

#include <google/protobuf/service.h>

#include "base/Logger.h"
#include "net/Buffer.h"
#include "net/Connector.h"
#include "net/EventLoop.h"
#include "net/Socket.h"
#include "net/TcpConnection.h"
#include "rpc/RpcChannel.h"
#include "rpc/RpcCodec.h"
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

}  // namespace

RpcClient::RpcClient(EventLoop* loop, const InetAddress& serverAddr,
                     const std::string& name)
    : loop_(loop), serverAddr_(serverAddr), name_(name) {
    // The channel is created unbound and attaches on every (re)connect,
    // so the pointer handed out by channel() stays valid forever.
    channel_ = std::make_shared<RpcChannel>();
    connector_ = std::make_shared<Connector>(loop_, serverAddr_);
    connector_->setNewConnectionCallback(
        [this](int fd) { newConnection(fd); });
    connector_->setConnectFailedCallback([this]() { connectFailed(); });
}

RpcClient::~RpcClient() { disconnect(); }

void RpcClient::enableRetry() { connector_->enableRetry(); }

bool RpcClient::connect(int timeoutMs) {
    if (connected()) return true;
    if (timeoutMs <= 0) timeoutMs = 3000;

    auto established = std::make_shared<std::promise<bool>>();
    auto future = established->get_future();
    {
        std::lock_guard<std::mutex> lock(promiseMtx_);
        connectPromise_ = established;
    }

    connector_->start();
    auto status = future.wait_for(std::chrono::milliseconds(timeoutMs));
    if (status != std::future_status::ready) {
        // Timed out. Detach the waiter: with retry enabled the
        // connector keeps trying in the background and the client
        // becomes usable the moment the server appears; without retry,
        // abandon the attempt entirely.
        {
            std::lock_guard<std::mutex> lock(promiseMtx_);
            connectPromise_.reset();
        }
        if (!connector_->retryEnabled()) connector_->stop();
        LOG_ERROR("RpcClient[%s]: connect timeout to %s", name_.c_str(),
                  serverAddr_.toIpPort().c_str());
        return false;
    }

    bool ok = future.get();
    if (!ok) {
        LOG_ERROR("RpcClient[%s]: connect failed to %s", name_.c_str(),
                  serverAddr_.toIpPort().c_str());
    }
    return ok;
}

void RpcClient::newConnection(int fd) {
    // IO thread, from the Connector: the TCP connect completed and the
    // fd is ours to own.

    InetAddress localAddr;
    struct sockaddr_in localRaw;
    socklen_t localLen = sizeof(localRaw);
    if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&localRaw),
                      &localLen) == 0) {
        localAddr.setSockAddr(localRaw);
    }

    auto conn = std::make_shared<TcpConnection>(loop_, name_, fd, localAddr,
                                                serverAddr_);
    conn->setTcpNoDelay(true);
    // Pongs reset the heartbeat miss counter (dead-peer detection).
    channel_->setOnHeartbeat([this]() { hbMisses_.store(0); });
    conn->setMessageCallback(
        [this](const std::shared_ptr<TcpConnection>&, Buffer* buf) {
            channel_->onMessage(buf);
        });
    conn->setCloseCallback(
        [this](const std::shared_ptr<TcpConnection>& c) { closeConnection(c); });

    {
        std::lock_guard<std::mutex> lock(connMtx_);
        conn_ = conn;
    }
    channel_->attachConnection(conn);
    hbMisses_.store(0);
    // On the IO thread: establish inline (tie + arm reading) so the
    // waiter released below sees a fully usable connection.
    conn->connectEstablished();

    // Release the initial connect() waiter, if one is still around.
    std::shared_ptr<std::promise<bool>> p;
    {
        std::lock_guard<std::mutex> lock(promiseMtx_);
        p = connectPromise_;
        connectPromise_.reset();
    }
    if (p) p->set_value(true);

    LOG_INFO("RpcClient[%s]: connected to %s", name_.c_str(),
             serverAddr_.toIpPort().c_str());
}

void RpcClient::connectFailed() {
    // IO thread: terminal failure (retry disabled). Wake a waiting
    // connect() instead of letting it burn its whole timeout.
    std::shared_ptr<std::promise<bool>> p;
    {
        std::lock_guard<std::mutex> lock(promiseMtx_);
        p = connectPromise_;
        connectPromise_.reset();
    }
    if (p) p->set_value(false);
}

void RpcClient::closeConnection(const std::shared_ptr<TcpConnection>& conn) {
    // Runs on the IO thread from the close callback.
    if (channel_ != nullptr) channel_->onDisconnect();
    loop_->queueInLoop([conn] { conn->connectDestroyed(); });
    {
        std::lock_guard<std::mutex> lock(connMtx_);
        if (conn_ == conn) {
            conn_.reset();
        }
    }
    // Auto-reconnect: hand the socket back to the Connector with a
    // fresh backoff. No-op unless started with retry enabled - a manual
    // disconnect() stops the connector before closing the connection.
    connector_->restart();
    LOG_INFO("RpcClient[%s]: disconnected from %s", name_.c_str(),
             serverAddr_.toIpPort().c_str());
}

void RpcClient::disconnect() {
    if (loop_ == nullptr) return;  // never had an IO thread: nothing to drain

    hbCancelled_.store(true);

    // Single teardown functor queued on the IO loop. It is the LAST work
    // item we queue that touches `this`, so the handshake below waits for
    // IT - not merely for the connection's close callback, which a
    // peer-initiated close can satisfy while this functor is still
    // pending (use-after-free on the freed RpcClient).
    {
        std::lock_guard<std::mutex> lock(closeMtx_);
        drained_ = false;
    }
    loop_->queueInLoop([this]() {
        // hbArmed_/hbTimerId_ are IO-thread-only state.
        if (hbArmed_) {
            loop_->cancel(hbTimerId_);
            hbArmed_ = false;
        }
        // Stop the connector BEFORE closing the connection: otherwise
        // closeConnection would restart it (auto-reconnect) on what is
        // a manual teardown. stop() also cancels pending retries and
        // closes an in-flight half-open socket, so no connector event
        // can fire after this functor finishes. The connection is
        // snapshotted here (not in disconnect()) so that a connect
        // completing in the same loop iteration is still torn down.
        connector_->stop();
        std::shared_ptr<TcpConnection> conn = snapshotConn();
        if (conn != nullptr && !conn->disconnected()) {
            // forceClose() uses runInLoop, which runs INLINE on the IO
            // thread: by the time it returns, handleClose ->
            // closeConnection has fully torn the connection down
            // (channel cleanup, pending-call failure, conn_ reset).
            conn->forceClose();
        }
        std::lock_guard<std::mutex> lock(closeMtx_);
        drained_ = true;
        closeCv_.notify_all();
    });

    // Unconditional handshake: the functor touches `this` even without a
    // connection (heartbeat-only teardown), so destruction must wait for
    // it in every case. Bounded wait in case the IO thread is gone.
    std::unique_lock<std::mutex> lock(closeMtx_);
    closeCv_.wait_for(lock, std::chrono::seconds(3),
                      [this] { return drained_; });
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

    // The channel arms an IO-thread deadline of `timeoutMs`; on expiry
    // the call completes with kTimeout ("timeout") through the normal
    // done-callback path - the same one async callers go through.
    RpcChannel::CallSlotPtr slot =
        channel_->sendRequest(serviceName, methodName, args, response,
                              &controller, new PromiseDone(promise),
                              timeoutMs);

    if (timeoutMs > 0) {
        // Grace window for the IO-side timeout to resolve the promise;
        // only a stalled/dead IO thread falls through to the manual
        // abandon path below.
        auto status = future.wait_for(
            std::chrono::milliseconds(timeoutMs + 500));
        if (status != std::future_status::ready) {
            // Give up: detach response/controller from the channel BEFORE
            // this stack frame dies, otherwise a late reply (or the
            // disconnect sweep of a subsequent failover) would
            // dereference dangling pointers.
            channel_->cancelRequest(slot);
            if (error != nullptr) *error = "timeout";
            return false;
        }
    } else {
        future.wait();
    }

    if (controller.Failed()) {
        if (error != nullptr) *error = controller.ErrorText();
        return false;
    }
    return true;
}

void RpcClient::enableHeartbeat(double intervalSec, int maxMissed) {
    hbRequested_.store(true);
    hbMaxMissed_ = maxMissed;
    hbMisses_.store(0);
    // Arm on the IO loop: runEvery itself only arms the timerfd, but the
    // hbArmed_/hbTimerId_ bookkeeping must stay IO-thread-only, and the
    // hbCancelled_ check must be evaluated on the loop (see disconnect).
    loop_->runInLoop([this, intervalSec]() {
        if (hbCancelled_.load() || hbArmed_) return;
        hbArmed_ = true;
        hbTimerId_ =
            loop_->runEvery(intervalSec, [this]() { heartbeatTick(); });
    });
}

void RpcClient::heartbeatTick() {
    std::shared_ptr<TcpConnection> conn = snapshotConn();
    if (conn == nullptr || !conn->connected()) {
        return;  // between connections; counting resumes on reconnect
    }
    // fetch_add returns the PRE-increment value: with maxMissed = 3 the
    // client sends 3 unanswered pings and reaps on the 4th tick.
    if (hbMisses_.fetch_add(1) >= hbMaxMissed_) {
        LOG_WARN("RpcClient[%s]: %d heartbeats unanswered, closing %s",
                 name_.c_str(), hbMaxMissed_,
                 serverAddr_.toIpPort().c_str());
        conn->forceClose();
        return;
    }
    std::string ping;
    RpcCodec::appendFrame(&ping, kHeartbeat, 0, std::string());
    conn->send(ping);
}

}  // namespace rpc
}  // namespace aether
