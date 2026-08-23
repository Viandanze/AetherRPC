#ifndef AETHER_RPC_CLIENT_H
#define AETHER_RPC_CLIENT_H
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include "base/TimerId.h"
#include "base/noncopyable.h"
#include "net/InetAddress.h"
#include "net/TcpConnection.h"

class EventLoop;

namespace aether {
namespace rpc {

class RpcChannel;

/// RpcClient: owns one TCP connection to one AetherRPC server plus the
/// RpcChannel multiplexing calls over it.
///
/// Usage:
///   EventLoopThread loopThread;
///   EventLoop* loop = loopThread.startLoop();
///   RpcClient client(loop, InetAddress("127.0.0.1", 8080));
///   client.connect();          // blocking until established (Phase 1)
///   EchoResponse resp;
///   if (client.syncCall("EchoService", "Echo", req, &resp)) { ... }
///
/// syncCall() must not be called from the connection's own IO thread
/// (it would wait on itself). Run it from a regular thread.
///
/// The connection itself uses non-blocking IO on the EventLoop; only the
/// connect() handshake blocks. A non-blocking connector with reconnect
/// is planned for Phase 3.
class RpcClient : noncopyable {
public:
    RpcClient(EventLoop* loop, const InetAddress& serverAddr,
              const std::string& name = "aether-rpc-client");
    ~RpcClient();

    /// Blocking connect with timeout. Returns false on failure/timeout.
    bool connect(int timeoutMs = 3000);

    /// Close the connection (fails pending calls).
    void disconnect();

    bool connected() const { return conn_ != nullptr && conn_->connected(); }

    /// Channel for generated stubs (asynchronous usage).
    google::protobuf::RpcChannel* channel();

    /// Synchronous call: blocks until the reply arrives or the deadline
    /// hits. Returns false on timeout / transport error; a failed RPC
    /// (service not found, ...) also returns false. `timeoutMs <= 0`
    /// disables the deadline (wait forever).
    bool syncCall(const std::string& serviceName,
                  const std::string& methodName,
                  const google::protobuf::Message& request,
                  google::protobuf::Message* response, int timeoutMs = 3000,
                  std::string* error = nullptr);

    /// Enable connection keepalive (Phase 3): send an empty HEARTBEAT
    /// frame every `intervalSec` seconds; a pong resets the miss
    /// counter. After `maxMissed` consecutive unanswered intervals the
    /// peer is considered dead and the connection is force-closed (so
    /// callers see the failure instead of hanging on a black hole).
    ///
    /// Call after a successful connect(). The timer lives on the IO
    /// loop and survives reconnects.
    void enableHeartbeat(double intervalSec, int maxMissed = 3);

    const InetAddress& serverAddr() const { return serverAddr_; }

private:
    void closeConnection(const std::shared_ptr<TcpConnection>& conn);
    void heartbeatTick();  // IO thread: send ping / reap dead peer

    EventLoop* loop_;
    const InetAddress serverAddr_;
    const std::string name_;

    std::shared_ptr<TcpConnection> conn_;
    std::shared_ptr<RpcChannel> channel_;

    // Heartbeat state. hbArmed_/hbTimerId_ are IO-thread-only; the
    // atomics arbitrate the arm-vs-teardown race (a heartbeat requested
    // right before destruction must never arm after the dtor finished).
    std::atomic<int> hbMisses_{0};
    std::atomic<bool> hbRequested_{false};
    std::atomic<bool> hbCancelled_{false};
    int hbMaxMissed_ = 3;
    bool hbArmed_ = false;  // IO thread only
    TimerId hbTimerId_{};   // IO thread only

    // disconnect() handshake. drained_ is set ONLY by the teardown
    // functor disconnect() queues on the IO loop, and disconnect() waits
    // on it before returning: the functor touches `this` (heartbeat
    // bookkeeping, close callbacks) even when there is no connection, so
    // ~RpcClient must never overtake it. Waiting on the connection's own
    // "closed" state instead would be racy: a peer-initiated close can
    // satisfy it while the teardown functor is still pending.
    std::mutex closeMtx_;
    std::condition_variable closeCv_;
    bool drained_ = true;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CLIENT_H
