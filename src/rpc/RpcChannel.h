#ifndef AETHER_RPC_CHANNEL_H
#define AETHER_RPC_CHANNEL_H
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <google/protobuf/service.h>

#include "base/TimerId.h"
#include "base/noncopyable.h"

class Buffer;
class TcpConnection;

namespace aether {
namespace rpc {

/// RpcChannel: the client side of the AetherRPC protocol.
///
/// Implements google::protobuf::RpcChannel so generated stubs can call
/// through it directly:
///
///   EchoService::Stub stub(client->channel());
///   stub.Echo(nullptr, &request, &response, done);
///
/// Threading model: the pending-call map lives on the connection's IO
/// thread. sendRequest() may be called from any thread; it hops to the IO
/// thread to register the pending entry and write the frame. Responses
/// arriving out of order are matched by request id.
///
/// Lifetime of caller-owned objects: a pending call references the
/// caller's response/controller until it completes. A caller that gives
/// up waiting (e.g. syncCall on timeout) MUST call cancelRequest() before
/// destroying them; afterwards the IO thread treats the call as
/// abandoned and never dereferences those pointers again.
class RpcChannel : public google::protobuf::RpcChannel,
                   public std::enable_shared_from_this<RpcChannel>,
                   noncopyable {
public:
    explicit RpcChannel(const std::shared_ptr<TcpConnection>& conn);
    ~RpcChannel() override;

    /// Per-call bookkeeping shared between the calling thread and the
    /// connection's IO thread. `response` and `controller` are owned by
    /// the caller; `done` is invoked by the channel exactly once.
    /// mtx guards the three pointers and the abandon transition:
    /// whoever nulls `response` under the lock owns the call outcome.
    struct CallSlot {
        explicit CallSlot(uint32_t callId) : id(callId) {}

        const uint32_t id;  // request id; 0 is never used
        mutable std::mutex mtx;
        google::protobuf::Message* response = nullptr;
        google::protobuf::RpcController* controller = nullptr;
        google::protobuf::Closure* done = nullptr;
        // Per-call deadline bookkeeping. Only the connection's IO thread
        // touches these: the timer is armed when the request is
        // registered and cancelled on every completion path, all of
        // which run on the IO thread.
        TimerId deadline;
        bool deadlineArmed = false;
    };
    using CallSlotPtr = std::shared_ptr<CallSlot>;

    // google::protobuf::RpcChannel interface (used by generated stubs).
    // `done` may be nullptr (fire-and-forget). `response` stays owned by
    // the caller; it is filled in when the reply arrives.
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

    /// String-keyed entry point used by RpcClient::syncCall; also handy
    /// for hand-rolled callers without a generated stub. Returns a
    /// handle that can be passed to cancelRequest() if the caller stops
    /// waiting. `timeoutMs > 0` arms an IO-thread deadline timer that
    /// fails the call with kTimeout ("timeout") even if the caller is
    /// not waiting on it (async/stub callers); `timeoutMs <= 0` means no
    /// deadline.
    CallSlotPtr sendRequest(const std::string& serviceName,
                            const std::string& methodName,
                            const std::string& serializedArgs,
                            google::protobuf::Message* response,
                            google::protobuf::RpcController* controller,
                            google::protobuf::Closure* done,
                            int timeoutMs = 0);

    /// Deadline applied to stub-driven (CallMethod) calls, which have no
    /// explicit timeout parameter. 0 disables deadlines (Phase 1
    /// behaviour). Read on any thread; set before traffic starts.
    void setDefaultTimeoutMs(int ms) { defaultTimeoutMs_ = ms; }

    /// Observer invoked on the IO thread for every HEARTBEAT frame
    /// received (pong). RpcClient wires this for dead-peer detection.
    void setOnHeartbeat(std::function<void()> cb) {
        onHeartbeat_ = std::move(cb);
    }

    /// Called by a caller that stopped waiting (timeout). Detaches the
    /// caller-owned pointers under the slot lock, so any in-flight
    /// completion on the IO thread has necessarily finished touching
    /// them by the time this returns. Safe to call from any thread.
    void cancelRequest(const CallSlotPtr& slot);

    // Must be wired to the connection's message callback; runs on the IO
    // thread, parses RESPONSE frames and completes pending calls.
    void onMessage(Buffer* buf);

    // Must be wired to the connection's close callback; fails all pending
    // calls with kConnectionClosed. Runs on the IO thread.
    void onDisconnect();

private:
    void sendRequestInLoop(const CallSlotPtr& slot,
                           const std::string& serviceName,
                           const std::string& methodName,
                           const std::string& serializedArgs,
                           int timeoutMs);

    /// Deadline timer body (IO thread): fails the call with kTimeout if
    /// it is still pending.
    void onDeadline(const CallSlotPtr& slot);

    /// Thread-safe snapshot of the connection reference. sendRequest /
    /// cancelRequest may run on caller threads while onDisconnect (IO
    /// thread) releases the reference; every use therefore copies it out
    /// under the mutex instead of dereferencing conn_ directly.
    std::shared_ptr<TcpConnection> snapshotConn() const;

    /// Complete a pending/brand-new call with an error and invoke done
    /// exactly once. Caller must not hold slot->mtx. Any thread.
    void failSlot(const CallSlotPtr& slot, const std::string& reason);

    mutable std::mutex connMtx_;
    std::shared_ptr<TcpConnection> conn_;
    std::atomic<uint32_t> nextId_{1};  // 0 is never used
    // Only touched on the connection's IO thread.
    std::unordered_map<uint32_t, CallSlotPtr> pending_;
    int defaultTimeoutMs_ = 0;
    std::function<void()> onHeartbeat_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CHANNEL_H
