#ifndef AETHER_RPC_CHANNEL_H
#define AETHER_RPC_CHANNEL_H
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <google/protobuf/service.h>

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
    /// waiting.
    CallSlotPtr sendRequest(const std::string& serviceName,
                            const std::string& methodName,
                            const std::string& serializedArgs,
                            google::protobuf::Message* response,
                            google::protobuf::RpcController* controller,
                            google::protobuf::Closure* done);

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
                           const std::string& serializedArgs);

    std::shared_ptr<TcpConnection> conn_;
    std::atomic<uint32_t> nextId_{1};  // 0 is never used
    // Only touched on the connection's IO thread.
    std::unordered_map<uint32_t, CallSlotPtr> pending_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CHANNEL_H
