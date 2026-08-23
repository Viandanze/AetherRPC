#ifndef AETHER_RPC_CHANNEL_H
#define AETHER_RPC_CHANNEL_H
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
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
class RpcChannel : public google::protobuf::RpcChannel,
                   noncopyable {
public:
    explicit RpcChannel(const std::shared_ptr<TcpConnection>& conn);
    ~RpcChannel() override;

    // google::protobuf::RpcChannel interface (used by generated stubs).
    // `done` may be nullptr (fire-and-forget). `response` stays owned by
    // the caller; it is filled in when the reply arrives.
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

    // String-keyed entry point used by RpcClient::syncCall; also handy for
    // hand-rolled callers without a generated stub.
    void sendRequest(const std::string& serviceName,
                     const std::string& methodName,
                     const std::string& serializedArgs,
                     google::protobuf::Message* response,
                     google::protobuf::RpcController* controller,
                     google::protobuf::Closure* done);

    // Must be wired to the connection's message callback; runs on the IO
    // thread, parses RESPONSE frames and completes pending calls.
    void onMessage(Buffer* buf);

    // Must be wired to the connection's close callback; fails all pending
    // calls with kConnectionClosed. Runs on the IO thread.
    void onDisconnect();

private:
    struct PendingCall {
        google::protobuf::Message* response = nullptr;
        google::protobuf::RpcController* controller = nullptr;
        google::protobuf::Closure* done = nullptr;
    };

    void sendRequestInLoop(uint32_t id, const std::string& serviceName,
                           const std::string& methodName,
                           const std::string& serializedArgs,
                           google::protobuf::Message* response,
                           google::protobuf::RpcController* controller,
                           google::protobuf::Closure* done);

    std::shared_ptr<TcpConnection> conn_;
    std::atomic<uint32_t> nextId_{1};  // 0 is never used
    // Only touched on the connection's IO thread.
    std::unordered_map<uint32_t, PendingCall> pending_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CHANNEL_H
