#ifndef AETHER_RPC_SERVER_H
#define AETHER_RPC_SERVER_H
#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <google/protobuf/service.h>

#include "base/noncopyable.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"
#include "rpc_common.pb.h"

class EventLoop;

namespace aether {
namespace rpc {

/// RpcServer: a protobuf service dispatcher on top of the Aether TCP stack.
///
/// Usage:
///   RpcServer server(loop, InetAddress(8080));
///   server.registerService(new EchoServiceImpl);  // raw or unique ptr, see note
///   server.setThreadNum(4);
///   server.start();
///
/// Dispatch flow per REQUEST frame:
///   decode frame -> parse RpcRequest -> look up Service by name ->
///   find MethodDescriptor -> build request/response prototypes ->
///   Service::CallMethod (runs inline on the IO thread in Phase 1;
///   a dedicated business-thread pool is planned for Phase 3) ->
///   done-callback serializes the result and writes the RESPONSE frame.
///
/// Note: registered services are NOT owned by RpcServer; keep them alive
/// for the lifetime of the server.
class RpcServer : noncopyable {
public:
    RpcServer(EventLoop* loop, const InetAddress& listenAddr,
              const std::string& name = "aether-rpc-server");

    void start();

    /// True once the underlying listener is up (safe to connect).
    bool listening() const { return server_.listening(); }

    /// Graceful teardown, must be invoked on the server loop thread
    /// (e.g. via loop->runInLoop): stop accepting, then force-close all
    /// remaining connections so their channels are removed from the
    /// poller before the EventLoop is destroyed.
    void stop() {
        server_.stopAccepting();
        server_.forceCloseAll();
    }

    void setThreadNum(int n) { server_.setThreadNum(n); }
    void setMaxConnections(int n) { server_.setMaxConnections(n); }
    void setIdleTimeout(int seconds) { server_.setIdleTimeout(seconds); }

    void registerService(google::protobuf::Service* service);

private:
    void onMessage(const std::shared_ptr<TcpConnection>& conn, Buffer* buf);
    void handleRequest(const std::shared_ptr<TcpConnection>& conn,
                       uint32_t requestId, const std::string& body);

    // Serialize `resp` (framework-level RpcResponse) into a RESPONSE frame.
    void sendResponse(const std::shared_ptr<TcpConnection>& conn,
                      uint32_t requestId, const RpcResponse& resp);

    // done-callback of Service::CallMethod: wrap the business response and
    // take ownership of `response`.
    void onCallDone(const std::shared_ptr<TcpConnection>& conn,
                    uint32_t requestId, google::protobuf::Message* response);

    void sendError(const std::shared_ptr<TcpConnection>& conn,
                   uint32_t requestId, int32_t code, const std::string& msg);

    TcpServer server_;
    std::unordered_map<std::string, google::protobuf::Service*> services_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_SERVER_H
