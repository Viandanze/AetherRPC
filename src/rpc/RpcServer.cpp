#include "rpc/RpcServer.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "base/Logger.h"
#include "net/TcpConnection.h"
#include "rpc/Callback.h"
#include "rpc/RpcCodec.h"
#include "rpc_common.pb.h"

namespace aether {
namespace rpc {

RpcServer::RpcServer(EventLoop* loop, const InetAddress& listenAddr,
                     const std::string& name)
    : server_(loop, listenAddr, name) {
    server_.setMessageCallback(
        [this](const std::shared_ptr<TcpConnection>& conn, Buffer* buf) {
            onMessage(conn, buf);
        });
}

void RpcServer::start() {
    if (workerThreads_ > 0) {
        workers_ = std::make_unique<base::ThreadPool>("rpc-workers");
        workers_->start(workerThreads_);
    }
    server_.start();
}

void RpcServer::registerService(google::protobuf::Service* service) {
    const google::protobuf::ServiceDescriptor* desc = service->GetDescriptor();
    services_[desc->name()] = service;
    LOG_INFO("RpcServer: registered service %s (%d methods)",
             desc->name().c_str(), desc->method_count());
}

void RpcServer::onMessage(const std::shared_ptr<TcpConnection>& conn,
                          Buffer* buf) {
    while (true) {  // drain all complete frames (sticky packages)
        RpcHeader header;
        std::string body;
        bool err = false;
        if (!RpcCodec::tryParse(buf, &header, &body, &err)) {
            if (err) {
                LOG_ERROR("RpcServer: protocol violation from %s, closing",
                          conn->peerAddr().toIpPort().c_str());
                conn->forceClose();
            }
            return;  // wait for more bytes (half package)
        }
        if (header.msgType == kHeartbeat) {
            // Keepalive: echo the frame back (pong). The inbound frame
            // already refreshed the server-side idle timer in
            // TcpConnection::handleRead.
            std::string pong;
            RpcCodec::appendFrame(&pong, kHeartbeat, header.requestId,
                                  std::string());
            conn->send(pong);
            continue;
        }
        if (header.msgType != kRequest) {
            LOG_WARN("RpcServer: unexpected msgType %u from %s, closing",
                     header.msgType, conn->peerAddr().toIpPort().c_str());
            conn->forceClose();
            return;
        }
        handleRequest(conn, header.requestId, body);
    }
}

void RpcServer::handleRequest(const std::shared_ptr<TcpConnection>& conn,
                              uint32_t requestId, const std::string& body) {
    RpcRequest req;
    if (!req.ParseFromString(body)) {
        sendError(conn, requestId, kParseError, "failed to parse RpcRequest");
        return;
    }

    auto it = services_.find(req.service_name());
    if (it == services_.end()) {
        sendError(conn, requestId, kServiceNotFound,
                  "service not found: " + req.service_name());
        return;
    }
    google::protobuf::Service* service = it->second;

    const google::protobuf::MethodDescriptor* method =
        service->GetDescriptor()->FindMethodByName(req.method_name());
    if (method == nullptr) {
        sendError(conn, requestId, kMethodNotFound,
                  "method not found: " + req.method_name());
        return;
    }

    google::protobuf::Message* request =
        service->GetRequestPrototype(method).New();
    google::protobuf::Message* response =
        service->GetResponsePrototype(method).New();
    if (!request->ParseFromString(req.args())) {
        delete request;
        delete response;
        sendError(conn, requestId, kParseError,
                  "failed to parse method args");
        return;
    }

    // The done-callback owns `response` and deletes it after
    // serialization. Handlers must run done before returning
    // (synchronous-handler contract).
    google::protobuf::Closure* done = new FunctionClosure(
        [this, conn, requestId, response]() {
            onCallDone(conn, requestId, response);
        });
    if (workers_) {
        // Business-thread pool: the handler (and `request`) leave the IO
        // thread. conn->send() inside onCallDone is thread-safe (it hops
        // back to the connection's IO loop), so slow handlers no longer
        // block other calls multiplexed on the same connection.
        workers_->run([service, method, request, response, done]() {
            service->CallMethod(method, nullptr, request, response, done);
            delete request;
        });
    } else {
        // Runs inline on the IO thread (Phase 1 semantics).
        service->CallMethod(method, nullptr, request, response, done);
        delete request;
    }
}

void RpcServer::onCallDone(const std::shared_ptr<TcpConnection>& conn,
                           uint32_t requestId,
                           google::protobuf::Message* response) {
    std::unique_ptr<google::protobuf::Message> guard(response);
    std::string result;
    if (!response->SerializeToString(&result)) {
        sendError(conn, requestId, kInternalError,
                  "failed to serialize response");
        return;
    }
    RpcResponse resp;
    resp.set_error_code(kOk);
    resp.set_result(std::move(result));
    sendResponse(conn, requestId, resp);
}

void RpcServer::sendResponse(const std::shared_ptr<TcpConnection>& conn,
                             uint32_t requestId, const RpcResponse& resp) {
    std::string body;
    resp.SerializeToString(&body);
    std::string frame;
    RpcCodec::appendFrame(&frame, kResponse, requestId, body);
    conn->send(frame);
}

void RpcServer::sendError(const std::shared_ptr<TcpConnection>& conn,
                          uint32_t requestId, int32_t code,
                          const std::string& msg) {
    RpcResponse resp;
    resp.set_error_code(code);
    resp.set_error_msg(msg);
    sendResponse(conn, requestId, resp);
}

}  // namespace rpc
}  // namespace aether
