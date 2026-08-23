#include "rpc/RpcChannel.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "base/Logger.h"
#include "net/Buffer.h"
#include "net/TcpConnection.h"
#include "net/EventLoop.h"
#include "rpc/RpcCodec.h"
#include "rpc/RpcProtocol.h"
#include "rpc_common.pb.h"

namespace aether {
namespace rpc {

RpcChannel::RpcChannel(const std::shared_ptr<TcpConnection>& conn)
    : conn_(conn) {}

RpcChannel::~RpcChannel() = default;

void RpcChannel::CallMethod(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    google::protobuf::Closure* done) {
    std::string args;
    if (request != nullptr && !request->SerializeToString(&args)) {
        if (controller != nullptr) {
            controller->SetFailed("failed to serialize request");
        }
        if (done != nullptr) done->Run();
        return;
    }
    sendRequest(method->service()->name(), method->name(), args, response,
                controller, done);
}

void RpcChannel::sendRequest(const std::string& serviceName,
                             const std::string& methodName,
                             const std::string& serializedArgs,
                             google::protobuf::Message* response,
                             google::protobuf::RpcController* controller,
                             google::protobuf::Closure* done) {
    uint32_t id = nextId_.fetch_add(1);
    // Register the pending entry and write the frame on the IO thread so
    // pending_ is single-threaded. Capture by value: runInLoop may execute
    // asynchronously after this function has returned.
    conn_->getLoop()->runInLoop([this, id, serviceName, methodName,
                                 serializedArgs, response, controller,
                                 done]() {
        sendRequestInLoop(id, serviceName, methodName, serializedArgs,
                          response, controller, done);
    });
}

void RpcChannel::sendRequestInLoop(
    uint32_t id, const std::string& serviceName,
    const std::string& methodName, const std::string& serializedArgs,
    google::protobuf::Message* response,
    google::protobuf::RpcController* controller,
    google::protobuf::Closure* done) {
    RpcRequest req;
    req.set_service_name(serviceName);
    req.set_method_name(methodName);
    req.set_args(serializedArgs);
    std::string body;
    req.SerializeToString(&body);

    std::string frame;
    RpcCodec::appendFrame(&frame, kRequest, id, body);

    pending_[id] = PendingCall{response, controller, done};
    conn_->send(frame);
}

void RpcChannel::onMessage(Buffer* buf) {
    while (true) {  // drain all complete frames
        RpcHeader header;
        std::string body;
        bool err = false;
        if (!RpcCodec::tryParse(buf, &header, &body, &err)) {
            return;  // wait for more bytes (half package)
        }
        if (header.msgType != kResponse) {
            LOG_WARN("RpcChannel: unexpected msgType %u, ignoring",
                     header.msgType);
            continue;
        }

        RpcResponse resp;
        if (!resp.ParseFromString(body)) {
            LOG_ERROR("RpcChannel: failed to parse RpcResponse, id=%u",
                      header.requestId);
            continue;
        }

        auto it = pending_.find(header.requestId);
        if (it == pending_.end()) {
            // Late reply for an already-timed-out call: nothing to do.
            LOG_WARN("RpcChannel: reply for unknown id=%u (timed out?)",
                     header.requestId);
            continue;
        }
        PendingCall call = it->second;
        pending_.erase(it);

        if (resp.error_code() == kOk) {
            if (call.response != nullptr &&
                !call.response->ParseFromString(resp.result())) {
                if (call.controller != nullptr) {
                    call.controller->SetFailed(
                        "failed to parse business response");
                }
            }
        } else {
            if (call.controller != nullptr) {
                call.controller->SetFailed(
                    "rpc error " + std::to_string(resp.error_code()) + ": " +
                    resp.error_msg());
            }
        }
        if (call.done != nullptr) call.done->Run();
    }
}

void RpcChannel::onDisconnect() {
    for (auto& kv : pending_) {
        PendingCall& call = kv.second;
        if (call.controller != nullptr) {
            call.controller->SetFailed("connection closed");
        }
        if (call.done != nullptr) call.done->Run();
    }
    pending_.clear();
}

}  // namespace rpc
}  // namespace aether
