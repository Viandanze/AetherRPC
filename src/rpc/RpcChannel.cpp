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

RpcChannel::~RpcChannel() {
    // Do not touch slot pointers here: callers that abandoned their
    // calls are long gone. Dropping the slots is enough; pending
    // closures that never ran are simply released with the channel.
    pending_.clear();
}

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

RpcChannel::CallSlotPtr RpcChannel::sendRequest(
    const std::string& serviceName, const std::string& methodName,
    const std::string& serializedArgs, google::protobuf::Message* response,
    google::protobuf::RpcController* controller,
    google::protobuf::Closure* done) {
    uint32_t id = nextId_.fetch_add(1);
    if (id == 0) id = nextId_.fetch_add(1);  // skip the 0 sentinel on wrap

    CallSlotPtr slot = std::make_shared<CallSlot>(id);
    slot->response = response;
    slot->controller = controller;
    slot->done = done;

    // Register the pending entry and write the frame on the IO thread so
    // pending_ stays single-threaded. Capture the slot by value: it owns
    // the call context, and its lock guards the raw pointers inside once
    // published (the publish itself is ordered by the functor queue).
    conn_->getLoop()->runInLoop(
        [this, slot, serviceName, methodName, serializedArgs]() {
            sendRequestInLoop(slot, serviceName, methodName, serializedArgs);
        });
    return slot;
}

void RpcChannel::cancelRequest(const CallSlotPtr& slot) {
    if (slot == nullptr) return;

    // Detach the caller-owned pointers under the slot lock. Every
    // IO-thread path checks (and nulls) them under this same lock, so
    // once this critical section completes, an in-flight completion has
    // necessarily finished touching the caller's objects.
    google::protobuf::Closure* done = nullptr;
    {
        std::lock_guard<std::mutex> guard(slot->mtx);
        slot->response = nullptr;
        slot->controller = nullptr;
        done = slot->done;
        slot->done = nullptr;
    }
    if (done != nullptr) done->Run();  // releases the abandoned promise

    // Hygiene: drop the map entry on the IO thread so abandoned entries
    // do not accumulate while the connection stays healthy. Bounded risk:
    // only the IO thread reads pending_, so a dead loop can never touch
    // the entry either - it dies with the channel.
    if (conn_ != nullptr && conn_->getLoop() != nullptr) {
        conn_->getLoop()->queueInLoop(
            [this, slot] { pending_.erase(slot->id); });
    }
}

void RpcChannel::sendRequestInLoop(const CallSlotPtr& slot,
                                   const std::string& serviceName,
                                   const std::string& methodName,
                                   const std::string& serializedArgs) {
    RpcRequest req;
    req.set_service_name(serviceName);
    req.set_method_name(methodName);
    req.set_args(serializedArgs);
    std::string body;
    req.SerializeToString(&body);

    std::string frame;
    RpcCodec::appendFrame(&frame, kRequest, slot->id, body);

    pending_[slot->id] = slot;
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
            // Late reply for an already-cancelled call: nothing to do.
            LOG_WARN("RpcChannel: reply for unknown id=%u (timed out?)",
                     header.requestId);
            continue;
        }
        CallSlotPtr slot = it->second;
        pending_.erase(it);

        google::protobuf::Closure* done = nullptr;
        {
            std::lock_guard<std::mutex> guard(slot->mtx);
            done = slot->done;
            slot->done = nullptr;
            if (slot->response == nullptr) {
                // Caller timed out and detached its objects already.
                LOG_WARN("RpcChannel: late reply for cancelled id=%u, "
                         "discarded",
                         header.requestId);
            } else if (resp.error_code() != kOk) {
                if (slot->controller != nullptr) {
                    slot->controller->SetFailed(
                        "rpc error " + std::to_string(resp.error_code()) +
                        ": " + resp.error_msg());
                }
                slot->response = nullptr;
                slot->controller = nullptr;
            } else if (!slot->response->ParseFromString(resp.result())) {
                if (slot->controller != nullptr) {
                    slot->controller->SetFailed(
                        "failed to parse business response");
                }
                slot->response = nullptr;
                slot->controller = nullptr;
            } else {
                slot->response = nullptr;
                slot->controller = nullptr;
            }
        }
        // Run user code outside the lock.
        if (done != nullptr) done->Run();
    }
}

void RpcChannel::onDisconnect() {
    for (auto& kv : pending_) {
        const CallSlotPtr& slot = kv.second;
        google::protobuf::Closure* done = nullptr;
        {
            std::lock_guard<std::mutex> guard(slot->mtx);
            done = slot->done;
            slot->done = nullptr;
            if (slot->response != nullptr) {  // not abandoned
                if (slot->controller != nullptr) {
                    slot->controller->SetFailed("connection closed");
                }
                slot->response = nullptr;
                slot->controller = nullptr;
            }
        }
        if (done != nullptr) done->Run();
    }
    pending_.clear();
}

}  // namespace rpc
}  // namespace aether
