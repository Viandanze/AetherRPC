#include "rpc/RpcChannel.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "base/Logger.h"
#include "net/Buffer.h"
#include "net/TcpConnection.h"
#include "net/EventLoop.h"
#include "rpc/RpcCodec.h"
#include "rpc/RpcController.h"
#include "rpc/RpcProtocol.h"
#include "rpc_common.pb.h"

namespace aether {
namespace rpc {

namespace {

/// Fail a controller with the timeout code. Controllers may be the
/// framework's own RpcController (code-carrying overload) or a foreign
/// implementation (fall back to the plain virtual).
void failWithTimeout(google::protobuf::RpcController* controller) {
    if (controller == nullptr) return;
    if (auto* ours =
            dynamic_cast<aether::rpc::RpcController*>(controller)) {
        ours->SetFailed(aether::rpc::kTimeout, "timeout");
    } else {
        controller->SetFailed("timeout");
    }
}

}  // namespace

RpcChannel::RpcChannel() = default;

RpcChannel::RpcChannel(const std::shared_ptr<TcpConnection>& conn)
    : conn_(conn) {}

void RpcChannel::attachConnection(const std::shared_ptr<TcpConnection>& conn) {
    std::lock_guard<std::mutex> guard(connMtx_);
    conn_ = conn;
}

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
                controller, done, defaultTimeoutMs_);
}

RpcChannel::CallSlotPtr RpcChannel::sendRequest(
    const std::string& serviceName, const std::string& methodName,
    const std::string& serializedArgs, google::protobuf::Message* response,
    google::protobuf::RpcController* controller,
    google::protobuf::Closure* done, int timeoutMs) {
    uint32_t id = nextId_.fetch_add(1);
    if (id == 0) id = nextId_.fetch_add(1);  // skip the 0 sentinel on wrap

    CallSlotPtr slot = std::make_shared<CallSlot>(id);
    slot->response = response;
    slot->controller = controller;
    slot->done = done;

    // Snapshot the connection under the mutex: onDisconnect may release
    // it concurrently on the IO thread. No connection (or a connection
    // without a loop) means the channel is detached - fail the call
    // synchronously instead of queueing onto a dead loop.
    std::shared_ptr<TcpConnection> conn = snapshotConn();
    if (conn == nullptr || conn->getLoop() == nullptr) {
        failSlot(slot, "connection closed");
        return slot;
    }

    // Register the pending entry and write the frame on the IO thread so
    // pending_ stays single-threaded. Capture the slot by value: it owns
    // the call context, and its lock guards the raw pointers inside once
    // published (the publish itself is ordered by the functor queue).
    // The channel is captured by shared_ptr so queued work cannot
    // outlive a channel being replaced by a reconnect.
    conn->getLoop()->runInLoop(
        [self = shared_from_this(), slot, serviceName, methodName,
         serializedArgs, timeoutMs]() {
            self->sendRequestInLoop(slot, serviceName, methodName,
                                    serializedArgs, timeoutMs);
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

    // Hygiene: drop the map entry (and any armed deadline timer) on the
    // IO thread so abandoned entries do not accumulate while the
    // connection stays healthy. Bounded risk: only the IO thread reads
    // pending_, so a dead loop can never touch the entry either - it
    // dies with the channel.
    std::shared_ptr<TcpConnection> conn = snapshotConn();
    if (conn != nullptr && conn->getLoop() != nullptr) {
        conn->getLoop()->queueInLoop(
            [self = shared_from_this(), slot]() {
                if (slot->deadlineArmed) {
                    std::shared_ptr<TcpConnection> c = self->snapshotConn();
                    if (c != nullptr && c->getLoop() != nullptr) {
                        c->getLoop()->cancel(slot->deadline);
                        slot->deadlineArmed = false;
                    }
                }
                self->pending_.erase(slot->id);
            });
    }
}

void RpcChannel::sendRequestInLoop(const CallSlotPtr& slot,
                                   const std::string& serviceName,
                                   const std::string& methodName,
                                   const std::string& serializedArgs,
                                   int timeoutMs) {
    // The connection may have dropped between the caller-thread
    // sendRequest() and this functor running on the IO thread. Fail the
    // call instead of writing into a dead socket.
    std::shared_ptr<TcpConnection> conn = snapshotConn();
    if (conn == nullptr || !conn->connected()) {
        failSlot(slot, "connection closed");
        return;
    }

    RpcRequest req;
    req.set_service_name(serviceName);
    req.set_method_name(methodName);
    req.set_args(serializedArgs);
    std::string body;
    req.SerializeToString(&body);

    std::string frame;
    RpcCodec::appendFrame(&frame, kRequest, slot->id, body);

    pending_[slot->id] = slot;
    conn->send(frame);

    // Arm the per-call deadline after registration. Everything here runs
    // on the IO thread, so the timer can never race the registration.
    if (timeoutMs > 0) {
        slot->deadline = conn->getLoop()->runAfter(
            timeoutMs / 1000.0,
            [self = shared_from_this(), slot]() { self->onDeadline(slot); });
        slot->deadlineArmed = true;
    }
}

void RpcChannel::onDeadline(const CallSlotPtr& slot) {
    // IO thread. A call that already completed (reply arrived, caller
    // cancelled, connection dropped) is no longer in pending_.
    auto it = pending_.find(slot->id);
    if (it == pending_.end()) return;
    pending_.erase(it);
    slot->deadlineArmed = false;  // this timer just fired

    google::protobuf::Closure* done = nullptr;
    {
        std::lock_guard<std::mutex> guard(slot->mtx);
        done = slot->done;
        slot->done = nullptr;
        if (slot->response != nullptr) {  // caller still waiting
            failWithTimeout(slot->controller);
            slot->response = nullptr;
            slot->controller = nullptr;
        }
        // response == nullptr: the caller gave up first (cancelRequest);
        // its done was already run there - nothing to do.
    }
    // Run user code outside the lock.
    if (done != nullptr) done->Run();
}

void RpcChannel::onMessage(Buffer* buf) {
    while (true) {  // drain all complete frames
        RpcHeader header;
        std::string body;
        bool err = false;
        if (!RpcCodec::tryParse(buf, &header, &body, &err)) {
            return;  // wait for more bytes (half package)
        }
        if (header.msgType == kHeartbeat) {
            // Pong: no pending call to complete; notify the observer
            // (RpcClient's dead-peer detection resets its miss counter).
            if (onHeartbeat_) onHeartbeat_();
            continue;
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
        if (slot->deadlineArmed) {
            // The reply won the race against the deadline timer; cancel
            // it so it does not fire on a completed call. Both this code
            // and the timer run on the IO thread, so no race.
            std::shared_ptr<TcpConnection> conn = snapshotConn();
            if (conn != nullptr && conn->getLoop() != nullptr) {
                conn->getLoop()->cancel(slot->deadline);
            }
            slot->deadlineArmed = false;
        }

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
    std::shared_ptr<TcpConnection> conn = snapshotConn();
    // Cancel any armed deadline timers first: the slots below are failed
    // right here, so a later timer firing would find nothing in pending_
    // (harmless) but would also keep the slot alive a bit longer.
    for (auto& kv : pending_) {
        const CallSlotPtr& slot = kv.second;
        if (slot->deadlineArmed && conn != nullptr &&
            conn->getLoop() != nullptr) {
            conn->getLoop()->cancel(slot->deadline);
            slot->deadlineArmed = false;
        }
    }
    for (auto& kv : pending_) {
        failSlot(kv.second, "connection closed");
    }
    pending_.clear();

    // Release the connection reference. Until this reset the channel
    // kept the TcpConnection (and therefore its socket fd) alive even
    // after a close - the fd only closed when the RpcClient itself was
    // destroyed, which on the server side looks like a connection that
    // never goes away (idle reaped connections were never removed).
    {
        std::lock_guard<std::mutex> guard(connMtx_);
        conn_.reset();
    }
}

std::shared_ptr<TcpConnection> RpcChannel::snapshotConn() const {
    std::lock_guard<std::mutex> guard(connMtx_);
    return conn_;
}

void RpcChannel::failSlot(const CallSlotPtr& slot, const std::string& reason) {
    google::protobuf::Closure* done = nullptr;
    {
        std::lock_guard<std::mutex> guard(slot->mtx);
        done = slot->done;
        slot->done = nullptr;
        if (slot->response != nullptr) {  // not abandoned
            if (slot->controller != nullptr) {
                slot->controller->SetFailed(reason);
            }
            slot->response = nullptr;
            slot->controller = nullptr;
        }
    }
    // Run user code outside the lock.
    if (done != nullptr) done->Run();
}

}  // namespace rpc
}  // namespace aether
