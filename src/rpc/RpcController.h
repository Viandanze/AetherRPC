#ifndef AETHER_RPC_CONTROLLER_H
#define AETHER_RPC_CONTROLLER_H
#pragma once

#include <string>

#include <google/protobuf/service.h>

namespace aether {
namespace rpc {

/// Minimal RpcController: carries failure state between the two ends of a
/// call. Cancellation is not implemented (Phase 3 candidate).
class RpcController : public google::protobuf::RpcController {
public:
    void Reset() override {
        failed_ = false;
        message_.clear();
    }

    bool Failed() const override { return failed_; }

    std::string ErrorText() const override { return message_; }

    void SetFailed(const std::string& reason) override {
        failed_ = true;
        message_ = reason;
    }

    // --- cancellation: not supported in Phase 1 ---
    void StartCancel() override {}
    bool IsCanceled() const override { return false; }
    void NotifyOnCancel(google::protobuf::Closure* callback) override {
        (void)callback;
    }

private:
    bool failed_ = false;
    std::string message_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CONTROLLER_H
