#ifndef AETHER_RPC_CONTROLLER_H
#define AETHER_RPC_CONTROLLER_H
#pragma once

#include <cstdint>
#include <string>

#include <google/protobuf/service.h>

#include "rpc/RpcProtocol.h"

namespace aether {
namespace rpc {

/// Minimal RpcController: carries failure state between the two ends of a
/// call, plus a machine-readable error code (RpcErrorCode) so callers can
/// branch on failure class (timeout vs transport vs business error)
/// without string matching.
///
/// The base-class virtual SetFailed(string) maps to kInternalError;
/// framework paths use the code-carrying overload (e.g. the channel's
/// deadline timer fails timed-out calls with kTimeout).
///
/// Cancellation is not implemented (Phase 3 candidate).
class RpcController : public google::protobuf::RpcController {
public:
    void Reset() override {
        failed_ = false;
        code_ = kOk;
        message_.clear();
    }

    bool Failed() const override { return failed_; }

    std::string ErrorText() const override { return message_; }

    /// RpcErrorCode of the failure; kOk unless Failed().
    int32_t ErrorCode() const { return code_; }

    void SetFailed(const std::string& reason) override {
        SetFailed(kInternalError, reason);
    }

    void SetFailed(int32_t code, const std::string& reason) {
        failed_ = true;
        code_ = code;
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
    int32_t code_ = kOk;
    std::string message_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CONTROLLER_H
