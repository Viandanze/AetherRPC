#ifndef AETHER_RPC_CALLBACK_H
#define AETHER_RPC_CALLBACK_H
#pragma once

#include <functional>

#include <google/protobuf/service.h>

namespace aether {
namespace rpc {

/// Self-deleting Closure wrapping a std::function. protobuf's own
/// NewCallback cannot bind capturing lambdas; this one can.
class FunctionClosure : public google::protobuf::Closure {
public:
    explicit FunctionClosure(std::function<void()> fn)
        : fn_(std::move(fn)) {}

    void Run() override {
        if (fn_) fn_();
        delete this;
    }

private:
    std::function<void()> fn_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CALLBACK_H
