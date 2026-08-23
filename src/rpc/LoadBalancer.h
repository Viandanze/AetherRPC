#ifndef AETHER_RPC_LOAD_BALANCER_H
#define AETHER_RPC_LOAD_BALANCER_H
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

namespace aether {
namespace rpc {

/// LoadBalancer: picks one endpoint from a candidate list.
///
/// The candidate list is supplied per call (the caller owns the
/// discovery snapshot), so a balancer instance is just a *policy* -
/// cheap to share across services and threads.
/// Returns "" for an empty candidate list.
class LoadBalancer {
public:
    virtual ~LoadBalancer() = default;
    virtual std::string select(const std::vector<std::string>& candidates) = 0;
};

using LoadBalancerPtr = std::unique_ptr<LoadBalancer>;

/// Even rotation through the candidate list. Deterministic, which makes
/// tests trivial; a good default.
class RoundRobinLoadBalancer : public LoadBalancer {
public:
    std::string select(const std::vector<std::string>& candidates) override {
        if (candidates.empty()) return "";
        // Overflow of the counter is fine: modulo still distributes.
        const uint64_t index =
            next_.fetch_add(1, std::memory_order_relaxed);
        return candidates[index % candidates.size()];
    }

private:
    std::atomic<uint64_t> next_{0};
};

/// Uniform random choice (thread-local engine, no contention).
class RandomLoadBalancer : public LoadBalancer {
public:
    std::string select(const std::vector<std::string>& candidates) override {
        if (candidates.empty()) return "";
        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
        return candidates[dist(rng)];
    }
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_LOAD_BALANCER_H
