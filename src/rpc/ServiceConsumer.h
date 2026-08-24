#ifndef AETHER_RPC_SERVICE_CONSUMER_H
#define AETHER_RPC_SERVICE_CONSUMER_H
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <google/protobuf/message.h>

#include "base/noncopyable.h"
#include "rpc/ConnectionPool.h"
#include "rpc/LoadBalancer.h"
#include "rpc/ServiceDiscovery.h"
#include "zk/ZkClient.h"

class EventLoop;

namespace aether {
namespace rpc {

class RpcClient;

/// ServiceConsumer: the client-side entry point of the distributed
/// phase. Ties together:
///
///   ServiceDiscovery   - which servers exist (live updated via ZK)
///   LoadBalancer       - which server to try next
///   connection pools   - a bounded ConnectionPool per endpoint
///                        (Phase 3): concurrent callers spread over up
///                        to `poolSizePerEndpoint` multiplexed
///                        connections; idle ones are recycled
///
/// Call flow of syncCall():
///   endpoints -> LB pick -> pooled lease -> call.
///   On failure (connect error / transport error / timeout) the lease
///   is marked unhealthy (its connection is dropped, not recycled) and
///   the call is retried on a DIFFERENT endpoint (up to kMaxAttempts),
///   which gives basic failover for free: kill a server mid-run and
///   calls continue on the rest.
///
/// Retry semantics: a timed-out call may already have executed on the
/// server, so a retry makes syncCall at-least-once. Idempotent
/// services are safe; strict at-most-once would need request
/// de-duplication on the server side (Phase 3 territory).
///
/// syncCall must be invoked from a regular thread (not the connection
/// IO threads), same rule as RpcClient::syncCall.
class ServiceConsumer : noncopyable {
public:
    /// Default per-endpoint pool size.
    static constexpr size_t kDefaultPoolSize = 4;

    /// `loop` backs all pooled client connections. `poolSizePerEndpoint`
    /// bounds how many multiplexed connections are kept per endpoint.
    ServiceConsumer(EventLoop* loop, zk::ZkClient* zk,
                    LoadBalancerPtr lb, std::string basePath = "/AetherRPC",
                    size_t poolSizePerEndpoint = kDefaultPoolSize);
    ~ServiceConsumer();

    ServiceConsumer(const ServiceConsumer&) = delete;
    ServiceConsumer& operator=(const ServiceConsumer&) = delete;

    /// Synchronous call against any live endpoint of `service`.
    ///
    /// `timeoutMs` is a TOTAL budget covering connect + call across ALL
    /// failover attempts: the caller gets an answer (or a "timeout") no
    /// later than roughly `timeoutMs` after the call started, instead of
    /// kMaxAttempts x timeoutMs. Each attempt (including its connect)
    /// draws from the remaining budget; retries stop once it runs dry.
    bool syncCall(const std::string& service, const std::string& method,
                  const google::protobuf::Message& request,
                  google::protobuf::Message* response,
                  int timeoutMs = 3000, std::string* error = nullptr);

    /// Current endpoint list of `service` (from discovery).
    std::vector<std::string> endpoints(const std::string& service) const {
        return discovery_.endpoints(service);
    }

private:
    static constexpr int kMaxAttempts = 3;
    /// Connect draws from the remaining budget but never more than this
    /// (a dead endpoint must not eat the whole budget before failover).
    static constexpr int kConnectBudgetMs = 1000;

    /// Get-or-create the pool for "host:port". The reference stays
    /// valid for the consumer's lifetime (pools are never erased
    /// individually; they all die in the destructor).
    ConnectionPool& poolFor(const std::string& addr);

    EventLoop* loop_;
    zk::ZkClient* zk_;
    LoadBalancerPtr lb_;
    ServiceDiscovery discovery_;
    const size_t poolSize_;

    std::mutex poolMtx_;
    std::map<std::string, std::unique_ptr<ConnectionPool>> pools_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_SERVICE_CONSUMER_H
