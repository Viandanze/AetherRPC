#ifndef AETHER_RPC_SERVICE_CONSUMER_H
#define AETHER_RPC_SERVICE_CONSUMER_H
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include <google/protobuf/message.h>

#include "base/noncopyable.h"
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
///   connection cache   - one RpcClient per endpoint; a connection is
///                        kept and multiplexed across calls (Phase 1
///                        channel semantics)
///
/// Call flow of syncCall():
///   endpoints -> LB pick -> cached (or new) connection -> call.
///   On failure (connect error / transport error / timeout) the
///   connection is dropped and the call is retried on a DIFFERENT
///   endpoint (up to kMaxAttempts), which gives basic failover for
///   free: kill a server mid-run and calls continue on the rest.
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
    /// `loop` backs all cached client connections.
    ServiceConsumer(EventLoop* loop, zk::ZkClient* zk,
                    LoadBalancerPtr lb,
                    std::string basePath = "/AetherRPC");
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

    /// Get-or-create the cached RpcClient for "host:port"; nullptr on
    /// connect failure (the failed entry is evicted).
    std::shared_ptr<RpcClient> connectionFor(const std::string& addr,
                                             int connectTimeoutMs);
    void dropConnection(const std::string& addr);

    EventLoop* loop_;
    zk::ZkClient* zk_;
    LoadBalancerPtr lb_;
    ServiceDiscovery discovery_;

    std::mutex connMtx_;
    std::map<std::string, std::shared_ptr<RpcClient>> conns_;
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_SERVICE_CONSUMER_H
