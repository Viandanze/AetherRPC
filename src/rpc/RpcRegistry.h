#ifndef AETHER_RPC_REGISTRY_H
#define AETHER_RPC_REGISTRY_H
#pragma once

#include <map>
#include <mutex>
#include <string>

#include "base/noncopyable.h"
#include "zk/ZkClient.h"

namespace aether {
namespace rpc {

/// RpcRegistry: server-side service registration on ZooKeeper.
///
/// Each (service, server address) pair becomes an EPHEMERAL node
///
///     {basePath}/{service_name}/{host:port}
///
/// so when a server dies (process crash, network partition, ...) its
/// session ends and the node disappears - consumers see the server
/// vanish without anyone running a health checker.
///
/// Session healing: a ZK session loss silently deletes ALL of our
/// ephemeral nodes. The registry subscribes to kReconnected and simply
/// re-registers everything it knows about.
class RpcRegistry : noncopyable {
public:
    RpcRegistry(zk::ZkClient* zk, std::string basePath = "/AetherRPC");

    /// Advertise `addr` ("host:port") as a provider of `service`.
    /// Idempotent; returns false on transport failure.
    bool registerService(const std::string& service, const std::string& addr);

    /// Remove our node for `service` (graceful shutdown path; the node
    /// also disappears automatically when the session ends).
    bool unregisterService(const std::string& service);

    std::string nodePath(const std::string& service,
                         const std::string& addr) const;

private:
    void onSessionEvent(zk::SessionEvent event);

    zk::ZkClient* zk_;
    const std::string basePath_;
    std::mutex mtx_;
    std::map<std::string, std::string> registered_;  // service -> addr
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_REGISTRY_H
