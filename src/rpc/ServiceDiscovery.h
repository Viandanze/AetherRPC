#ifndef AETHER_RPC_SERVICE_DISCOVERY_H
#define AETHER_RPC_SERVICE_DISCOVERY_H
#pragma once

#include <atomic>
#include <functional>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "base/noncopyable.h"
#include "zk/ZkClient.h"

namespace aether {
namespace rpc {

/// ServiceDiscovery: client-side endpoint discovery with live updates.
///
/// Watched services are listed under {basePath}/{service}; each child
/// node name IS an endpoint address ("host:port").
///
/// Update pipeline (ZK 3.4 watchers are one-shot):
///   ZK IO thread: child event -> wake discovery worker (nothing else!)
///   worker thread: debounce 50ms -> getChildrenAndWatch (re-arms the
///   watcher atomically with the fetch) -> if the set changed, publish
///   it and fire the user callback.
///
/// Because the re-fetch and the watcher installation happen in one
/// call, any change between two fetches is guaranteed to fire the new
/// watcher - no lost-update window.
///
/// Session loss clears ALL watchers; on kReconnected the worker
/// re-fetches every watched service.
class ServiceDiscovery : noncopyable {
public:
    using ListCallback =
        std::function<void(const std::string& service,
                           const std::vector<std::string>& endpoints)>;

    ServiceDiscovery(zk::ZkClient* zk, std::string basePath = "/AetherRPC");
    ~ServiceDiscovery();

    /// Start watching `service`. Fetches the current endpoint list
    /// synchronously (so endpoints() is meaningful right after) and
    /// keeps it updated afterwards. `cb` (optional) fires from the
    /// worker thread whenever the list changes.
    bool watchService(const std::string& service, ListCallback cb = nullptr);

    /// Snapshot of the current endpoints for `service`.
    std::vector<std::string> endpoints(const std::string& service) const;

    std::string servicePath(const std::string& service) const {
        return basePath_ + "/" + service;
    }

private:
    void startWorker();
    void workerLoop();
    void refreshAll();
    void onChildEvent(const std::string& path);
    void onSessionEvent(zk::SessionEvent event);

    zk::ZkClient* zk_;
    const std::string basePath_;

    mutable std::mutex tableMtx_;
    std::map<std::string, std::vector<std::string>> table_;
    std::map<std::string, ListCallback> callbacks_;

    std::thread worker_;
    std::mutex wakeMtx_;
    std::condition_variable wakeCv_;
    bool dirty_ = false;          // guarded by wakeMtx_
    std::atomic<bool> running_{false};
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_SERVICE_DISCOVERY_H
