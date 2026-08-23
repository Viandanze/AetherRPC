#include "rpc/ServiceDiscovery.h"

#include <algorithm>
#include <chrono>

namespace aether {
namespace rpc {

namespace {
constexpr int kDebounceMs = 50;
}

ServiceDiscovery::ServiceDiscovery(zk::ZkClient* zk, std::string basePath)
    : zk_(zk), basePath_(std::move(basePath)) {
    zk_->addChildCallback(
        [this](const std::string& path) { onChildEvent(path); });
    zk_->addSessionCallback([this](zk::SessionEvent ev) {
        onSessionEvent(ev);
    });
}

ServiceDiscovery::~ServiceDiscovery() {
    running_.store(false);
    {
        std::lock_guard<std::mutex> lock(wakeMtx_);
        dirty_ = true;
    }
    wakeCv_.notify_all();
    if (worker_.joinable()) worker_.join();
}

bool ServiceDiscovery::watchService(const std::string& service,
                                     ListCallback cb) {
    // Make sure the parent exists so we can install a watcher at all.
    // (Whoever creates it first - registry or discovery - is fine; the
    // node is persistent and idempotent to create.)
    if (!zk_->createPersistent(servicePath(service))) return false;

    std::vector<std::string> eps;
    if (!zk_->getChildrenAndWatch(servicePath(service), &eps)) return false;
    std::sort(eps.begin(), eps.end());

    {
        std::lock_guard<std::mutex> lock(tableMtx_);
        table_[service] = eps;
        if (cb) callbacks_[service] = std::move(cb);
    }
    startWorker();
    return true;
}

std::vector<std::string> ServiceDiscovery::endpoints(
    const std::string& service) const {
    std::lock_guard<std::mutex> lock(tableMtx_);
    auto it = table_.find(service);
    return it == table_.end() ? std::vector<std::string>() : it->second;
}

void ServiceDiscovery::startWorker() {
    if (running_.exchange(true)) return;  // already running
    worker_ = std::thread([this] { workerLoop(); });
}

void ServiceDiscovery::onChildEvent(const std::string& /*path*/) {
    // Runs on ZK's IO thread: wake the worker, nothing more.
    {
        std::lock_guard<std::mutex> lock(wakeMtx_);
        dirty_ = true;
    }
    wakeCv_.notify_one();
}

void ServiceDiscovery::onSessionEvent(zk::SessionEvent event) {
    if (event == zk::SessionEvent::kReconnected ||
        event == zk::SessionEvent::kReconnecting) {
        // kReconnected: watchers were wiped by the session loss.
        // kReconnecting: cheap to re-check anyway.
        {
            std::lock_guard<std::mutex> lock(wakeMtx_);
            dirty_ = true;
        }
        wakeCv_.notify_one();
    }
}

void ServiceDiscovery::workerLoop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(wakeMtx_);
            wakeCv_.wait(lock, [this] { return dirty_ || !running_.load(); });
            if (!running_.load()) return;
            dirty_ = false;
        }

        // Debounce: ZK often fires child events in bursts (node created,
        // then a sibling's session cleanup, ...). Coalesce them.
        std::this_thread::sleep_for(std::chrono::milliseconds(kDebounceMs));

        refreshAll();
    }
}

void ServiceDiscovery::refreshAll() {
    std::vector<std::string> services;
    {
        std::lock_guard<std::mutex> lock(tableMtx_);
        for (const auto& kv : table_) services.push_back(kv.first);
    }

    for (const auto& service : services) {
        std::vector<std::string> eps;
        if (!zk_->getChildrenAndWatch(servicePath(service), &eps)) {
            // Node vanished (service fully unregistered?) - treat as
            // empty rather than keeping a stale list.
            eps.clear();
        }
        std::sort(eps.begin(), eps.end());

        bool changed = false;
        ListCallback cb;
        {
            std::lock_guard<std::mutex> lock(tableMtx_);
            auto it = table_.find(service);
            if (it != table_.end()) {
                if (it->second != eps) {
                    it->second = eps;
                    changed = true;
                }
                auto cit = callbacks_.find(service);
                if (cit != callbacks_.end()) cb = cit->second;
            }
        }
        if (changed && cb) cb(service, eps);
    }
}

}  // namespace rpc
}  // namespace aether
