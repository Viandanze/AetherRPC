#include "rpc/RpcRegistry.h"

namespace aether {
namespace rpc {

RpcRegistry::RpcRegistry(zk::ZkClient* zk, std::string basePath)
    : zk_(zk), basePath_(std::move(basePath)) {
    zk_->addSessionCallback([this](zk::SessionEvent ev) {
        onSessionEvent(ev);
    });
}

std::string RpcRegistry::nodePath(const std::string& service,
                                  const std::string& addr) const {
    return basePath_ + "/" + service + "/" + addr;
}

bool RpcRegistry::registerService(const std::string& service,
                                  const std::string& addr) {
    if (!zk_->createEphemeral(nodePath(service, addr), addr)) return false;
    std::lock_guard<std::mutex> lock(mtx_);
    registered_[service] = addr;
    return true;
}

bool RpcRegistry::unregisterService(const std::string& service) {
    std::string addr;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = registered_.find(service);
        if (it == registered_.end()) return true;
        addr = it->second;
    }
    if (!zk_->remove(nodePath(service, addr))) return false;
    std::lock_guard<std::mutex> lock(mtx_);
    registered_.erase(service);
    return true;
}

void RpcRegistry::onSessionEvent(zk::SessionEvent event) {
    if (event != zk::SessionEvent::kReconnected) return;

    // New session: all previous ephemeral nodes are gone. Re-register
    // everything we still believe we serve.
    std::map<std::string, std::string> snapshot;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        snapshot = registered_;
    }
    for (const auto& [service, addr] : snapshot) {
        zk_->createEphemeral(nodePath(service, addr), addr);
    }
}

}  // namespace rpc
}  // namespace aether
