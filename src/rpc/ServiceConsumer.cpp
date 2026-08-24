#include "rpc/ServiceConsumer.h"


#include "net/InetAddress.h"
#include "rpc/RpcClient.h"

namespace aether {
namespace rpc {

namespace {

/// "host:port" -> InetAddress (host may be a name or dotted quad).
InetAddress parseAddr(const std::string& addr) {
    auto colon = addr.find_last_of(':');
    std::string host = addr.substr(0, colon);
    uint16_t port =
        static_cast<uint16_t>(::atoi(addr.substr(colon + 1).c_str()));
    return InetAddress(host.c_str(), port);
}

std::vector<std::string> exclude(const std::vector<std::string>& eps,
                                 const std::set<std::string>& tried) {
    std::vector<std::string> out;
    out.reserve(eps.size());
    for (const auto& e : eps) {
        if (tried.count(e) == 0) out.push_back(e);
    }
    return out;
}

}  // namespace

ServiceConsumer::ServiceConsumer(EventLoop* loop, zk::ZkClient* zk,
                                 LoadBalancerPtr lb, std::string basePath,
                                 size_t poolSizePerEndpoint)
    : loop_(loop),
      zk_(zk),
      lb_(std::move(lb)),
      discovery_(zk, std::move(basePath)),
      poolSize_(poolSizePerEndpoint == 0 ? 1 : poolSizePerEndpoint) {}

ServiceConsumer::~ServiceConsumer() {
    // Destroy pools while their IO loop is still alive; ~ConnectionPool
    // runs each pooled client's synchronous disconnect handshake.
    std::map<std::string, std::unique_ptr<ConnectionPool>> pools;
    {
        std::lock_guard<std::mutex> lock(poolMtx_);
        pools.swap(pools_);
    }
    pools.clear();
}

bool ServiceConsumer::syncCall(const std::string& service,
                               const std::string& method,
                               const google::protobuf::Message& request,
                               google::protobuf::Message* response,
                               int timeoutMs, std::string* error) {
    // Lazy watch: first call for a service installs discovery.
    {
        std::vector<std::string> eps = discovery_.endpoints(service);
        if (eps.empty()) {
            if (!discovery_.watchService(service)) {
                if (error) *error = "service discovery failed: " + service;
                return false;
            }
        }
    }

    // Total budget across connect + call for every attempt. A budget of
    // <= 0 means "no deadline" and is passed through to RpcClient.
    const bool bounded = timeoutMs > 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    auto remainingMs = [&]() -> int {
        if (!bounded) return 0;  // unbounded: pass through
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                        deadline - std::chrono::steady_clock::now())
                        .count();
        return static_cast<int>(left);
    };

    std::set<std::string> tried;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        int left = remainingMs();
        if (bounded && left < 50) {
            // Budget exhausted: no point starting another attempt that
            // cannot finish inside the caller's deadline.
            if (error) *error = "timeout";
            return false;
        }

        std::vector<std::string> candidates =
            exclude(discovery_.endpoints(service), tried);
        if (candidates.empty()) {
            if (error) *error = "no live endpoints for service: " + service;
            return false;
        }

        std::string addr = lb_->select(candidates);
        if (addr.empty()) continue;

        int connectMs = bounded ? std::min(left, kConnectBudgetMs) : 0;
        ConnectionPool::Lease lease = poolFor(addr).acquire(connectMs);
        if (!lease) {
            tried.insert(addr);
            continue;
        }

        if (lease->syncCall(service, method, request, response,
                            remainingMs(), error)) {
            return true;  // ~Lease recycles the healthy connection
        }

        // Connection-level failure: drop this connection (not the whole
        // pool - its other connections may be fine) and try another
        // endpoint.
        lease.markUnhealthy();
        tried.insert(addr);
    }
    if (error && error->empty()) *error = "all attempts failed";
    return false;
}

ConnectionPool& ServiceConsumer::poolFor(const std::string& addr) {
    {
        std::lock_guard<std::mutex> lock(poolMtx_);
        auto it = pools_.find(addr);
        if (it != pools_.end()) return *it->second;
    }
    auto pool = std::make_unique<ConnectionPool>(loop_, parseAddr(addr),
                                                 "consumer", poolSize_);
    ConnectionPool& ref = *pool;
    std::lock_guard<std::mutex> lock(poolMtx_);
    // First creator wins; the loser's pool (with no connections yet) is
    // destroyed right away - its dtor is cheap with an empty idle set.
    auto [it2, inserted] = pools_.try_emplace(addr, std::move(pool));
    if (!inserted) return *it2->second;
    return ref;
}

}  // namespace rpc
}  // namespace aether
