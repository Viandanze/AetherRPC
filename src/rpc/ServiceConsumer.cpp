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
                                 LoadBalancerPtr lb, std::string basePath)
    : loop_(loop),
      zk_(zk),
      lb_(std::move(lb)),
      discovery_(zk, std::move(basePath)) {}

ServiceConsumer::~ServiceConsumer() {
    // Evict connections while their IO loop is still alive. RpcClient's
    // dtor performs a synchronous disconnect handshake with the loop.
    std::map<std::string, std::shared_ptr<RpcClient>> conns;
    {
        std::lock_guard<std::mutex> lock(connMtx_);
        conns.swap(conns_);
    }
    conns.clear();
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

    std::set<std::string> tried;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        std::vector<std::string> candidates =
            exclude(discovery_.endpoints(service), tried);
        if (candidates.empty()) {
            if (error) *error = "no live endpoints for service: " + service;
            return false;
        }

        std::string addr = lb_->select(candidates);
        if (addr.empty()) continue;

        std::shared_ptr<RpcClient> client = connectionFor(addr);
        if (client == nullptr) {
            tried.insert(addr);
            continue;
        }

        if (client->syncCall(service, method, request, response, timeoutMs,
                             error)) {
            return true;
        }

        // Connection-level failure: evict and try another endpoint.
        dropConnection(addr);
        tried.insert(addr);
    }
    if (error && error->empty()) *error = "all attempts failed";
    return false;
}

std::shared_ptr<RpcClient> ServiceConsumer::connectionFor(
    const std::string& addr) {
    {
        std::lock_guard<std::mutex> lock(connMtx_);
        auto it = conns_.find(addr);
        if (it != conns_.end() && it->second->connected()) return it->second;
    }

    InetAddress serverAddr = parseAddr(addr);
    auto client = std::make_shared<RpcClient>(loop_, serverAddr,
                                              "consumer-" + addr);
    if (!client->connect()) return nullptr;

    std::lock_guard<std::mutex> lock(connMtx_);
    // First caller to connect wins; ours goes to the cache (and gets
    // disconnected on destruction - acceptable for this phase).
    auto it = conns_.find(addr);
    if (it != conns_.end() && it->second->connected()) return it->second;
    conns_[addr] = client;
    return client;
}

void ServiceConsumer::dropConnection(const std::string& addr) {
    std::shared_ptr<RpcClient> client;
    {
        std::lock_guard<std::mutex> lock(connMtx_);
        auto it = conns_.find(addr);
        if (it == conns_.end()) return;
        client = it->second;
        conns_.erase(it);
    }
    // RpcClient dtor runs the synchronous disconnect handshake outside
    // connMtx_ (it waits on the IO loop thread).
}

}  // namespace rpc
}  // namespace aether
