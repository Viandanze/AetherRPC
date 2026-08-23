// Integration tests for the ZooKeeper-based service registry, discovery
// and consumer failover. Requires a reachable ZooKeeper on
// AETHER_ZK (default 127.0.0.1:2181); the suite self-skips otherwise.
//
// Covered:
//   * registry -> discovery basic visibility (2 endpoints)
//   * live updates: third server registers, one unregisters
//   * watch callback fires on list changes
//   * full stack: 3 real RpcServers, RoundRobin spread, failover retry
//     when a server dies but its ZK node lingers, discovery shrink
//     after unregister, calls keep succeeding throughout.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "rpc/LoadBalancer.h"
#include "rpc/RpcRegistry.h"
#include "rpc/RpcServer.h"
#include "rpc/ServiceConsumer.h"
#include "rpc/ServiceDiscovery.h"
#include "test_framework.h"
#include "zk/ZkClient.h"
#include "echo.pb.h"

using namespace aether::rpc;
using aether::zk::ZkClient;

namespace {

const char* kZkServers = "127.0.0.1:2181";
const char* kEnvZk = "AETHER_ZK";

bool waitUntil(int timeoutMs, const std::function<bool()>& pred) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

std::string zkAddr() {
    const char* env = ::getenv(kEnvZk);
    return env != nullptr && *env != '\0' ? env : kZkServers;
}

bool zkReachable() {
    ZkClient probe(zkAddr());
    return probe.connect(2000);
}

/// Best-effort recursive delete of a test base path (leftovers from a
/// crashed previous run must not poison this one).
void cleanupTree(ZkClient& zk, const std::string& base) {
    std::vector<std::string> children;
    if (!zk.getChildren(base, &children)) {
        zk.remove(base);
        return;
    }
    for (const auto& c : children) {
        std::vector<std::string> leaves;
        if (zk.getChildren(base + "/" + c, &leaves)) {
            for (const auto& l : leaves)
                zk.remove(base + "/" + c + "/" + l);
        }
        zk.remove(base + "/" + c);
    }
    zk.remove(base);
}

class CountingMathServiceImpl final
    : public ::aether::rpc::example::MathService {
public:
    void Add(google::protobuf::RpcController*,
             const ::aether::rpc::example::AddRequest* request,
             ::aether::rpc::example::AddResponse* response,
             google::protobuf::Closure* done) override {
        response->set_sum(request->a() + request->b());
        ++count_;
        if (done != nullptr) done->Run();
    }

    int count() const { return count_.load(); }

private:
    std::atomic<int> count_{0};
};

/// One in-process "server process": its own loop thread, RPC server,
/// counting service and ZK registration.
struct ServerNode {
    ServerNode(ZkClient* zk, const std::string& base, uint16_t port)
        : loop(loopThread.startLoop()),
          server(std::make_unique<RpcServer>(
              loop, InetAddress(port),
              "zk-test-server-" + std::to_string(port))),
          registry(zk, base),
          addr("127.0.0.1:" + std::to_string(port)) {}

    void start() {
        server->registerService(&math);
        loop->runInLoop([this] { server->start(); });
        // Wait until the listener is up (learned the hard way in Phase 1).
        waitUntil(3000, [this] { return server->listening(); });
        registry.registerService("MathService", addr);
    }

    void stopServer() {
        loop->runInLoop([this] { server->stop(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        server.reset();
        loop->quit();
    }

    EventLoopThread loopThread;
    EventLoop* loop;
    std::unique_ptr<RpcServer> server;
    RpcRegistry registry;
    CountingMathServiceImpl math;
    const std::string addr;
};

}  // namespace

void test_registry_and_discovery() {
    TEST_SUITE("registry + discovery visibility");
    ZkClient zk(zkAddr());
    ASSERT_TRUE(zk.connect());
    const std::string base = "/AetherRPC-test-reg-" +
                             std::to_string(static_cast<long>(::getpid()));
    cleanupTree(zk, base);

    RpcRegistry r1(&zk, base), r2(&zk, base);
    ASSERT_TRUE(r1.registerService("Svc", "127.0.0.1:19101"));
    ASSERT_TRUE(r2.registerService("Svc", "127.0.0.1:19102"));

    ServiceDiscovery discovery(&zk, base);
    ASSERT_TRUE(discovery.watchService("Svc"));
    ASSERT_TRUE(waitUntil(5000, [&] {
        return discovery.endpoints("Svc").size() == 2;
    }));
    auto eps = discovery.endpoints("Svc");
    ASSERT_EQ(eps.size(), 2u);
    ASSERT_STREQ(eps[0], "127.0.0.1:19101");  // discovery sorts
    ASSERT_STREQ(eps[1], "127.0.0.1:19102");

    TEST_SUITE("discovery live updates");
    std::atomic<int> callbackCount{0};
    ServiceDiscovery watching(&zk, base);
    ASSERT_TRUE(watching.watchService(
        "Svc", [&](const std::string&, const std::vector<std::string>&) {
            ++callbackCount;
        }));
    ASSERT_EQ(watching.endpoints("Svc").size(), 2u);

    RpcRegistry r3(&zk, base);
    ASSERT_TRUE(r3.registerService("Svc", "127.0.0.1:19103"));
    ASSERT_TRUE(waitUntil(5000, [&] {
        return watching.endpoints("Svc").size() == 3;
    }));
    ASSERT_EQ(watching.endpoints("Svc").size(), 3u);

    ASSERT_TRUE(r2.unregisterService("Svc"));
    ASSERT_TRUE(waitUntil(5000, [&] {
        return watching.endpoints("Svc").size() == 2;
    }));
    ASSERT_EQ(watching.endpoints("Svc").size(), 2u);
    ASSERT_TRUE(waitUntil(5000, [&] { return callbackCount.load() >= 2; }));

    cleanupTree(zk, base);
    zk.shutdown();
}

void test_distributed_e2e_failover() {
    TEST_SUITE("3-server distributed calls + LB spread");
    ZkClient zk(zkAddr());
    ASSERT_TRUE(zk.connect());
    const std::string base = "/AetherRPC-test-e2e-" +
                             std::to_string(static_cast<long>(::getpid()));
    cleanupTree(zk, base);

    const uint16_t ports[3] = {19201, 19202, 19203};
    std::vector<std::unique_ptr<ServerNode>> nodes;
    for (int i = 0; i < 3; ++i)
        nodes.emplace_back(
            std::make_unique<ServerNode>(&zk, base, ports[i]));
    for (auto& n : nodes) n->start();

    EventLoopThread clientLoopThread;
    EventLoop* clientLoop = clientLoopThread.startLoop();
    ServiceConsumer consumer(clientLoop, &zk,
                             std::make_unique<RoundRobinLoadBalancer>(),
                             base);

    std::string error;
    bool allOk = true;
    for (int i = 0; i < 9; ++i) {
        ::aether::rpc::example::AddRequest req;
        req.set_a(i);
        req.set_b(100);
        ::aether::rpc::example::AddResponse resp;
        if (!consumer.syncCall("MathService", "Add", req, &resp, 3000,
                               &error)) {
            printf("  call %d failed: %s\n", i, error.c_str());
            allOk = false;
        } else if (resp.sum() != i + 100) {
            allOk = false;
        }
    }
    ASSERT_TRUE(allOk);
    // RoundRobin over 3 sorted endpoints, 9 calls: exactly 3 each.
    ASSERT_EQ(nodes[0]->math.count(), 3);
    ASSERT_EQ(nodes[1]->math.count(), 3);
    ASSERT_EQ(nodes[2]->math.count(), 3);

    TEST_SUITE("failover: server dies, ZK node lingers");
    // Kill node 2's RPC server but leave its ZK registration alive
    // (simulates a half-dead process before session expiry). The
    // consumer must retry on another endpoint and stay green.
    nodes[1]->loop->runInLoop([&] { nodes[1]->server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    error.clear();
    int failoverOk = 0;
    for (int i = 0; i < 4; ++i) {
        ::aether::rpc::example::AddRequest req;
        req.set_a(i);
        req.set_b(200);
        ::aether::rpc::example::AddResponse resp;
        if (consumer.syncCall("MathService", "Add", req, &resp, 3000,
                              &error)) {
            ++failoverOk;
        }
    }
    ASSERT_EQ(failoverOk, 4);
    // The dead node served nothing extra; survivors shared the 4 calls.
    ASSERT_EQ(nodes[1]->math.count(), 3);
    ASSERT_EQ(nodes[0]->math.count() + nodes[2]->math.count(), 3 + 3 + 4);

    TEST_SUITE("discovery shrink after unregister");
    // Nodes die for real now (registry unregisters, session-end would
    // do the same automatically).
    nodes[1]->registry.unregisterService("MathService");
    ASSERT_TRUE(waitUntil(5000, [&] {
        return consumer.endpoints("MathService").size() == 2;
    }));
    ASSERT_EQ(consumer.endpoints("MathService").size(), 2u);

    error.clear();
    int afterShrinkOk = 0;
    for (int i = 0; i < 3; ++i) {
        ::aether::rpc::example::AddRequest req;
        req.set_a(i);
        req.set_b(300);
        ::aether::rpc::example::AddResponse resp;
        if (consumer.syncCall("MathService", "Add", req, &resp, 3000,
                              &error)) {
            ++afterShrinkOk;
        }
    }
    ASSERT_EQ(afterShrinkOk, 3);

    // Teardown order matters:
    //   * The client loop must stay ALIVE until the consumer (and its
    //     cached connections) is destroyed - conns.clear() runs in
    //     ~ServiceConsumer at scope exit, and ~RpcClient's disconnect
    //     handshake queues work on that loop. ~EventLoopThread (declared
    //     before the consumer, destroyed after it) quits and joins the
    //     loop at the right time - do NOT quit it here.
    //   * Then servers go down on their own loops (server.reset()
    //     before loop->quit(), so connections close while their loop
    //     still lives).
    //   * Then ZK (the discovery worker already tolerates a closed
    //     handle: its sync calls return false immediately).
    for (auto& n : nodes) n->stopServer();
    cleanupTree(zk, base);
    zk.shutdown();
}

int main() {
    if (!zkReachable()) {
        printf("── SKIP: no ZooKeeper at %s (set %s to override) ──\n",
               zkAddr().c_str(), kEnvZk);
        return 0;
    }
    test_registry_and_discovery();
    test_distributed_e2e_failover();
    TEST_SUMMARY();
}
