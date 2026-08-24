// Phase 3 end-to-end tests: per-call deadlines (sync + async stub),
// heartbeat keepalive vs server idle timeout, dead-peer detection, the
// business-thread pool, the non-blocking connector with automatic
// reconnect, and the per-endpoint connection pool.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <google/protobuf/service.h>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "net/TcpServer.h"
#include "rpc/ConnectionPool.h"
#include "rpc/RpcChannel.h"
#include "rpc/RpcClient.h"
#include "rpc/RpcController.h"
#include "rpc/RpcProtocol.h"
#include "rpc/RpcServer.h"
#include "test_framework.h"
#include "echo.pb.h"

using namespace aether::rpc;
using namespace std::chrono_literals;

namespace {

const uint16_t kPortTimeout    = 18931;
const uint16_t kPortAsync      = 18932;
const uint16_t kPortKeepalive  = 18933;
const uint16_t kPortIdle       = 18934;
const uint16_t kPortDeadPeer   = 18935;
const uint16_t kPortWorkers    = 18936;
const uint16_t kPortReconnect  = 18937;
const uint16_t kPortPool       = 18938;

class FunctionClosure : public google::protobuf::Closure {
public:
    explicit FunctionClosure(std::function<void()> fn) : fn_(std::move(fn)) {}
    void Run() override {
        if (fn_) fn_();
        delete this;
    }
private:
    std::function<void()> fn_;
};

/// Echo handler that sleeps before replying: exercises deadlines and
/// worker-pool parallelism.
class SlowEchoServiceImpl final : public ::aether::rpc::example::EchoService {
public:
    explicit SlowEchoServiceImpl(int delayMs) : delayMs_(delayMs) {}
    void Echo(google::protobuf::RpcController*,
              const ::aether::rpc::example::EchoRequest* request,
              ::aether::rpc::example::EchoResponse* response,
              google::protobuf::Closure* done) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs_));
        response->set_message(request->message());
        if (done != nullptr) done->Run();
    }
private:
    int delayMs_;
};

class EchoServiceImpl final : public ::aether::rpc::example::EchoService {
public:
    void Echo(google::protobuf::RpcController*,
              const ::aether::rpc::example::EchoRequest* request,
              ::aether::rpc::example::EchoResponse* response,
              google::protobuf::Closure* done) override {
        response->set_message(request->message());
        if (done != nullptr) done->Run();
    }
};

/// Poll `pred` every 10ms until true or `timeoutMs` elapses.
bool waitFor(const std::function<bool()>& pred, int timeoutMs) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

/// Destroy `server` on its own loop and wait: the documented TcpServer
/// destruction contract. A main-thread destruction races in-flight
/// close callbacks (ASan-verified heap-use-after-free - the window is
/// narrow, but real).
void destroyServerOnLoop(EventLoop* loop,
                         std::unique_ptr<RpcServer>& server) {
    std::promise<void> gone;
    loop->runInLoop([&] {
        if (server) {
            server->stop();
            server.reset();
        }
        gone.set_value();
    });
    gone.get_future().wait();
}

/// Connect with retries (the server may still be binding its port).
bool connectWithRetry(RpcClient* client) {
    for (int i = 0; i < 200; ++i) {
        if (client->connect()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

}  // namespace

void test_sync_call_timeout() {
    TEST_SUITE("Timeout: sync call hits the channel deadline");

    SlowEchoServiceImpl slow(300);
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortTimeout), "timeout-server");
    server->registerService(&slow);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortTimeout));
    ASSERT_TRUE(connectWithRetry(&client));

    example::EchoRequest req;
    req.set_message("deadline");
    example::EchoResponse resp;
    std::string error;

    auto t0 = std::chrono::steady_clock::now();
    bool ok = client.syncCall("EchoService", "Echo", req, &resp, 150, &error);
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0)
            .count();
    ASSERT_FALSE(ok);
    ASSERT_CONTAINS(error, "timeout");
    // The 150ms deadline must beat the 300ms handler; 450ms allows for
    // timer/scheduling slack without waiting the full handler time.
    ASSERT_TRUE(elapsed < 450);

    // Plenty of budget: the call succeeds.
    ok = client.syncCall("EchoService", "Echo", req, &resp, 2000, &error);
    ASSERT_TRUE(ok);
    ASSERT_STREQ(resp.message(), "deadline");

    destroyServerOnLoop(serverLoop, server);
}

void test_async_stub_timeout() {
    TEST_SUITE("Timeout: async stub deadline + late reply discarded");

    SlowEchoServiceImpl slow(300);
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortAsync), "async-timeout-server");
    server->registerService(&slow);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortAsync));
    ASSERT_TRUE(connectWithRetry(&client));

    // Stub-driven calls carry no explicit timeout: the channel-wide
    // default deadline covers them.
    auto* ch = static_cast<RpcChannel*>(client.channel());
    ch->setDefaultTimeoutMs(150);

    example::EchoService::Stub stub(ch);
    example::EchoRequest req;
    req.set_message("async-deadline");
    example::EchoResponse resp;
    RpcController controller;
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    stub.Echo(&controller, &req, &resp,
              new FunctionClosure([promise]() { promise->set_value(); }));

    ASSERT_TRUE(future.wait_for(2s) == std::future_status::ready);
    ASSERT_TRUE(controller.Failed());
    ASSERT_EQ(controller.ErrorCode(), kTimeout);
    ASSERT_CONTAINS(controller.ErrorText(), "timeout");

    // The late reply (handler finishes at ~300ms) must be discarded
    // without touching the detached response/controller. The ASan build
    // guards the memory-safety half of this assertion.
    std::this_thread::sleep_for(500ms);
    ASSERT_STREQ(resp.message(), "");

    destroyServerOnLoop(serverLoop, server);
}

void test_heartbeat_keeps_idle_connection_alive() {
    TEST_SUITE("Heartbeat: keepalive beats server idle timeout");

    EchoServiceImpl echo;
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortKeepalive), "keepalive-server");
    server->registerService(&echo);
    server->setIdleTimeout(1);  // reap after 1s of silence
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortKeepalive));
    ASSERT_TRUE(connectWithRetry(&client));
    client.enableHeartbeat(0.3, 3);

    // Idle timeout is 1s; heartbeats every 300ms keep the connection
    // alive through 3s of RPC silence.
    std::this_thread::sleep_for(3s);

    example::EchoRequest req;
    req.set_message("still-alive");
    example::EchoResponse resp;
    std::string error;
    ASSERT_TRUE(client.syncCall("EchoService", "Echo", req, &resp, 2000,
                                &error));
    ASSERT_STREQ(resp.message(), "still-alive");
    ASSERT_EQ(server->connectionCount(), 1);

    destroyServerOnLoop(serverLoop, server);
}

void test_idle_timeout_without_heartbeat() {
    TEST_SUITE("Idle timeout: silent connection is reaped");

    EchoServiceImpl echo;
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortIdle), "idle-server");
    server->registerService(&echo);
    server->setIdleTimeout(1);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortIdle));
    ASSERT_TRUE(connectWithRetry(&client));

    example::EchoRequest req;
    req.set_message("first");
    example::EchoResponse resp;
    ASSERT_TRUE(client.syncCall("EchoService", "Echo", req, &resp, 2000,
                                nullptr));

    // No heartbeat: after ~1-2s of silence the server reaps the
    // connection; the client observes the close.
    ASSERT_TRUE(waitFor([&] { return server->connectionCount() == 0; },
                        4000));
    ASSERT_TRUE(waitFor([&] { return !client.connected(); }, 1000));

    destroyServerOnLoop(serverLoop, server);
}

void test_dead_peer_detection() {
    TEST_SUITE("Heartbeat: unanswered pings close the connection");

    // A raw TcpServer with no RPC layer: heartbeat frames are never
    // answered (black hole).
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    TcpServer rawServer(serverLoop, InetAddress(kPortDeadPeer),
                        "black-hole");
    serverLoop->runInLoop([&] { rawServer.start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortDeadPeer));
    ASSERT_TRUE(connectWithRetry(&client));

    // 200ms interval, 2 misses allowed: detection within ~0.6-1s.
    client.enableHeartbeat(0.2, 2);
    ASSERT_TRUE(waitFor([&] { return !client.connected(); }, 2000));

    // Teardown on the loop (same destruction contract, raw TcpServer).
    std::promise<void> gone;
    serverLoop->runInLoop([&] {
        rawServer.stopAccepting();
        rawServer.forceCloseAll();
        gone.set_value();
    });
    gone.get_future().wait();
}

void test_worker_pool_concurrency() {
    TEST_SUITE("Business thread pool: slow handlers run in parallel");

    SlowEchoServiceImpl slow(300);
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortWorkers), "worker-server");
    server->registerService(&slow);
    server->setWorkerThreads(4);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortWorkers));
    ASSERT_TRUE(connectWithRetry(&client));

    // 8 concurrent calls over one multiplexed connection. Serialized on
    // the IO thread they would take >= 8 x 300ms = 2400ms; with 4
    // workers ~600ms. 1500ms separates the two regimes under sandbox
    // jitter.
    constexpr int kCalls = 8;
    std::vector<std::thread> threads;
    std::atomic<int> okCount{0};
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kCalls; ++i) {
        threads.emplace_back([&, i]() {
            example::EchoRequest req;
            req.set_message("call-" + std::to_string(i));
            example::EchoResponse resp;
            if (client.syncCall("EchoService", "Echo", req, &resp, 5000)) {
                if (resp.message() == "call-" + std::to_string(i)) {
                    okCount.fetch_add(1);
                }
            }
        });
    }
    for (auto& t : threads) t.join();
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0)
            .count();

    ASSERT_EQ(okCount.load(), kCalls);
    ASSERT_TRUE(elapsed < 1500);

    destroyServerOnLoop(serverLoop, server);
}

void test_connector_reconnect() {
    TEST_SUITE("Connector: auto-reconnect after server restart");

    EchoServiceImpl echo;
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortReconnect), "reconnect-server-1");
    server->registerService(&echo);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kPortReconnect));
    client.enableRetry();
    ASSERT_TRUE(connectWithRetry(&client));

    example::EchoRequest req;
    req.set_message("round-one");
    example::EchoResponse resp;
    std::string error;
    ASSERT_TRUE(client.syncCall("EchoService", "Echo", req, &resp, 2000,
                                &error));
    ASSERT_STREQ(resp.message(), "round-one");

    // Kill the server. stop()+destroy must run ON the server loop (the
    // documented teardown contract): destroying a TcpServer with live
    // connections from another thread races the loop's close callbacks.
    // This mirrors a real server process dying.
    std::promise<void> dead;
    serverLoop->runInLoop([&] {
        server->stop();
        server.reset();
        dead.set_value();
    });
    dead.get_future().wait();
    ASSERT_TRUE(waitFor([&] { return !client.connected(); }, 3000));

    // While disconnected, calls fail fast instead of hanging.
    ASSERT_FALSE(client.syncCall("EchoService", "Echo", req, &resp, 2000,
                                 &error));
    ASSERT_CONTAINS(error, "not connected");

    // Bring a fresh server up on the same port. The client reconnects
    // on its own (exponential backoff) and calls work again through the
    // SAME channel/stub pointer - no re-bind needed on the caller side.
    auto server2 = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kPortReconnect), "reconnect-server-2");
    server2->registerService(&echo);
    serverLoop->runInLoop([&] { server2->start(); });

    ASSERT_TRUE(waitFor([&] { return client.connected(); }, 15000));

    req.set_message("round-two");
    resp.Clear();
    error.clear();
    ASSERT_TRUE(client.syncCall("EchoService", "Echo", req, &resp, 3000,
                                &error));
    ASSERT_STREQ(resp.message(), "round-two");
    ASSERT_EQ(server2->connectionCount(), 1);

    // Explicit teardown of server2, same contract as above; the client
    // notices the drop and starts retrying (the client destructor stops
    // the retry loop).
    std::promise<void> dead2;
    serverLoop->runInLoop([&] {
        server2->stop();
        server2.reset();
        dead2.set_value();
    });
    dead2.get_future().wait();
    ASSERT_TRUE(waitFor([&] { return !client.connected(); }, 3000));
}

void test_connection_pool() {
    TEST_SUITE("Pool: bounded connections serve concurrent callers");
    EchoServiceImpl echo;
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(serverLoop,
                                              InetAddress(kPortPool),
                                              "pool-server");
    server->registerService(&echo);
    serverLoop->runInLoop([&] { server->start(); });
    ASSERT_TRUE(waitFor([&] { return server->listening(); }, 3000));

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();

    const size_t kMax = 2;
    ConnectionPool pool(clientLoop, InetAddress("127.0.0.1", kPortPool),
                        "pool-test", kMax);

    // 6 caller threads x 5 calls each: the pool must serve all 30
    // calls while never opening more than kMax connections to the
    // endpoint (verified server-side).
    const int kThreads = 6;
    const int kCallsEach = 5;
    std::atomic<int> okCount{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < kCallsEach; ++i) {
                ConnectionPool::Lease lease = pool.acquire(2000);
                if (!lease) continue;
                example::EchoRequest req;
                req.set_message("pool-" + std::to_string(t) + "-" +
                                std::to_string(i));
                example::EchoResponse resp;
                if (lease->syncCall("EchoService", "Echo", req, &resp, 2000)) {
                    if (resp.message() == req.message()) okCount.fetch_add(1);
                } else {
                    lease.markUnhealthy();
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    ASSERT_EQ(okCount.load(), kThreads * kCallsEach);
    // Cap enforced: the server never saw more than kMax live
    // connections, and by now all leases are back idle.
    ASSERT_TRUE(server->connectionCount() <= static_cast<int>(kMax));
    ASSERT_EQ(server->connectionCount(), static_cast<int>(kMax));
    ASSERT_EQ(pool.liveCount(), kMax);
    ASSERT_EQ(pool.idleCount(), kMax);

    // Connections are RECYCLED: reuse the idle set for a sequential
    // burst - no new connections should appear server-side.
    for (int i = 0; i < 8; ++i) {
        ConnectionPool::Lease lease = pool.acquire(2000);
        ASSERT_TRUE(static_cast<bool>(lease));
        example::EchoRequest req;
        req.set_message("recycle");
        example::EchoResponse resp;
        ASSERT_TRUE(lease->syncCall("EchoService", "Echo", req, &resp, 2000));
    }
    ASSERT_EQ(server->connectionCount(), static_cast<int>(kMax));
    ASSERT_EQ(pool.liveCount(), kMax);

    TEST_SUITE("Pool: unhealthy lease is dropped, not recycled");
    // A killed connection returned as unhealthy must be discarded; the
    // next acquire transparently dials a fresh one.
    ConnectionPool::Lease lease = pool.acquire(2000);
    ASSERT_TRUE(static_cast<bool>(lease));
    lease.markUnhealthy();
    lease = ConnectionPool::Lease();  // release the broken connection

    ConnectionPool::Lease fresh = pool.acquire(2000);
    ASSERT_TRUE(static_cast<bool>(fresh));
    example::EchoRequest req;
    req.set_message("fresh");
    example::EchoResponse resp;
    ASSERT_TRUE(fresh->syncCall("EchoService", "Echo", req, &resp, 2000));
    ASSERT_STREQ(resp.message(), "fresh");
    // We dropped the broken one and are holding the survivor: live is
    // idle(0) + this lease(1).
    ASSERT_EQ(pool.liveCount(), 1u);

    // Releasing the healthy lease and acquiring again refills to cap
    // as demand rises: first acquire recycles the idle connection,
    // second (idle empty, below cap) dials a fresh one.
    fresh = ConnectionPool::Lease();
    ConnectionPool::Lease l1 = pool.acquire(2000);
    ConnectionPool::Lease l2 = pool.acquire(2000);
    ASSERT_TRUE(static_cast<bool>(l1));
    ASSERT_TRUE(static_cast<bool>(l2));
    ASSERT_EQ(pool.liveCount(), kMax);

    TEST_SUITE("Pool: connect failure surfaces as an empty lease");
    ConnectionPool deadPool(clientLoop, InetAddress("127.0.0.1", kPortPool + 1),
                            "pool-dead", kMax);
    ConnectionPool::Lease none = deadPool.acquire(500);
    ASSERT_FALSE(static_cast<bool>(none));
    ASSERT_EQ(deadPool.liveCount(), 0u);

    // Teardown on the server loop (destruction contract); the pools
    // die at scope exit while their IO loop still lives.
    destroyServerOnLoop(serverLoop, server);
}

int main() {
    test_sync_call_timeout();
    test_async_stub_timeout();
    test_heartbeat_keeps_idle_connection_alive();
    test_idle_timeout_without_heartbeat();
    test_dead_peer_detection();
    test_worker_pool_concurrency();
    test_connector_reconnect();
    test_connection_pool();
    TEST_SUMMARY();
}
