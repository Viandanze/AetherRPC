// End-to-end tests for AetherRPC: real TCP loop over 127.0.0.1 with the
// Aether network stack on both ends.
// Tests: sync call happy paths, unknown service/method error paths,
// async stub call, concurrent calls over one multiplexed connection,
// large payloads.
#include <atomic>
#include <memory>
#include <chrono>
#include <cstdio>
#include <functional>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include <google/protobuf/service.h>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "rpc/RpcClient.h"
#include "rpc/RpcChannel.h"
#include "rpc/RpcController.h"
#include "rpc/RpcServer.h"
#include "test_framework.h"
#include "echo.pb.h"

using namespace aether::rpc;

namespace {

const uint16_t kTestPort = 18923;

// --- service implementations ----------------------------------------------

class EchoServiceImpl final : public ::aether::rpc::example::EchoService {
public:
    void Echo(google::protobuf::RpcController*,
              const ::aether::rpc::example::EchoRequest* request,
              ::aether::rpc::example::EchoResponse* response,
              google::protobuf::Closure* done) override {
        std::string out;
        int repeat = request->repeat() > 0 ? request->repeat() : 1;
        for (int i = 0; i < repeat; ++i) out += request->message();
        response->set_message(std::move(out));
        if (done != nullptr) done->Run();
    }
};

class MathServiceImpl final : public ::aether::rpc::example::MathService {
public:
    void Add(google::protobuf::RpcController*,
             const ::aether::rpc::example::AddRequest* request,
             ::aether::rpc::example::AddResponse* response,
             google::protobuf::Closure* done) override {
        response->set_sum(request->a() + request->b());
        if (done != nullptr) done->Run();
    }
};

// self-deleting closure wrapping a std::function (for async stub tests)
class FunctionClosure : public google::protobuf::Closure {
public:
    explicit FunctionClosure(std::function<void()> fn)
        : fn_(std::move(fn)) {}
    void Run() override {
        if (fn_) fn_();
        delete this;
    }

private:
    std::function<void()> fn_;
};

EchoServiceImpl g_echo;
MathServiceImpl g_math;

}  // namespace

void test_sync_calls() {
    TEST_SUITE("Sync Calls (happy path)");

    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(serverLoop, InetAddress(kTestPort), "e2e-server");
    server->registerService(&g_echo);
    server->registerService(&g_math);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kTestPort));
    bool connected = false;
    for (int i = 0; i < 200 && !connected; ++i) {
        connected = client.connect();
        if (!connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ASSERT_TRUE(connected);
    std::string error;

    {   // echo
        example::EchoRequest req;
        req.set_message("ping ");
        req.set_repeat(4);
        example::EchoResponse resp;
        bool ok = client.syncCall("EchoService", "Echo", req, &resp, 3000,
                                  &error);
        ASSERT_TRUE(ok);
        ASSERT_STREQ(resp.message(), "ping ping ping ping ");
    }

    {   // add
        example::AddRequest req;
        req.set_a(137);
        req.set_b(289);
        example::AddResponse resp;
        bool ok = client.syncCall("MathService", "Add", req, &resp, 3000,
                                  &error);
        ASSERT_TRUE(ok);
        ASSERT_EQ(resp.sum(), 426);
    }

    client.disconnect();
    // give both sides time to process the FIN before tearing the loops down
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // graceful server shutdown must run on the server loop thread itself
    serverLoop->runInLoop([&] { server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Acceptor's destructor touches the poller: the server must die
    // while its loop thread (and EventLoop object) is still alive.
    server.reset();
    serverLoop->quit();
}

void test_error_paths() {
    TEST_SUITE("Error Paths");

    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(serverLoop, InetAddress(kTestPort), "e2e-server");
    server->registerService(&g_echo);
    server->registerService(&g_math);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kTestPort));
    bool connected = false;
    for (int i = 0; i < 200 && !connected; ++i) {
        connected = client.connect();
        if (!connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ASSERT_TRUE(connected);
    std::string error;

    {   // unknown service
        example::AddRequest req;
        example::AddResponse resp;
        bool ok = client.syncCall("NopeService", "Add", req, &resp, 3000,
                                  &error);
        ASSERT_FALSE(ok);
        ASSERT_TRUE(error.find("service not found") != std::string::npos);
    }

    {   // unknown method
        example::AddRequest req;
        example::AddResponse resp;
        bool ok = client.syncCall("MathService", "Multiply", req, &resp, 3000,
                                  &error);
        ASSERT_FALSE(ok);
        ASSERT_TRUE(error.find("method not found") != std::string::npos);
    }

    {   // proto3 lenient parsing: an EchoRequest byte stream parses as an
        // empty AddRequest (unknown fields skipped, missing fields default)
        example::EchoRequest wrongType;
        wrongType.set_message("garbage");
        example::AddResponse resp;
        bool ok = client.syncCall("MathService", "Add", wrongType, &resp,
                                  3000, &error);
        ASSERT_TRUE(ok);
        ASSERT_EQ(resp.sum(), 0);  // a=0, b=0
    }

    {   // corrupt wire bytes: server reports a parse error
        example::AddResponse resp;
        std::promise<void> promise;
        auto future = promise.get_future();
        RpcController controller;
        static_cast<aether::rpc::RpcChannel*>(client.channel())->sendRequest(
            "MathService", "Add", std::string("\xff\xff\xff\xff\xff", 5),
            &resp, &controller,
            new FunctionClosure([&promise] { promise.set_value(); }));
        future.wait_for(std::chrono::seconds(3));
        ASSERT_TRUE(controller.Failed());
        ASSERT_TRUE(controller.ErrorText().find("parse") != std::string::npos);
    }

    client.disconnect();
    // give both sides time to process the FIN before tearing the loops down
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // graceful server shutdown must run on the server loop thread itself
    serverLoop->runInLoop([&] { server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Acceptor's destructor touches the poller: the server must die
    // while its loop thread (and EventLoop object) is still alive.
    server.reset();
    serverLoop->quit();
}

void test_async_stub() {
    TEST_SUITE("Async Stub Call");

    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(serverLoop, InetAddress(kTestPort), "e2e-server");
    server->registerService(&g_echo);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kTestPort));
    bool connected = false;
    for (int i = 0; i < 200 && !connected; ++i) {
        connected = client.connect();
        if (!connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ASSERT_TRUE(connected);

    example::EchoService::Stub stub(client.channel());
    example::EchoRequest req;
    req.set_message("async ");
    req.set_repeat(2);
    example::EchoResponse resp;

    std::promise<void> promise;
    std::future<void> future = promise.get_future();
    RpcController controller;
    stub.Echo(&controller, &req, &resp,
              new FunctionClosure([&promise] { promise.set_value(); }));

    ASSERT_EQ(future.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    ASSERT_FALSE(controller.Failed());
    ASSERT_STREQ(resp.message(), "async async ");

    client.disconnect();
    // give both sides time to process the FIN before tearing the loops down
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // graceful server shutdown must run on the server loop thread itself
    serverLoop->runInLoop([&] { server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Acceptor's destructor touches the poller: the server must die
    // while its loop thread (and EventLoop object) is still alive.
    server.reset();
    serverLoop->quit();
}

void test_concurrent_calls() {
    TEST_SUITE("Concurrent Calls Over One Connection");

    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(serverLoop, InetAddress(kTestPort), "e2e-server");
    server->registerService(&g_math);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kTestPort));
    bool connected = false;
    for (int i = 0; i < 200 && !connected; ++i) {
        connected = client.connect();
        if (!connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ASSERT_TRUE(connected);

    // 8 worker threads x 25 calls each, all multiplexed over one TCP
    // connection; request ids pair the interleaved replies correctly.
    const int kThreads = 8;
    const int kCallsPerThread = 25;
    std::atomic<int> failures{0};

    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < kCallsPerThread; ++i) {
                example::AddRequest req;
                req.set_a(t * 1000 + i);
                req.set_b(1);
                example::AddResponse resp;
                std::string err;
                if (!client.syncCall("MathService", "Add", req, &resp, 5000,
                                     &err) ||
                    resp.sum() != t * 1000 + i + 1) {
                    ++failures;
                }
            }
        });
    }
    for (auto& w : workers) w.join();

    ASSERT_EQ(failures.load(), 0);

    client.disconnect();
    // give both sides time to process the FIN before tearing the loops down
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // graceful server shutdown must run on the server loop thread itself
    serverLoop->runInLoop([&] { server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Acceptor's destructor touches the poller: the server must die
    // while its loop thread (and EventLoop object) is still alive.
    server.reset();
    serverLoop->quit();
}

void test_large_payload() {
    TEST_SUITE("Large Payload (1 MB)");

    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    auto server = std::make_unique<RpcServer>(serverLoop, InetAddress(kTestPort), "e2e-server");
    server->registerService(&g_echo);
    serverLoop->runInLoop([&] { server->start(); });

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    RpcClient client(clientLoop, InetAddress("127.0.0.1", kTestPort));
    bool connected = false;
    for (int i = 0; i < 200 && !connected; ++i) {
        connected = client.connect();
        if (!connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ASSERT_TRUE(connected);
    std::string error;

    std::string big(1024 * 1024, 'L');
    example::EchoRequest req;
    req.set_message(std::move(big));
    req.set_repeat(1);
    example::EchoResponse resp;
    bool ok = client.syncCall("EchoService", "Echo", req, &resp, 10000,
                              &error);
    ASSERT_TRUE(ok);
    ASSERT_EQ(resp.message().size(), 1024u * 1024);

    client.disconnect();
    // give both sides time to process the FIN before tearing the loops down
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // graceful server shutdown must run on the server loop thread itself
    serverLoop->runInLoop([&] { server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // Acceptor's destructor touches the poller: the server must die
    // while its loop thread (and EventLoop object) is still alive.
    server.reset();
    serverLoop->quit();
}

int main() {
    test_sync_calls();
    test_error_paths();
    test_async_stub();
    test_concurrent_calls();
    test_large_payload();

    printf("\n%d assertions, %d passed, %d failed\n", g_test_count,
           g_test_passed, g_test_failed);
    return g_test_failed == 0 ? 0 : 1;
}
