// AetherRPC example: distributed echo server.
//
//   ./dist_echo_server [port] [zk_servers] [duration_sec]
//
// Starts an RpcServer with the example services and registers it in
// ZooKeeper via RpcRegistry. Run several instances on different ports
// and watch dist_echo_client spread its calls over them. The node dies
// with the process (EPHEMERAL), so no manual deregistration is needed
// on crash - just stop the process.
//
// The echo reply is prefixed with the local port so you can SEE which
// server answered - useful to eyeball load balancing and failover.

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "rpc/RpcRegistry.h"
#include "rpc/RpcServer.h"
#include "zk/ZkClient.h"
#include "echo.pb.h"

using namespace aether::rpc;
using namespace aether::zk;

namespace {

class TaggedEchoServiceImpl final
    : public ::aether::rpc::example::EchoService {
public:
    explicit TaggedEchoServiceImpl(std::string tag) : tag_(std::move(tag)) {}

    void Echo(google::protobuf::RpcController*,
              const ::aether::rpc::example::EchoRequest* request,
              ::aether::rpc::example::EchoResponse* response,
              google::protobuf::Closure* done) override {
        std::string out;
        int repeat = request->repeat() > 0 ? request->repeat() : 1;
        for (int i = 0; i < repeat; ++i) out += request->message();
        response->set_message("[" + tag_ + "] " + out);
        if (done != nullptr) done->Run();
    }

private:
    std::string tag_;
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

}  // namespace

int main(int argc, char* argv[]) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 8100;
    const char* zkServers = argc > 2 ? argv[2] : "127.0.0.1:2181";
    int durationSec = argc > 3 ? atoi(argv[3]) : 30;

    // --- network + RPC server (Phase 1 machinery) -------------------------
    EventLoopThread loopThread;
    EventLoop* loop = loopThread.startLoop();

    InetAddress listenAddr(port);
    auto server = std::make_unique<RpcServer>(loop, listenAddr,
                                              "echo-server-" + std::to_string(port));
    TaggedEchoServiceImpl echo("node:" + std::to_string(port));
    MathServiceImpl math;
    server->registerService(&echo);
    server->registerService(&math);
    loop->runInLoop([&] { server->start(); });
    printf("[server] listening on 127.0.0.1:%u\n", port);

    // --- registry: advertise in ZooKeeper (Phase 2 machinery) -------------
    ZkClient zkClient(zkServers);
    if (!zkClient.connect()) {
        fprintf(stderr, "[server] cannot reach zookeeper at %s\n", zkServers);
        return 1;
    }
    RpcRegistry registry(&zkClient);
    const std::string addr = "127.0.0.1:" + std::to_string(port);
    if (!registry.registerService("EchoService", addr) ||
        !registry.registerService("MathService", addr)) {
        fprintf(stderr, "[server] zookeeper registration failed\n");
        return 1;
    }
    printf("[server] registered %s for EchoService/MathService in zk\n",
           addr.c_str());

    // --- run for a while, then exit cleanly --------------------------------
    for (int i = 0; i < durationSec; ++i) std::this_thread::sleep_for(std::chrono::seconds(1));
    printf("[server] %us elapsed, shutting down\n", durationSec);

    // Teardown order matters (see test_rpc_e2e.cpp): registry first (ZK
    // has no EventLoop dependency), then the server on its own loop,
    // then quit the loop, then close ZK.
    registry.unregisterService("EchoService");
    registry.unregisterService("MathService");
    loop->runInLoop([&] { server->stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    server.reset();
    loop->quit();
    zkClient.shutdown();
    printf("[server] clean exit\n");
    return 0;
}
