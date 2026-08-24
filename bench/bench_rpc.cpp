// AetherRPC vs gRPC unary echo benchmark, single machine / loopback.
//
// Methodology:
//   * same payload shape (bytes echo), same call pattern per thread
//   * one connection/channel per worker thread (AetherRPC holds one
//     ConnectionPool lease for the whole run; gRPC uses one channel)
//   * untimed warmup (200 calls per thread) primes connections,
//     JIT-less code paths and the gRPC channel handshake
//   * barrier start, wall-clock stop when the last thread finishes;
//     per-call latency percentiles are reported for single-thread runs
//
// Usage:
//   bench_rpc <aether|grpc> <payload-bytes> <threads> <calls-per-thread>
//
// Output (CSV): framework,payload,threads,calls,wall_ms,qps,
//               lat_avg_us,lat_p50_us,lat_p99_us,failures
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "rpc/ConnectionPool.h"
#include "rpc/RpcClient.h"
#include "rpc/RpcServer.h"
#include "bench.pb.h"
#include "bench_grpc.grpc.pb.h"

using EventLoop = ::EventLoop;
using EventLoopThread = ::EventLoopThread;
using InetAddress = ::InetAddress;
using aether::rpc::ConnectionPool;
using aether::rpc::RpcServer;

namespace {

const uint16_t kAetherPort = 19501;
const uint16_t kGrpcPort   = 19502;
const int kWarmupPerThread = 200;

// ---------------------------------------------------------------- common

struct Result {
    long long wallMs = 0;
    long long totalCalls = 0;
    long long failures = 0;
    double latAvgUs = 0, latP50Us = 0, latP99Us = 0;
};

class Barrier {
public:
    explicit Barrier(int n) : n_(n) {}
    void wait() {
        std::unique_lock<std::mutex> lock(mtx_);
        if (++count_ == n_) cv_.notify_all();
        else cv_.wait(lock, [&] { return count_ >= n_; });
    }
private:
    const int n_;
    std::mutex mtx_;
    std::condition_variable cv_;
    int count_ = 0;
};

void report(const char* framework, int payload, int threads,
            const Result& r) {
    std::printf("%s,%d,%d,%lld,%.1f,%.0f,%.1f,%.1f,%.1f,%lld\n", framework,
                payload, threads, r.totalCalls, r.wallMs / 1e0,
                r.totalCalls * 1000.0 / (r.wallMs > 0 ? r.wallMs : 1),
                r.latAvgUs, r.latP50Us, r.latP99Us, r.failures);
}

// ------------------------------------------------------------ aetherrpc

class AetherBenchServiceImpl final : public ::aether::bench::Bench {
public:
    void Echo(google::protobuf::RpcController*,
              const ::aether::bench::Payload* request,
              ::aether::bench::Payload* response,
              google::protobuf::Closure* done) override {
        response->set_data(request->data());
        if (done != nullptr) done->Run();
    }
};

int runAether(int payloadBytes, int threads, int callsPerThread) {
    AetherBenchServiceImpl impl;
    EventLoopThread serverThread;
    EventLoop* serverLoop = serverThread.startLoop();
    std::unique_ptr<RpcServer> server;
    std::promise<void> listening;
    server = std::make_unique<RpcServer>(
        serverLoop, InetAddress(kAetherPort), "bench-server");
    server->registerService(&impl);
    serverLoop->runInLoop([&] {
        server->start();
        listening.set_value();
    });
    listening.get_future().wait();

    EventLoopThread clientThread;
    EventLoop* clientLoop = clientThread.startLoop();
    ConnectionPool pool(clientLoop,
                        InetAddress("127.0.0.1", kAetherPort),
                        "bench", static_cast<size_t>(threads));

    std::atomic<long long> failures{0};
    std::vector<std::vector<long long>> lats(threads);
    Barrier barrier(threads);
    std::vector<std::thread> workers;

    const auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            ::aether::bench::Payload req, resp;
            req.set_data(std::string(payloadBytes, 'x'));
            std::string error;
            ConnectionPool::Lease lease = pool.acquire(3000);
            if (!lease) {
                failures.fetch_add(callsPerThread + kWarmupPerThread);
                barrier.wait();
                return;
            }
            for (int i = 0; i < kWarmupPerThread; ++i)
                lease->syncCall("Bench", "Echo", req, &resp, 5000, &error);

            barrier.wait();
            for (int i = 0; i < callsPerThread; ++i) {
                const auto s = std::chrono::steady_clock::now();
                if (!lease->syncCall("Bench", "Echo", req, &resp, 5000,
                                     &error)) {
                    failures.fetch_add(1);
                    continue;
                }
                const auto e = std::chrono::steady_clock::now();
                lats[t].push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(e - s)
                        .count());
            }
        });
    }
    for (auto& w : workers) w.join();
    const auto t1 = std::chrono::steady_clock::now();

    Result r;
    r.wallMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
            .count();
    r.totalCalls = static_cast<long long>(threads) * callsPerThread;
    r.failures = failures.load();
    if (threads == 1 && !lats[0].empty()) {
        std::vector<long long>& v = lats[0];
        std::sort(v.begin(), v.end());
        long long sum = 0;
        for (long long x : v) sum += x;
        r.latAvgUs = sum / 1000.0 / v.size();
        r.latP50Us = v[v.size() / 2] / 1000.0;
        r.latP99Us = v[(v.size() * 99) / 100] / 1000.0;
    }

    // Teardown on the loops (documented destruction contract).
    {
        std::promise<void> gone;
        serverLoop->runInLoop([&] {
            server->stop();
            server.reset();
            gone.set_value();
        });
        gone.get_future().wait();
    }
    report("aether", payloadBytes, threads, r);
    return r.failures == 0 ? 0 : 1;
}

// ----------------------------------------------------------------- grpc

class GrpcBenchServiceImpl final : public ::aether::bench::BenchG::Service {
public:
    grpc::Status Echo(grpc::ServerContext*,
                      const ::aether::bench::PayloadG* request,
                      ::aether::bench::PayloadG* response) override {
        response->set_data(request->data());
        return grpc::Status::OK;
    }
};

int runGrpc(int payloadBytes, int threads, int callsPerThread) {
    GrpcBenchServiceImpl impl;
    grpc::ServerBuilder builder;
    const std::string addr =
        std::string("127.0.0.1:") + std::to_string(kGrpcPort);
    builder.AddListeningPort(addr,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&impl);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (server == nullptr) {
        std::fprintf(stderr, "gRPC server failed to start\n");
        return 2;
    }

    std::atomic<long long> failures{0};
    std::vector<std::vector<long long>> lats(threads);
    Barrier barrier(threads);
    std::vector<std::thread> workers;

    const auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            auto channel = grpc::CreateChannel(
                addr, grpc::InsecureChannelCredentials());
            auto stub = ::aether::bench::BenchG::NewStub(channel);
            ::aether::bench::PayloadG req, resp;
            req.set_data(std::string(payloadBytes, 'x'));
            for (int i = 0; i < kWarmupPerThread; ++i) {
                grpc::ClientContext ctx;
                stub->Echo(&ctx, req, &resp);
            }

            barrier.wait();
            for (int i = 0; i < callsPerThread; ++i) {
                grpc::ClientContext ctx;
                const auto s = std::chrono::steady_clock::now();
                grpc::Status st = stub->Echo(&ctx, req, &resp);
                const auto e = std::chrono::steady_clock::now();
                if (!st.ok()) {
                    failures.fetch_add(1);
                    continue;
                }
                lats[t].push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(e - s)
                        .count());
            }
        });
    }
    for (auto& w : workers) w.join();
    const auto t1 = std::chrono::steady_clock::now();

    server->Shutdown();
    server.reset();

    Result r;
    r.wallMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
            .count();
    r.totalCalls = static_cast<long long>(threads) * callsPerThread;
    r.failures = failures.load();
    if (threads == 1 && !lats[0].empty()) {
        std::vector<long long>& v = lats[0];
        std::sort(v.begin(), v.end());
        long long sum = 0;
        for (long long x : v) sum += x;
        r.latAvgUs = sum / 1000.0 / v.size();
        r.latP50Us = v[v.size() / 2] / 1000.0;
        r.latP99Us = v[(v.size() * 99) / 100] / 1000.0;
    }
    report("grpc", payloadBytes, threads, r);
    return r.failures == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::fprintf(stderr,
                     "usage: %s <aether|grpc> <payload-bytes> <threads> "
                     "<calls-per-thread>\n",
                     argv[0]);
        return 2;
    }
    const std::string fw = argv[1];
    const int payload = std::atoi(argv[2]);
    const int threads = std::atoi(argv[3]);
    const int calls = std::atoi(argv[4]);
    if (payload <= 0 || threads <= 0 || calls <= 0) {
        std::fprintf(stderr, "bad arguments\n");
        return 2;
    }
    if (fw == "aether") return runAether(payload, threads, calls);
    if (fw == "grpc") return runGrpc(payload, threads, calls);
    std::fprintf(stderr, "unknown framework: %s\n", fw.c_str());
    return 2;
}
