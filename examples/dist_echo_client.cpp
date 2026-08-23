// AetherRPC example: distributed echo client.
//
//   ./dist_echo_client [n_calls] [zk_servers] [interval_ms]
//
// Discovers EchoService/MathService endpoints from ZooKeeper and calls
// them through ServiceConsumer (discovery + round-robin load balancing
// + cached multiplexed connections + failover retries).
//
// Replies carry a "[node:port]" prefix from the server, so the output
// directly shows how calls rotate across servers. Kill a server while
// this runs to watch failover: calls keep succeeding on the survivors
// after discovery updates.

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "rpc/LoadBalancer.h"
#include "rpc/ServiceConsumer.h"
#include "zk/ZkClient.h"
#include "echo.pb.h"

using namespace aether::rpc;
using namespace aether::zk;

int main(int argc, char* argv[]) {
    int nCalls = argc > 1 ? atoi(argv[1]) : 12;
    const char* zkServers = argc > 2 ? argv[2] : "127.0.0.1:2181";
    int intervalMs = argc > 3 ? atoi(argv[3]) : 200;

    EventLoopThread loopThread;
    EventLoop* loop = loopThread.startLoop();

    ZkClient zkClient(zkServers);
    if (!zkClient.connect()) {
        fprintf(stderr, "[client] cannot reach zookeeper at %s\n", zkServers);
        return 1;
    }

    ServiceConsumer consumer(loop, &zkClient,
                              std::make_unique<RoundRobinLoadBalancer>());

    int ok = 0, fail = 0;
    for (int i = 0; i < nCalls; ++i) {
        std::string error;
        if (i % 3 == 2) {
            ::aether::rpc::example::AddRequest req;
            req.set_a(i);
            req.set_b(100);
            ::aether::rpc::example::AddResponse resp;
            if (consumer.syncCall("MathService", "Add", req, &resp, 3000,
                                  &error)) {
                printf("[%3d] Add(%d, 100) -> %d\n", i, i, resp.sum());
                ++ok;
            } else {
                printf("[%3d] Add failed: %s\n", i, error.c_str());
                ++fail;
            }
        } else {
            ::aether::rpc::example::EchoRequest req;
            req.set_message("ping-" + std::to_string(i));
            req.set_repeat(1);
            ::aether::rpc::example::EchoResponse resp;
            if (consumer.syncCall("EchoService", "Echo", req, &resp, 3000,
                                  &error)) {
                printf("[%3d] Echo -> %s\n", i, resp.message().c_str());
                ++ok;
            } else {
                printf("[%3d] Echo failed: %s\n", i, error.c_str());
                ++fail;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }

    printf("[client] done: %d ok, %d failed\n", ok, fail);
    // Do NOT quit the loop here: the consumer's cached connections are
    // torn down in ~ServiceConsumer at scope exit, and that teardown
    // queues work on the loop. ~EventLoopThread (declared first,
    // destroyed last) quits and joins the loop at the right time.
    zkClient.shutdown();
    return fail == 0 ? 0 : 2;
}
