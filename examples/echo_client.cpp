// AetherRPC example: echo client.
//
//   ./echo_client [ip] [port]
//
// Demonstrates synchronous calls through syncCall(), including the
// error path (unknown service). Asynchronous stub-based usage is
// covered by test/test_rpc_e2e.cpp.

#include <cstdio>
#include <string>

#include "net/EventLoop.h"
#include "net/EventLoopThread.h"
#include "net/InetAddress.h"
#include "rpc/RpcClient.h"
#include "echo.pb.h"

using namespace aether::rpc;

int main(int argc, char* argv[]) {
    const char* ip = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? static_cast<uint16_t>(atoi(argv[2])) : 8080;

    EventLoopThread loopThread;
    EventLoop* loop = loopThread.startLoop();

    InetAddress serverAddr(ip, port);
    RpcClient client(loop, serverAddr, "echo-client");
    if (!client.connect()) {
        fprintf(stderr, "connect to %s:%u failed\n", ip, port);
        return 1;
    }

    std::string error;

    {   // happy path: echo with repeat
        example::EchoRequest req;
        req.set_message("hello aetherrpc ");
        req.set_repeat(3);
        example::EchoResponse resp;
        if (client.syncCall("EchoService", "Echo", req, &resp, 3000, &error)) {
            printf("[syncCall]  Echo -> \"%s\"\n", resp.message().c_str());
        } else {
            printf("[syncCall]  Echo failed: %s\n", error.c_str());
        }
    }

    {   // happy path: second registered service
        example::AddRequest req;
        req.set_a(19);
        req.set_b(23);
        example::AddResponse resp;
        if (client.syncCall("MathService", "Add", req, &resp, 3000, &error)) {
            printf("[syncCall]  Add(19, 23) -> %d\n", resp.sum());
        } else {
            printf("[syncCall]  Add failed: %s\n", error.c_str());
        }
    }

    {   // error path: unknown service
        example::AddRequest req;
        example::AddResponse resp;
        bool ok =
            client.syncCall("NopeService", "Add", req, &resp, 3000, &error);
        printf("[syncCall]  unknown service -> %s (%s)\n",
               ok ? "ok" : "failed", error.c_str());
    }

    {   // error path: unknown method
        example::AddRequest req;
        example::AddResponse resp;
        bool ok = client.syncCall("MathService", "Multiply", req, &resp, 3000,
                                  &error);
        printf("[syncCall]  unknown method -> %s (%s)\n",
               ok ? "ok" : "failed", error.c_str());
    }

    client.disconnect();
    return 0;
}
