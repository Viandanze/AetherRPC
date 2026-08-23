# AetherRPC

A lightweight C++ RPC framework built on the network kernel of
[Aether](https://github.com/Viandanze/Aether) — a high-performance
epoll-based HTTP server (500K+ QPS on static content). AetherRPC strips
the HTTP layer and reuses the Reactor core (EventLoop / Epoller /
TcpConnection / Buffer / thread pool) as a general-purpose TCP transport,
layering a custom binary RPC protocol and protobuf service dispatch on top.

## Architecture

```
+---------------------------------------------------------------+
|  Business services (generated stubs / RpcClient::syncCall)    |
+---------------------------------------------------------------+
|  RPC layer (src/rpc)                                          |
|   RpcServer    service registry + protobuf reflection dispatch|
|   RpcChannel   client-side call multiplexing (request-id)     |
|   RpcClient    connection management + synchronous calls      |
|   RpcCodec     frame encode/decode (half/sticky package safe) |
+---------------------------------------------------------------+
|  Network kernel (src/net, src/base) — inherited from Aether   |
|   EventLoop / Epoller / Acceptor / TcpConnection / Buffer     |
|   EventLoopThreadPool / TimerQueue / AsyncLogger              |
+---------------------------------------------------------------+
|  Linux (epoll, non-blocking IO, SO_REUSEPORT)                 |
+---------------------------------------------------------------+
```

## Wire protocol

Each frame is a 12-byte fixed header (little-endian) plus a
protobuf-encoded body:

```
+--------+---------+---------+------------+---------+-------------+
| magic  | version | msgType | requestId  | bodyLen |  body (pb)  |
|  2B    |   1B    |   1B    |    4B      |   4B    |   bodyLen   |
+--------+---------+---------+------------+---------+-------------+

magic     0xAE50 ("AE" + "P")
msgType   1 = REQUEST, 2 = RESPONSE, 3 = HEARTBEAT (reserved)
body      RpcRequest  { service_name, method_name, args }
          RpcResponse { error_code, error_msg, result }
```

Requests are matched to responses by `requestId`, so many calls can be
in flight concurrently over one TCP connection. The codec handles half
packages and sticky packages via length-prefix framing.

## Features

- **Synchronous and asynchronous calls** — `RpcClient::syncCall()` for
  simple usage, generated `Service::Stub` over `RpcChannel` for async
- **Protobuf reflection dispatch** — register any `google::protobuf::Service*`;
  method lookup by name, request/response prototypes built at runtime
- **Multi-service support** — several services per server, dispatched by name
- **Call multiplexing** — concurrent calls share one connection, replies
  matched by request id
- **Clean error reporting** — unknown service/method, parse errors,
  timeouts and transport failures surface through the controller
- **Aether-grade transport** — multi-reactor with IO thread pool,
  non-blocking IO, TCP_NODELAY, idle timeouts, graceful shutdown

## Build

Requirements: Linux, CMake >= 3.10, g++ >= 9 (C++17), protobuf (runtime +
compiler).

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
ctest            # 5 suites: buffer/inet/timestamp regression + codec + e2e
```

## Quick start

Server:

```cpp
EventLoop loop;
RpcServer server(&loop, InetAddress(8080));
server.registerService(new EchoServiceImpl);   // protobuf service impl
server.setThreadNum(4);
server.start();
loop.loop();
```

Client:

```cpp
EventLoopThread loopThread;
EventLoop* loop = loopThread.startLoop();
RpcClient client(loop, InetAddress("127.0.0.1", 8080));
client.connect();

EchoRequest req;  req.set_message("hello");  req.set_repeat(3);
EchoResponse resp;
client.syncCall("EchoService", "Echo", req, &resp);

// or through a generated stub:
EchoService::Stub stub(client.channel());
stub.Echo(nullptr, &req, &resp, done);
```

Runnable versions: `examples/echo_server.cpp`, `examples/echo_client.cpp`.

## Roadmap

- [x] Phase 1 — single-node RPC: custom protocol, protobuf dispatch,
      sync/async calls, error paths, e2e tests
- [ ] Phase 2 — service discovery (ZooKeeper ephemeral nodes + watchers),
      client-side load balancing
- [ ] Phase 3 — timeouts, heartbeats, connection pooling, business-thread
      pool, non-blocking connector with reconnect, benchmarks vs gRPC

## License

MIT
