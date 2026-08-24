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
|  Distributed layer (src/rpc, src/zk)                          |
|   RpcRegistry     server-side ZK registration (ephemeral)     |
|   ServiceDiscovery  client-side endpoint watching (watchers)  |
|   ServiceConsumer  discovery + LB + connection cache + retry  |
|   LoadBalancer    round-robin / random endpoint selection     |
|   ZkClient        thin thread-safe ZooKeeper C-API wrapper    |
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
- **Service discovery (ZooKeeper)** — servers register as ephemeral
  nodes; clients watch the service path and see endpoints come and go
  live. Session loss triggers watcher re-arm and cache refresh
- **Client-side load balancing** — round-robin or random, plug your own
  `LoadBalancer` policy
- **Connection cache with failover** — one multiplexed connection per
  endpoint; on connect error / transport error / timeout the entry is
  evicted and the call retried on a different endpoint (at-least-once
  semantics — use idempotent services)
- **Per-call deadlines** — every pending call arms a deadline on the IO
  loop; expiry fails the call through the normal completion path, and
  retries share one total budget instead of resetting it per attempt
- **Keepalive and dead-peer detection** — configurable ping interval /
  missed-ping limit force-closes a silent connection so callers fail
  fast instead of hanging on a black hole; the server reaps idle
  connections with an O(1) timing wheel
- **Business-thread pool** — slow service handlers run on a dedicated
  worker pool (`RpcServer::setWorkerThreads()`), keeping the IO loops
  responsive
- **Aether-grade transport** — multi-reactor with IO thread pool,
  non-blocking IO, TCP_NODELAY, idle timeouts, graceful shutdown
- **Non-blocking connector with auto-reconnect** — `Connector` drives
  connect attempts with a state machine (non-blocking connect +
  epoll write-ready), optional exponential-backoff retry
  (`RpcClient::enableRetry()`, 500ms growing to 30s cap). On transport
  failure the connection re-establishes itself; pending stubs survive
  because `RpcChannel` re-binds to the fresh connection
- **Per-endpoint connection pools** — `ConnectionPool` keeps up to N
  multiplexed connections per endpoint (RAII `Lease`, idle recycling,
  slot reservation so the cap holds under concurrent acquires).
  `ServiceConsumer` uses a pool per discovered endpoint; a failed call
  drops just its connection while the pool's others stay warm

## Build

Requirements: Linux, CMake >= 3.10, g++ >= 9 (C++17), protobuf (runtime +
compiler), ZooKeeper C client (libzookeeper-mt-dev) for the distributed
tests and examples.

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
ctest            # 8 suites: net regression + codec + rpc e2e + LB + zk + phase3
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

## Distributed usage (Phase 2)

One `ZkClient` per process talks to ZooKeeper; servers register their
endpoint as an ephemeral node under `/AetherRPC/<service>/<host:port>`,
so a crashed server disappears automatically when its session expires.

Server side:

```cpp
ZkClient zk("127.0.0.1:2181");
zk.connect();
RpcRegistry registry(&zk);            // basePath defaults to /AetherRPC
// after the RpcServer is listening:
registry.registerService("EchoService", "127.0.0.1:8101");
```

Client side — `ServiceConsumer` ties discovery, load balancing and a
per-endpoint connection cache together:

```cpp
EventLoopThread loopThread;
EventLoop* loop = loopThread.startLoop();
ZkClient zk("127.0.0.1:2181");
zk.connect();

ServiceConsumer consumer(loop, &zk,
                          std::make_unique<RoundRobinLoadBalancer>());
EchoRequest req;  req.set_message("hello");
EchoResponse resp;
consumer.syncCall("EchoService", "Echo", req, &resp, 3000);
```

The first call installs the ZK watch; afterwards `syncCall` picks a
live endpoint, reuses the cached connection, and on failure evicts it
and retries on another endpoint (up to 3 attempts). Kill a server
mid-run and calls keep succeeding on the survivors.

Lifecycle rules (important):

- the `EventLoop` backing the consumer must outlive the consumer —
  declare `loopThread` before `consumer` and never quit the loop early;
  `~EventLoopThread` quits and joins at the right time
- shut the `ZkClient` down before the consumer's destructor runs (the
  discovery worker tolerates a closed handle and exits promptly)

Runnable versions: `examples/dist_echo_server.cpp`,
`examples/dist_echo_client.cpp`:

```bash
./dist_echo_server 8101 & ./dist_echo_server 8102 & ./dist_echo_server 8103 &
./dist_echo_client 12 127.0.0.1:2181
```

## Benchmarks

Unary echo, same machine (loopback), one connection/channel per worker
thread, 200 untimed warmup calls per thread, median of 3 runs.
`bench_rpc <aether|grpc> <payload-bytes> <threads> <calls-per-thread>`
compares AetherRPC against gRPC 1.30 (synchronous unary API, default
server thread pool) with the same bytes-echo payload.

| Payload | Threads | AetherRPC QPS | gRPC QPS | AetherRPC p50 | gRPC p50 |
|---------|---------|---------------|----------|---------------|----------|
| 64 B    | 1       | 62.5k         | 39.1k    | 14.8 µs       | 21.1 µs  |
| 1 KiB   | 1       | 59.5k         | 37.9k    | 15.3 µs       | 22.1 µs  |
| 16 KiB  | 1       | 26.6k         | 30.9k    | 33.3 µs       | 26.5 µs  |
| 64 B    | 8       | 72.4k         | 29.2k    | —             | —        |
| 1 KiB   | 8       | 69.3k         | 29.6k    | —             | —        |
| 16 KiB  | 8       | 31.6k         | 22.7k    | —             | —        |

Numbers are from a CPU-throttled cloud sandbox (release build,
`-O2`); treat them as a same-machine relative comparison, not absolute
hardware performance. gRPC's HTTP/2 framing costs show up at small
payloads, where AetherRPC's length-prefixed protobuf frames win ~35%
on latency and ~2.5x on throughput; at 16 KiB gRPC's tuned write path
pulls ahead on single-connection latency. The benchmark binary builds
automatically when a gRPC development installation is detected and is
otherwise skipped:

```bash
./bench/bench_rpc aether 64 8 2000
./bench/bench_rpc grpc   64 8 2000
```

## Roadmap

- [x] Phase 1 — single-node RPC: custom protocol, protobuf dispatch,
      sync/async calls, error paths, e2e tests
- [x] Phase 2 — distributed: ZooKeeper service registration (ephemeral
      nodes + watchers), client-side discovery cache, round-robin /
      random load balancing, per-endpoint connection cache, failover
      retries, distributed e2e tests with ASan
- [x] Phase 3 — production hardening:
  - [x] Per-call deadlines with a shared retry budget
  - [x] Connection keepalive + dead-peer detection (ping/pong, miss
        counting), server-side idle-connection reaping (timing wheel)
  - [x] Business-thread pool: slow service handlers run off the IO loop
  - [x] Non-blocking connector with automatic reconnect
  - [x] Connection pooling (per-endpoint pool with failover)
  - [x] Benchmarks vs gRPC

## License

MIT
