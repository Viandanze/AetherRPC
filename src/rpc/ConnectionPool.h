#ifndef AETHER_RPC_CONNECTION_POOL_H
#define AETHER_RPC_CONNECTION_POOL_H
#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include "base/noncopyable.h"
#include "net/InetAddress.h"

class EventLoop;

namespace aether {
namespace rpc {

class RpcClient;

/// ConnectionPool: a bounded pool of RpcClient connections to ONE
/// endpoint. A single multiplexed connection already carries many
/// concurrent calls (the channel matches replies by id), but a pool
/// raises the ceiling when a handful of long-running calls would
/// otherwise serialize behind one TCP stream's queueing behavior, and
/// it amortizes reconnect cost across callers.
///
/// Usage:
///   ConnectionPool pool(loop, InetAddress(8080), "consumer", 4);
///   auto lease = pool.acquire(1000);
///   if (lease) lease->syncCall(...);
///   // ~Lease returns the connection (or drops it if unhealthy)
///
/// `Lease` is an RAII handle: releasing is automatic, and a caller
/// that observed a transport failure calls markUnhealthy() first so
/// the broken connection is discarded instead of recycled.
///
/// acquire() semantics:
///   * an idle healthy connection is reused (checked with connected())
///   * otherwise a new one is created while a slot is reserved, so
///     liveCount never exceeds maxSize even when several threads
///     acquire concurrently
///   * when the pool is saturated, the caller waits on a condition
///     variable until a lease is released
///   * a failed connect (server down) returns an empty Lease - the
///     caller is expected to fail over to another endpoint
///
/// Thread-safety: acquire/release from any thread; `loop` must
/// outlive the pool (destruction runs each client's disconnect
/// handshake on it), same contract as RpcClient.
class ConnectionPool : noncopyable {
public:
    class Lease {
    public:
        Lease() = default;
        Lease(Lease&& other) noexcept;
        Lease& operator=(Lease&& other) noexcept;
        ~Lease();

        RpcClient* get() const { return client_.get(); }
        RpcClient* operator->() const { return client_.get(); }
        /// False when the pool could not provide a connection (connect
        /// failed); dereferencing is invalid in that state.
        explicit operator bool() const { return client_ != nullptr; }

        /// Tell the pool this connection is broken; it is discarded on
        /// release instead of being handed to the next caller.
        void markUnhealthy() { healthy_ = false; }

    private:
        friend class ConnectionPool;
        Lease(ConnectionPool* pool, std::shared_ptr<RpcClient> client);

        ConnectionPool* pool_ = nullptr;
        std::shared_ptr<RpcClient> client_;
        bool healthy_ = true;
    };

    /// `maxSize` = max live connections (idle + leased) to the endpoint.
    ConnectionPool(EventLoop* loop, const InetAddress& addr,
                   const std::string& name, size_t maxSize = 4);
    ~ConnectionPool();

    /// Blocking acquire (see class comment). `connectTimeoutMs <= 0`
    /// disables the connect deadline. Never returns a connection the
    /// pool knows to be dead; may return an empty Lease.
    Lease acquire(int connectTimeoutMs = 1000);

    const std::string& addr() const { return addrStr_; }
    size_t maxSize() const { return maxSize_; }

    /// Idle connections ready for reuse.
    size_t idleCount() const;
    /// Idle + currently leased connections (never exceeds maxSize()).
    size_t liveCount() const;

private:
    void release(Lease& lease);

    EventLoop* loop_;
    const InetAddress addr_;
    const std::string addrStr_;  // "host:port"
    const std::string name_;
    const size_t maxSize_;

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<std::shared_ptr<RpcClient>> idle_;
    size_t live_ = 0;   // idle_ size + outstanding leases
    size_t seq_ = 0;    // connection name suffix, under mtx_
};

}  // namespace rpc
}  // namespace aether

#endif  // AETHER_RPC_CONNECTION_POOL_H
