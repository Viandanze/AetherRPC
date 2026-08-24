#include "rpc/ConnectionPool.h"

#include <utility>

#include "rpc/RpcClient.h"

namespace aether {
namespace rpc {

namespace {
std::string addrToString(const InetAddress& addr) {
    return addr.toIpPort();  // assumes net layer provides host:port text
}
}  // namespace

// ---------------------------------------------------------------- Lease

ConnectionPool::Lease::Lease(ConnectionPool* pool,
                             std::shared_ptr<RpcClient> client)
    : pool_(pool), client_(std::move(client)) {}

ConnectionPool::Lease::Lease(Lease&& other) noexcept
    : pool_(other.pool_), client_(std::move(other.client_)),
      healthy_(other.healthy_) {
    other.pool_ = nullptr;
    other.healthy_ = true;
}

ConnectionPool::Lease& ConnectionPool::Lease::operator=(Lease&& other) noexcept {
    if (this != &other) {
        if (pool_ != nullptr && client_ != nullptr) pool_->release(*this);
        pool_ = other.pool_;
        client_ = std::move(other.client_);
        healthy_ = other.healthy_;
        other.pool_ = nullptr;
        other.healthy_ = true;
    }
    return *this;
}

ConnectionPool::Lease::~Lease() {
    if (pool_ != nullptr && client_ != nullptr) pool_->release(*this);
}

// ---------------------------------------------------------- ConnectionPool

ConnectionPool::ConnectionPool(EventLoop* loop, const InetAddress& addr,
                               const std::string& name, size_t maxSize)
    : loop_(loop), addr_(addr), addrStr_(addrToString(addr)),
      name_(name), maxSize_(maxSize == 0 ? 1 : maxSize) {}

ConnectionPool::~ConnectionPool() {
    // Drop idle clients while their IO loop is still alive (the owner
    // must destroy the pool before stopping `loop`). Their dtors run
    // the synchronous disconnect handshake.
    std::deque<std::shared_ptr<RpcClient>> idle;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        idle.swap(idle_);
        live_ -= idle.size();
    }
    // Destroyed outside the lock (each one waits on the IO thread).
}

ConnectionPool::Lease ConnectionPool::acquire(int connectTimeoutMs) {
    std::unique_lock<std::mutex> lock(mtx_);
    for (;;) {
        // Reuse an idle connection that is still alive.
        while (!idle_.empty()) {
            std::shared_ptr<RpcClient> client = std::move(idle_.front());
            idle_.pop_front();
            if (client != nullptr && client->connected()) {
                return Lease(this, std::move(client));
            }
            --live_;  // stale entry: dropped by its lease or dead peer
        }

        if (live_ < maxSize_) {
            ++live_;  // reserve the slot before the blocking connect
            const size_t seq = ++seq_;
            lock.unlock();

            auto client = std::make_shared<RpcClient>(
                loop_, addr_, name_ + "-c" + std::to_string(seq));
            bool ok = false;
            try {
                ok = client->connect(connectTimeoutMs);
            } catch (...) {
                ok = false;
            }
            if (!ok) {
                lock.lock();
                --live_;
                cv_.notify_one();
                return Lease();  // endpoint unreachable
            }
            return Lease(this, std::move(client));
        }

        // Saturated: wait until a lease is released.
        cv_.wait(lock);
    }
}

void ConnectionPool::release(Lease& lease) {
    std::shared_ptr<RpcClient> client = std::move(lease.client_);
    const bool healthy = lease.healthy_;
    lease.pool_ = nullptr;
    lease.healthy_ = true;

    bool recycle = healthy && client != nullptr && client->connected();
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (recycle) {
            idle_.push_back(std::move(client));
        } else {
            --live_;
        }
    }
    cv_.notify_one();
    // A discarded client is destroyed here, outside the lock; its
    // disconnect handshake queues on the IO loop and waits for it.
}

size_t ConnectionPool::idleCount() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return idle_.size();
}

size_t ConnectionPool::liveCount() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return live_;
}

}  // namespace rpc
}  // namespace aether
