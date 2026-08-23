#include "TcpServer.h"
#include "EventLoop.h"
#include "EventLoopThreadPool.h"
#include "Acceptor.h"
#include "InetAddress.h"
#include "TcpConnection.h"
#include "base/Logger.h"
#include <cstdio>
#include <unistd.h>

TcpServer::TcpServer(EventLoop* loop, const InetAddress& listenAddr, const std::string& name)
    : loop_(loop),
      name_(name),
      acceptor_(new Acceptor(loop, listenAddr, true)),
      threadPool_(new EventLoopThreadPool(loop, name)),
      connCount_(0),
      maxConnections_(0),
      idleTimeoutSeconds_(0) {
    acceptor_->setNewConnectionCallback([this](int fd, const InetAddress& peerAddr) {
        newConnection(fd, peerAddr);
    });
}

TcpServer::~TcpServer() {}

void TcpServer::setThreadNum(int numThreads) {
    threadPool_->setThreadNum(numThreads);
}

bool TcpServer::listening() const { return acceptor_->listening(); }

void TcpServer::start() {
    threadPool_->start();

    // if idle timeout is configured, create a TimerWheel on every EventLoop
    if (idleTimeoutSeconds_ > 0) {
        auto loops = threadPool_->getAllLoops();
        for (auto* ioLoop : loops) {
            ioLoop->setIdleTimeout(idleTimeoutSeconds_);
        }
    }

    acceptor_->listen();
    LOG_INFO("TcpServer[%s] started with %d IO threads, maxConn=%s, idleTimeout=%ds, listening...",
             name_.c_str(), threadPool_->threadNum(),
             maxConnections_ > 0 ? std::to_string(maxConnections_).c_str() : "unlimited",
             idleTimeoutSeconds_);
}

void TcpServer::newConnection(int fd, const InetAddress& peerAddr) {
    // connection count limit check
    if (maxConnections_ > 0 && static_cast<int>(connections_.size()) >= maxConnections_) {
        LOG_WARN("TcpServer: max connections %d reached, rejecting %s",
                 maxConnections_, peerAddr.toIpPort().c_str());
        ::close(fd);
        return;
    }

    char buf[64];
    snprintf(buf, sizeof(buf), "-%d", connCount_++);
    std::string connName = name_ + buf;

    // get local address
    struct sockaddr_in localAddr;
    socklen_t len = sizeof(localAddr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&localAddr), &len);
    InetAddress local(localAddr);

    // round-robin IO thread selection
    EventLoop* ioLoop = threadPool_->getNextLoop();

    auto conn = std::make_shared<TcpConnection>(ioLoop, connName, fd, local, peerAddr);
    connections_[connName] = conn;
    conn->setConnectionCallback(connectionCallback_);
    conn->setMessageCallback(messageCallback_);
    conn->setWriteCompleteCallback(writeCompleteCallback_);
    conn->setCloseCallback([this](const std::shared_ptr<TcpConnection>& c) {
        removeConnection(c);
    });

    // finish connection setup in the IO thread; also register the
    // connection with that loop's idle wheel (no-op when no idle timeout
    // is configured). TcpConnection::handleRead refreshes it on traffic.
    ioLoop->runInLoop([conn, ioLoop]() {
        conn->connectEstablished();
        ioLoop->insertToWheel(conn);
    });

    LOG_INFO("New connection [%s] from %s -> loop %p (total: %zu)",
             connName.c_str(), peerAddr.toIpPort().c_str(), ioLoop,
             connections_.size());
}

void TcpServer::removeConnection(const std::shared_ptr<TcpConnection>& conn) {
    // thread safety: every connections_ operation must run in the main EventLoop thread
    loop_->runInLoop([this, conn]() { removeConnectionInLoop(conn); });
}

void TcpServer::removeConnectionInLoop(const std::shared_ptr<TcpConnection>& conn) {
    LOG_INFO("TcpServer::removeConnection [%s]", conn->name().c_str());
    size_t n = connections_.erase(conn->name());
    (void)n;

    // destroy safely in the connection's own IO thread
    EventLoop* ioLoop = conn->getLoop();
    ioLoop->queueInLoop([conn]() { conn->connectDestroyed(); });
}

// --- graceful shutdown ---

void TcpServer::stopAccepting() {
    loop_->runInLoop([this]() {
        acceptor_->stopListening();
        LOG_INFO("TcpServer[%s]: stopped accepting new connections, %zu active connections remaining",
                 name_.c_str(), connections_.size());
    });
}

void TcpServer::forceCloseAll() {
    loop_->runInLoop([this]() {
        LOG_WARN("TcpServer[%s]: force closing %zu connections",
                 name_.c_str(), connections_.size());
        // conn->forceClose() runs synchronously on this thread and may
        // synchronously call back into removeConnection(), which erases
        // from connections_ — iterating the map directly would invalidate
        // the iterator. Snapshot the connections first.
        std::vector<std::shared_ptr<TcpConnection>> conns;
        conns.reserve(connections_.size());
        for (auto& [name, conn] : connections_) {
            conns.push_back(conn);
        }
        for (auto& conn : conns) {
            conn->forceClose();
        }
    });
}
