#ifndef AETHER_NET_TCPSERVER_H
#define AETHER_NET_TCPSERVER_H
#pragma once
#include <string>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <functional>
#include "base/noncopyable.h"
#include "net/Buffer.h"

class EventLoop;
class InetAddress;
class Acceptor;
class TcpConnection;
class EventLoopThreadPool;

/// TcpServer: manages the Acceptor and all TcpConnections
///
/// Supports the main/sub reactor model: setThreadNum(N) enables N IO threads
/// Supports graceful shutdown: stopAccepting() -> drain connections -> forceCloseAll()
class TcpServer : noncopyable {
public:
    using ConnectionCallback    = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using MessageCallback       = std::function<void(const std::shared_ptr<TcpConnection>&, Buffer*)>;
    using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;

    TcpServer(EventLoop* loop, const InetAddress& listenAddr, const std::string& name);
    ~TcpServer();

    void start();

    // --- configuration ---
    void setThreadNum(int numThreads);
    void setMaxConnections(int maxConn) { maxConnections_ = maxConn; }
    void setIdleTimeout(int seconds) { idleTimeoutSeconds_ = seconds; }

    // --- callback setup ---
    void setConnectionCallback(ConnectionCallback cb) { connectionCallback_ = std::move(cb); }
    void setMessageCallback(MessageCallback cb) { messageCallback_ = std::move(cb); }
    void setWriteCompleteCallback(WriteCompleteCallback cb) { writeCompleteCallback_ = std::move(cb); }

    // --- state queries ---
    int connectionCount() const { return static_cast<int>(connections_.size()); }
    EventLoop* getLoop() const { return loop_; }

    // --- graceful shutdown ---
    void stopAccepting();   // stop accepting new connections
    void forceCloseAll();   // force-close all connections

private:
    void newConnection(int fd, const InetAddress& peerAddr);
    void removeConnection(const std::shared_ptr<TcpConnection>& conn);
    void removeConnectionInLoop(const std::shared_ptr<TcpConnection>& conn);

    EventLoop* loop_;
    const std::string name_;
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> threadPool_;
    std::atomic_int connCount_;

    int maxConnections_;
    int idleTimeoutSeconds_;  // idle timeout in seconds (0 = no timeout)

    using ConnectionMap = std::unordered_map<std::string, std::shared_ptr<TcpConnection>>;
    ConnectionMap connections_;

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
};
#endif // AETHER_NET_TCPSERVER_H
