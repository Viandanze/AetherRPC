#ifndef AETHER_NET_TCPCONNECTION_H
#define AETHER_NET_TCPCONNECTION_H
#pragma once
#include <string>
#include <memory>
#include <functional>
#include <any>
#include <cstdint>
#include "base/noncopyable.h"
#include "net/Buffer.h"
#include "net/InetAddress.h"

class EventLoop;
class Socket;
class Channel;
class InetAddress;

/// TcpConnection: manages one established TCP connection
///
/// Lifetime: Connecting -> Connected -> Disconnecting -> Disconnected
/// Held by shared_ptr so the connection survives across callbacks
class TcpConnection : noncopyable,
                      public std::enable_shared_from_this<TcpConnection> {
public:
    using ConnectionCallback    = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using MessageCallback       = std::function<void(const std::shared_ptr<TcpConnection>&, Buffer*)>;
    using WriteCompleteCallback = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using CloseCallback         = std::function<void(const std::shared_ptr<TcpConnection>&)>;
    using HighWaterMarkCallback = std::function<void(const std::shared_ptr<TcpConnection>&, size_t)>;

    TcpConnection(EventLoop* loop, const std::string& name, int fd,
                  const InetAddress& localAddr, const InetAddress& peerAddr);
    ~TcpConnection();

    EventLoop* getLoop() const { return loop_; }
    const std::string& name() const { return name_; }
    const InetAddress& localAddr() const { return localAddr_; }
    const InetAddress& peerAddr() const { return peerAddr_; }
    bool connected() const { return state_ == kConnected; }
    bool disconnected() const { return state_ == kDisconnected; }

    // --- sending ---
    void send(const std::string& message);
    void send(const char* data, size_t len);
    void send(Buffer* buf);  // zero-copy swap

    // zero-copy file send: push via sendfile(2); on EAGAIN pread the remainder into outputBuf_ and continue.
    // this connection takes fd ownership (closes it when the send completes or the connection closes); the caller must not touch it again
    void sendFile(int fileFd, off_t offset, size_t count);

    // --- closing ---
    void shutdown();       // half-close (close the write side after pending data drains)
    void forceClose();     // close immediately
    void forceCloseWithDelay(double seconds);  // force-close after a delay

    // --- callback setup ---
    void setConnectionCallback(ConnectionCallback cb) { connectionCallback_ = std::move(cb); }
    void setMessageCallback(MessageCallback cb) { messageCallback_ = std::move(cb); }
    void setWriteCompleteCallback(WriteCompleteCallback cb) { writeCompleteCallback_ = std::move(cb); }
    void setCloseCallback(CloseCallback cb) { closeCallback_ = std::move(cb); }
    void setHighWaterMarkCallback(HighWaterMarkCallback cb, size_t highWaterMark) {
        highWaterMarkCallback_ = std::move(cb);
        highWaterMark_ = highWaterMark;
    }

    // --- context ---
    void setContext(const std::any& ctx) { context_ = ctx; }
    const std::any& getContext() const { return context_; }
    std::any* getMutableContext() { return &context_; }

    // --- buffer access ---
    Buffer* inputBuffer() { return &inputBuf_; }
    Buffer* outputBuffer() { return &outputBuf_; }

    // --- idle timeout (for TimerWheel) ---
    uint64_t idleTimerGeneration() const { return idleTimerGeneration_; }
    uint64_t& idleTimerGeneration() { return idleTimerGeneration_; }

    // --- TCP options ---
    void setTcpNoDelay(bool on);

    // --- connect/disconnect (internal) ---
    void connectEstablished();
    void connectDestroyed();

private:
    enum StateE { kDisconnected, kConnecting, kConnected, kDisconnecting };

    void setState(StateE s) { state_ = s; }
    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();
    void sendInLoop(const std::string& message);
    void sendInLoop(const char* data, size_t len);
    void sendFileInLoop(int fileFd, off_t offset, size_t count);
    void shutdownInLoop();
    void forceCloseInLoop();

    EventLoop* loop_;
    std::string name_;
    StateE state_;
    std::unique_ptr<Socket> socket_;
    std::unique_ptr<Channel> channel_;
    InetAddress localAddr_;
    InetAddress peerAddr_;

    Buffer inputBuf_;   // input buffer
    Buffer outputBuf_;  // output buffer

    ConnectionCallback connectionCallback_;
    MessageCallback messageCallback_;
    WriteCompleteCallback writeCompleteCallback_;
    CloseCallback closeCallback_;
    HighWaterMarkCallback highWaterMarkCallback_;
    size_t highWaterMark_;

    std::any context_;  // per-connection context (e.g. HttpContext)

    uint64_t idleTimerGeneration_;  // for TimerWheel: bumped on every refresh so stale entries invalidate
};
#endif // AETHER_NET_TCPCONNECTION_H
