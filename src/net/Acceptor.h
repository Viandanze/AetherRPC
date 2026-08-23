#ifndef AETHER_NET_ACCEPTOR_H
#define AETHER_NET_ACCEPTOR_H
#pragma once
#include <functional>
#include <memory>
#include "base/noncopyable.h"

class EventLoop;
class InetAddress;
class Socket;
class Channel;

/// Acceptor: listens on a port and accepts new connections
class Acceptor : noncopyable {
public:
    using NewConnectionCallback = std::function<void(int fd, const InetAddress&)>;

    Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reusePort);
    ~Acceptor();

    void setNewConnectionCallback(NewConnectionCallback cb) {
        newConnectionCallback_ = std::move(cb);
    }

    void listen();
    bool listening() const { return listening_; }
    void stopListening();  // stop listening (for graceful shutdown)

private:
    void handleRead();

    EventLoop* loop_;
    std::unique_ptr<Socket> acceptSocket_;
    std::unique_ptr<Channel> acceptChannel_;
    NewConnectionCallback newConnectionCallback_;
    bool listening_;
    int idleFd_;  // spare fd for handling EMFILE (fd exhaustion)
};
#endif // AETHER_NET_ACCEPTOR_H
