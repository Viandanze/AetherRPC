#ifndef AETHER_NET_CONNECTOR_H
#define AETHER_NET_CONNECTOR_H
#pragma once

#include <atomic>
#include <functional>
#include <memory>

#include "base/TimerId.h"
#include "base/noncopyable.h"
#include "net/InetAddress.h"

class Channel;
class EventLoop;

/// Connector: initiates a non-blocking TCP connect on an EventLoop and
/// retries with exponential backoff. On success the fd is handed to the
/// owner via the new-connection callback; the owner (typically a client
/// wrapper building a TcpConnection) owns the fd from that point on.
///
/// States (loop thread only unless noted):
///   kDisconnected -> kConnecting -> kConnected
/// A failed attempt returns to kDisconnected and either retries (when
/// enableRetry() was called) or reports a terminal failure through
/// connectFailedCallback_ and stays idle until the next start().
///
/// start()/stop() may be called from any thread (they funnel through
/// runInLoop); restart() must be called on the loop thread (from the
/// owner's connection-close callback). Callbacks fire on the loop
/// thread.
///
/// Backoff: 500ms initially, doubling per consecutive failure up to
/// 30s; a successful connect resets it.
class Connector : noncopyable,
                  public std::enable_shared_from_this<Connector> {
public:
    using NewConnectionCallback = std::function<void(int fd)>;
    using ConnectFailedCallback = std::function<void()>;

    Connector(EventLoop* loop, const InetAddress& serverAddr);
    ~Connector();

    void setNewConnectionCallback(NewConnectionCallback cb) {
        newConnectionCallback_ = std::move(cb);
    }
    /// Fired (loop thread) when an attempt fails terminally, i.e. retry
    /// is disabled. Lets a blocking connect() front-end fail fast
    /// instead of waiting out its whole timeout.
    void setConnectFailedCallback(ConnectFailedCallback cb) {
        connectFailedCallback_ = std::move(cb);
    }

    /// Begin connecting. No-op unless currently kDisconnected. Any
    /// thread.
    void start();

    /// Enable retry-with-backoff on failures. Any thread.
    void enableRetry() { retry_ = true; }

    /// Stop connecting/retrying (idempotent, any thread). An fd already
    /// handed off is NOT affected - its owner keeps it.
    void stop();

    /// Called by the owner when an established connection drops: resets
    /// the backoff and starts a fresh attempt. No-op unless started and
    /// retry enabled. Loop thread only.
    void restart();

    bool retryEnabled() const { return retry_.load(); }

private:
    enum States { kDisconnected, kConnecting, kConnected };

    void startInLoop();
    void stopInLoop();
    void connect();
    void connecting(int sockfd);
    /// Disable + deregister the channel from the epoller and return its
    /// fd. The channel_ pointer itself is reset from a queued functor:
    /// removeChannel() can be called from inside that channel's own
    /// handleEvent, so destroying it here would be a use-after-free.
    int  removeChannel();
    void resetChannel();
    void handleWrite();
    void handleError();
    void retry(int sockfd);
    void retryInLoop();

    void setState(States s) { state_.store(s); }

    EventLoop* loop_;
    InetAddress serverAddr_;
    std::atomic<bool> connect_{false};  // start()/stop() intent
    std::atomic<bool> retry_{false};
    std::atomic<int> state_{kDisconnected};
    int retryDelayMs_;   // loop thread only
    std::unique_ptr<Channel> channel_;  // armed only in kConnecting
    TimerId retryTimer_{};
    bool retryTimerArmed_ = false;      // loop thread only
    NewConnectionCallback newConnectionCallback_;
    ConnectFailedCallback connectFailedCallback_;
};

#endif  // AETHER_NET_CONNECTOR_H
