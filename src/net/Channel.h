#ifndef AETHER_NET_CHANNEL_H
#define AETHER_NET_CHANNEL_H
#pragma once
#include <functional>
#include <memory>
#include "base/noncopyable.h"

class EventLoop;

// Channel: event dispatcher for an fd
// One Channel per fd (socket); registers epoll events and dispatches callbacks
class Channel : noncopyable {
public:
    using EventCallback = std::function<void()>;
    using ReadEventCallback = std::function<void()>;

    // channel state in the epoller
    enum { kNew = -1, kAdded = 1, kDeleted = 2 };

    Channel(EventLoop* loop, int fd);
    ~Channel();

    void handleEvent();  // core: dispatch callbacks based on revents_

    // set callbacks
    void setReadCallback(ReadEventCallback cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(EventCallback cb)    { writeCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb)    { closeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb)    { errorCallback_ = std::move(cb); }

    // set events of interest
    void enableReading()  { events_ |= kReadEvent; update(); }
    void disableReading() { events_ &= ~kReadEvent; update(); }
    void enableWriting()  { events_ |= kWriteEvent; update(); }
    void disableWriting() { events_ &= ~kWriteEvent; update(); }
    void disableAll()     { events_ = kNoneEvent; update(); }

    // edge-triggered mode
    void enableET() { events_ |= kET; update(); }

    bool isNoneEvent() const { return events_ == kNoneEvent; }
    bool isWriting()   const { return events_ & kWriteEvent; }

    int  fd()      const { return fd_; }
    int  events()  const { return events_; }
    void set_revents(int revt) { revents_ = revt; }
    int  index()   const { return index_; }
    void set_index(int idx) { index_ = idx; }

    EventLoop* ownerLoop() { return loop_; }
    void remove();  // remove self from the EventLoop

private:
    void update();

    static const int kNoneEvent;
    static const int kReadEvent;
    static const int kWriteEvent;
    static const int kET;

    EventLoop* loop_;
    const int fd_;
    int events_;     // registered events
    int revents_;    // ready events returned by epoll
    int index_;      // state in the epoller

    ReadEventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback closeCallback_;
    EventCallback errorCallback_;
};
#endif // AETHER_NET_CHANNEL_H
