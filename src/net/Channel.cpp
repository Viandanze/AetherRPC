#include "Channel.h"
#include "EventLoop.h"
#include "base/Logger.h"
#include <sys/epoll.h>
#include <cassert>

const int Channel::kNoneEvent  = 0;
const int Channel::kReadEvent  = EPOLLIN | EPOLLPRI;
const int Channel::kWriteEvent = EPOLLOUT;
const int Channel::kET         = EPOLLET;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      events_(0),
      revents_(0),
      index_(kNew) {}

Channel::~Channel() {}

void Channel::tie(const std::shared_ptr<void>& owner) {
    tie_ = owner;
    tied_ = true;
}

void Channel::handleEvent() {
    // If tied, keep the owner alive for the duration of the dispatch.
    // epoll_wait can return a batch of active channels; tearing down one
    // connection may destroy another whose Channel is still in this batch.
    // A failed lock means the owner is gone: skip the stale event.
    std::shared_ptr<void> guard;
    if (tied_) {
        guard = tie_.lock();
        if (!guard) {
            LOG_DEBUG("Channel::handleEvent fd=%d skipped: owner gone", fd_);
            return;
        }
    }

    if (revents_ & (EPOLLERR)) {
        if (errorCallback_) errorCallback_();
    }
    if (revents_ & (EPOLLHUP) && !(revents_ & EPOLLIN)) {
        if (closeCallback_) closeCallback_();
    }
    if (revents_ & (EPOLLIN | EPOLLPRI)) {
        if (readCallback_) readCallback_();
    }
    if (revents_ & EPOLLOUT) {
        if (writeCallback_) writeCallback_();
    }
}

void Channel::update() {
    // update epoll events via the EventLoop
    loop_->updateChannel(this);
}

void Channel::remove() {
    loop_->removeChannel(this);
}
