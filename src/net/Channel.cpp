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

void Channel::handleEvent() {
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
