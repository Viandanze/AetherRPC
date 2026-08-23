#include "Epoller.h"
#include "Channel.h"
#include "base/Logger.h"
#include <cassert>
#include <cerrno>
#include <unistd.h>
#include <cstring>

Epoller::Epoller()
    : epollFd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize) {
    if (epollFd_ < 0) {
        LOG_FATAL("epoll_create1 failed: %s", strerror(errno));
    }
}

Epoller::~Epoller() { ::close(epollFd_); }

void Epoller::updateChannel(Channel* channel) {
    const int index = channel->index();
    if (index == Channel::kNew || index == Channel::kDeleted) {
        // new or deleted channel -> ADD
        int fd = channel->fd();
        channel->set_index(Channel::kAdded);
        update(EPOLL_CTL_ADD, channel);
    } else {
        // existing channel -> MOD
        int fd = channel->fd();
        (void)fd;
        if (channel->isNoneEvent()) {
            update(EPOLL_CTL_DEL, channel);
            channel->set_index(Channel::kDeleted);
        } else {
            update(EPOLL_CTL_MOD, channel);
        }
    }
}

void Epoller::removeChannel(Channel* channel) {
    int fd = channel->fd();
    int index = channel->index();
    if (index == Channel::kAdded) {
        update(EPOLL_CTL_DEL, channel);
    }
    channel->set_index(Channel::kNew);
}

int Epoller::poll(int timeoutMs, ChannelList* activeChannels) {
    int numEvents = ::epoll_wait(epollFd_, events_.data(),
                                  static_cast<int>(events_.size()), timeoutMs);
    int savedErrno = errno;
    if (numEvents > 0) {
        fillActiveChannels(numEvents, activeChannels);
        // dynamic growth: double the event list once it fills up
        if (static_cast<size_t>(numEvents) == events_.size()) {
            events_.resize(events_.size() * 2);
        }
    } else if (numEvents == 0) {
        // LOG_DEBUG("nothing happened");
    } else {
        if (savedErrno != EINTR) {
            errno = savedErrno;
            LOG_ERROR("epoll_wait error: %s", strerror(savedErrno));
        }
    }
    return numEvents;
}

void Epoller::fillActiveChannels(int numEvents, ChannelList* activeChannels) const {
    for (int i = 0; i < numEvents; ++i) {
        auto* channel = static_cast<Channel*>(events_[i].data.ptr);
        channel->set_revents(events_[i].events);
        activeChannels->push_back(channel);
    }
}

void Epoller::update(int operation, Channel* channel) {
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.events = channel->events();
    event.data.ptr = channel;
    int fd = channel->fd();
    if (::epoll_ctl(epollFd_, operation, fd, &event) < 0) {
        if (operation == EPOLL_CTL_DEL) {
            LOG_ERROR("epoll_ctl del fd=%d error: %s", fd, strerror(errno));
        } else {
            LOG_FATAL("epoll_ctl add/mod fd=%d error: %s", fd, strerror(errno));
        }
    }
}
