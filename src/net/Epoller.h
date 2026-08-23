#ifndef AETHER_NET_EPOLLER_H
#define AETHER_NET_EPOLLER_H
#pragma once
#include <vector>
#include <sys/epoll.h>
#include <unistd.h>
#include "base/noncopyable.h"

class Channel;

// epoll wrapper: supports LT/ET modes
class Epoller : noncopyable {
public:
    Epoller();
    ~Epoller();

    // event operations
    void updateChannel(Channel* channel);   // add/modify
    void removeChannel(Channel* channel);   // remove

    // block waiting for events, return the ready Channels
    using ChannelList = std::vector<Channel*>;
    int poll(int timeoutMs, ChannelList* activeChannels);

private:
    static const int kInitEventListSize = 16;

    void fillActiveChannels(int numEvents, ChannelList* activeChannels) const;
    void update(int operation, Channel* channel);

    int epollFd_;
    std::vector<struct epoll_event> events_;
};
#endif // AETHER_NET_EPOLLER_H
