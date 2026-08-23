#ifndef AETHER_NET_EVENTLOOPTHREADPOOL_H
#define AETHER_NET_EVENTLOOPTHREADPOOL_H
#pragma once
#include <vector>
#include <memory>
#include <functional>
#include <atomic>
#include <string>
#include "base/noncopyable.h"

class EventLoop;
class EventLoopThread;

// EventLoopThreadPool: sub-reactor thread pool for the main/sub reactor model
// mainReactor (Acceptor) takes new connections -> round-robin to subReactors (IO threads)
// setThreadNum(0) = single-reactor mode (main thread does all IO)
// setThreadNum(N) = 1 main + N sub reactors
class EventLoopThreadPool : noncopyable {
public:
    using ThreadInitCallback = std::function<void(EventLoop*)>;

    EventLoopThreadPool(EventLoop* baseLoop, const std::string& name = std::string());
    ~EventLoopThreadPool();

    void setThreadNum(int numThreads) { numThreads_ = numThreads; }
    int  threadNum() const { return numThreads_; }

    void start(const ThreadInitCallback& cb = ThreadInitCallback());

    // round-robin the next EventLoop (thread-safe)
    // if the pool isn't started, returns baseLoop (single-reactor mode)
    EventLoop* getNextLoop();

    // all loops (for things like timers that need iteration)
    std::vector<EventLoop*> getAllLoops();

    bool started() const { return started_; }
    const std::string& name() const { return name_; }

private:
    EventLoop* baseLoop_;  // the main reactor's loop (the thread TcpServer lives in)
    std::string name_;
    bool started_;
    int numThreads_;

    std::atomic_int next_;  // round-robin index

    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;  // raw pointers to all sub loops
};
#endif // AETHER_NET_EVENTLOOPTHREADPOOL_H
