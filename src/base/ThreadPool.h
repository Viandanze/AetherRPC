#ifndef AETHER_BASE_THREADPOOL_H
#define AETHER_BASE_THREADPOOL_H
#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "base/noncopyable.h"

namespace aether {
namespace base {

/// ThreadPool: fixed-size worker pool for business (compute) tasks.
///
/// Distinct from EventLoopThreadPool (which owns IO loops): this pool has
/// no event loop; workers just pull plain tasks off a queue. RpcServer
/// uses it to keep slow business handlers off the IO threads (Phase 3).
///
/// Semantics:
/// - start(n) spawns n workers; run(task) is safe from any thread.
/// - stop() drains every queued task (tasks are NOT dropped), then joins.
///   This matters for RPC replies: a queued response callback must still
///   run, otherwise a client would hang waiting for an answer that was
///   computed but never sent.
class ThreadPool : noncopyable {
public:
    using Task = std::function<void()>;

    explicit ThreadPool(std::string name = "worker-pool");
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void start(int numThreads);
    void stop();

    /// Queue a task. Safe from any thread. Must only be called between
    /// start() and stop() (a task submitted after stop() is dropped with
    /// a log line - it can never run).
    void run(Task task);

    size_t queueSize() const;

private:
    void runInThread();
    Task take();

    const std::string name_;
    std::vector<std::unique_ptr<std::thread>> threads_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<Task> queue_;
    bool running_ = false;
};

}  // namespace base
}  // namespace aether

#endif  // AETHER_BASE_THREADPOOL_H
