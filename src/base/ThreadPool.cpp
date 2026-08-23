#include "base/ThreadPool.h"

#include "base/Logger.h"

namespace aether {
namespace base {

ThreadPool::ThreadPool(std::string name) : name_(std::move(name)) {}

ThreadPool::~ThreadPool() {
    if (running_) stop();
}

void ThreadPool::start(int numThreads) {
    if (running_) return;
    running_ = true;
    for (int i = 0; i < numThreads; ++i) {
        threads_.emplace_back(std::make_unique<std::thread>(
            [this]() { runInThread(); }));
    }
    LOG_INFO("ThreadPool[%s]: started with %d workers", name_.c_str(),
             numThreads);
}

void ThreadPool::stop() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!running_) return;
        running_ = false;
    }
    // Wake every worker; each drains the queue before exiting, so no
    // queued task is dropped (see class comment).
    cv_.notify_all();
    for (auto& t : threads_) {
        if (t->joinable()) t->join();
    }
    threads_.clear();
    LOG_INFO("ThreadPool[%s]: stopped, queue drained", name_.c_str());
}

void ThreadPool::run(Task task) {
    if (task == nullptr) return;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!running_) {
            LOG_WARN("ThreadPool[%s]: run() after stop, task dropped",
                     name_.c_str());
            return;
        }
        queue_.push_back(std::move(task));
    }
    cv_.notify_one();
}

size_t ThreadPool::queueSize() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return queue_.size();
}

void ThreadPool::runInThread() {
    while (true) {
        Task task = take();
        if (task == nullptr) break;  // stopped and queue drained
        task();
    }
}

ThreadPool::Task ThreadPool::take() {
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [this]() { return !queue_.empty() || !running_; });
    if (queue_.empty()) return nullptr;  // !running_ && drained
    Task task = std::move(queue_.front());
    queue_.pop_front();
    return task;
}

}  // namespace base
}  // namespace aether
