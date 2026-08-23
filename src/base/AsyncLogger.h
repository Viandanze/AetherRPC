#ifndef AETHER_BASE_ASYNCLOGGER_H
#define AETHER_BASE_ASYNCLOGGER_H
#pragma once
#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <vector>
#include <atomic>
#include <fstream>
#include <cstring>

/// AsyncLogger: async double-buffered logging system
///
/// Design:
/// - Frontend (any thread): LOG_XXX macro -> format into FrontBuffer -> wake backend thread
/// - Backend (dedicated thread): every flushInterval seconds or when FrontBuffer is full,
///   swap Front/Back buffers and write Back to file
/// - Double buffering keeps frontend writes non-blocking; file I/O never blocks the frontend
///
/// Rollover policy:
/// - By size: roll when the file exceeds rollSize_
/// - By date: roll to a new file at midnight
/// - Keep N old log files, delete older ones automatically
class AsyncLogger {
public:
    static const int kBufferSize = 4 * 1024 * 1024;  // 4MB

    class Buffer {
    public:
        Buffer() : data_(kBufferSize), used_(0) {}

        void append(const char* buf, size_t len) {
            if (static_cast<size_t>(avail()) > len) {
                memcpy(data_.data() + used_, buf, len);
                used_ += static_cast<int>(len);
            }
        }

        const char* data() const { return data_.data(); }
        int length() const { return used_; }
        int avail() const { return kBufferSize - used_; }
        void reset() { used_ = 0; }
        bool empty() const { return used_ == 0; }

    private:
        std::vector<char> data_;
        int used_;
    };

    static AsyncLogger& instance();

    void start();
    void stop();

    void append(const char* logline, int len);

    void setLogFile(const std::string& path) { logFile_ = path; }
    void setFlushInterval(int seconds) { flushInterval_ = seconds; }
    void setRollSize(size_t size) { rollSize_ = size; }
    void setKeepFiles(int n) { keepFiles_ = n; }

private:
    AsyncLogger();
    ~AsyncLogger();

    void threadFunc();

    std::string getLogFileName(const struct tm& tm_now);
    void rollFile();
    void cleanOldFiles();
    bool isSameDay(const struct tm& a, const struct tm& b);

    std::string logFile_;
    int flushInterval_;
    size_t rollSize_;
    int keepFiles_;     // number of old log files to keep
    size_t writtenBytes_;
    int lastDay_;       // day-of-month (tm_mday) of last rollover, for day-change detection

    std::unique_ptr<Buffer> frontBuf_;
    std::unique_ptr<Buffer> backBuf_;
    std::vector<std::unique_ptr<Buffer>> buffersToWrite_;

    std::mutex mtx_;
    std::condition_variable cond_;
    std::thread thread_;
    std::atomic_bool running_;

    std::ofstream fileStream_;
};
#endif // AETHER_BASE_ASYNCLOGGER_H
