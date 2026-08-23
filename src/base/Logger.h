#ifndef AETHER_BASE_LOGGER_H
#define AETHER_BASE_LOGGER_H
#pragma once
#include <string>
#include <cstdio>
#include <cstdarg>
#include <mutex>

// log levels
enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL
};

// Logger: logging interface
// Default: sync writes to stderr; can switch to async mode (file output)
class Logger {
public:
    static Logger& instance();
    void setLevel(LogLevel level);
    void log(LogLevel level, const char* file, int line, const char* fmt, ...);

    // enable async logging (write to file)
    void enableAsync(const std::string& logFile = "./aether.log");

    // get current log level
    LogLevel level() const { return level_; }

private:
    Logger();
    LogLevel level_;
    std::mutex mtx_;
    bool asyncMode_;
    const char* levelStr(LogLevel level);
};

#define LOG_DEBUG(...) Logger::instance().log(LogLevel::DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  Logger::instance().log(LogLevel::INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  Logger::instance().log(LogLevel::WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) Logger::instance().log(LogLevel::ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_FATAL(...) Logger::instance().log(LogLevel::FATAL, __FILE__, __LINE__, __VA_ARGS__)
#endif // AETHER_BASE_LOGGER_H
