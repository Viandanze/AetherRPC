#ifndef AETHER_BASE_NONCOPYABLE_H
#define AETHER_BASE_NONCOPYABLE_H
#pragma once
// Base class that disables copying, similar to muduo's noncopyable
class noncopyable {
public:
    noncopyable() = default;
    ~noncopyable() = default;
    noncopyable(const noncopyable&) = delete;
    noncopyable& operator=(const noncopyable&) = delete;
};
#endif // AETHER_BASE_NONCOPYABLE_H
