// Unit tests for TimeStamp class
// Tests: now, after, secondsFromNow, diffSeconds, comparison operators, toTimeSpec, nowString
#include "test_framework.h"
#include "base/TimeStamp.h"
#include <thread>

void test_timestamp_now() {
    TEST_SUITE("TimeStamp now()");
    TimeStamp t = TimeStamp::now();
    // now() should be very close to current time
    double diff = t.secondsFromNow();
    ASSERT_TRUE(diff <= 0.1);   // Should be within 100ms
    ASSERT_TRUE(diff >= -0.1);
}

void test_timestamp_after() {
    TEST_SUITE("TimeStamp after()");
    TimeStamp t = TimeStamp::after(5.0);
    double diff = t.secondsFromNow();
    ASSERT_TRUE(diff > 4.9 && diff < 5.1);
}

void test_timestamp_past() {
    TEST_SUITE("TimeStamp Past");
    TimeStamp past = TimeStamp::after(-1.0);
    double diff = past.secondsFromNow();
    ASSERT_TRUE(diff < 0);  // Negative = in the past
}

void test_timestamp_diff_seconds() {
    TEST_SUITE("TimeStamp diffSeconds()");
    TimeStamp t1 = TimeStamp::now();
    TimeStamp t2 = TimeStamp::after(2.0);
    double diff = t2.diffSeconds(t1);
    ASSERT_TRUE(diff > 1.9 && diff < 2.1);

    // Reverse should be negative
    double reverse = t1.diffSeconds(t2);
    ASSERT_TRUE(reverse < -1.9 && reverse > -2.1);
}

void test_timestamp_comparison() {
    TEST_SUITE("TimeStamp Comparison");
    TimeStamp t1 = TimeStamp::after(0);
    TimeStamp t2 = TimeStamp::after(1.0);

    ASSERT_TRUE(t1 < t2);
    ASSERT_TRUE(t2 > t1);
    ASSERT_TRUE(t1 <= t2);
    ASSERT_TRUE(t2 >= t1);
    // t2 and t3 are both ~1s in the future, but not necessarily equal
    // So we test with same timestamp
    TimeStamp same = t2;
    ASSERT_TRUE(same == t2);
    ASSERT_TRUE(same <= t2);
    ASSERT_TRUE(same >= t2);
}

void test_timestamp_to_timespec() {
    TEST_SUITE("TimeStamp toTimeSpec()");
    TimeStamp t = TimeStamp::now();
    struct timespec ts = t.toTimeSpec();
    // timespec should have valid values
    ASSERT_TRUE(ts.tv_sec >= 0);
    ASSERT_TRUE(ts.tv_nsec >= 0 && ts.tv_nsec < 1000000000L);
}

void test_timestamp_now_string() {
    TEST_SUITE("TimeStamp nowString()");
    std::string s = TimeStamp::nowString();
    // Should be a non-empty date string like "2026-07-31 22:00:00"
    ASSERT_TRUE(s.size() >= 19);
    ASSERT_TRUE(s.find('-') != std::string::npos);
    ASSERT_TRUE(s.find(':') != std::string::npos);
}

int main() {
    test_timestamp_now();
    test_timestamp_after();
    test_timestamp_past();
    test_timestamp_diff_seconds();
    test_timestamp_comparison();
    test_timestamp_to_timespec();
    test_timestamp_now_string();
    TEST_SUMMARY();
}
