#pragma once
// Minimal test framework for Aether — no external dependencies
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

static int g_test_count = 0;
static int g_test_passed = 0;
static int g_test_failed = 0;
static std::string g_current_suite;

#define TEST_SUITE(name) \
    g_current_suite = name; \
    printf("\n── %s ──\n", name)

#define ASSERT_TRUE(expr) \
    do { \
        g_test_count++; \
        if (expr) { g_test_passed++; } \
        else { g_test_failed++; \
            printf("  FAIL: %s:%d: ASSERT_TRUE(%s)\n", __FILE__, __LINE__, #expr); } \
    } while(0)

#define ASSERT_FALSE(expr) \
    do { \
        g_test_count++; \
        if (!(expr)) { g_test_passed++; } \
        else { g_test_failed++; \
            printf("  FAIL: %s:%d: ASSERT_FALSE(%s)\n", __FILE__, __LINE__, #expr); } \
    } while(0)

#define ASSERT_EQ(a, b) \
    do { \
        g_test_count++; \
        if ((a) == (b)) { g_test_passed++; } \
        else { g_test_failed++; \
            printf("  FAIL: %s:%d: ASSERT_EQ(%s, %s)\n", __FILE__, __LINE__, #a, #b); } \
    } while(0)

#define ASSERT_NE(a, b) \
    do { \
        g_test_count++; \
        if ((a) != (b)) { g_test_passed++; } \
        else { g_test_failed++; \
            printf("  FAIL: %s:%d: ASSERT_NE(%s, %s)\n", __FILE__, __LINE__, #a, #b); } \
    } while(0)

#define ASSERT_STREQ(a, b) \
    do { \
        g_test_count++; \
        std::string _sa(a); std::string _sb(b); \
        if (_sa == _sb) { g_test_passed++; } \
        else { g_test_failed++; \
            printf("  FAIL: %s:%d: ASSERT_STREQ\n    expected: \"%s\"\n    actual:   \"%s\"\n", \
                __FILE__, __LINE__, _sb.c_str(), _sa.c_str()); } \
    } while(0)

#define ASSERT_CONTAINS(haystack, needle) \
    do { \
        g_test_count++; \
        std::string _h(haystack); std::string _n(needle); \
        if (_h.find(_n) != std::string::npos) { g_test_passed++; } \
        else { g_test_failed++; \
            printf("  FAIL: %s:%d: ASSERT_CONTAINS\n    haystack: \"%s\"\n    needle:   \"%s\"\n", \
                __FILE__, __LINE__, _h.c_str(), _n.c_str()); } \
    } while(0)

#define TEST_SUMMARY() \
    printf("\n═══════════════════════════════════════\n"); \
    printf("  Total: %d | Passed: %d | Failed: %d\n", g_test_count, g_test_passed, g_test_failed); \
    printf("  Result: %s\n", g_test_failed == 0 ? "✅ ALL PASSED" : "❌ SOME FAILED"); \
    printf("═══════════════════════════════════════\n"); \
    return g_test_failed == 0 ? 0 : 1
