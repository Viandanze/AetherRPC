// Unit tests for the load balancing policies (no ZooKeeper, no network).
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "rpc/LoadBalancer.h"
#include "test_framework.h"

using namespace aether::rpc;

void test_round_robin_distribution() {
    TEST_SUITE("RoundRobin distribution");
    RoundRobinLoadBalancer lb;
    std::vector<std::string> eps{"a", "b", "c"};

    std::map<std::string, int> counts;
    for (int i = 0; i < 90; ++i) ++counts[lb.select(eps)];
    ASSERT_EQ(counts["a"], 30);
    ASSERT_EQ(counts["b"], 30);
    ASSERT_EQ(counts["c"], 30);
}

void test_round_robin_rotation_order() {
    TEST_SUITE("RoundRobin rotation order");
    RoundRobinLoadBalancer lb;
    std::vector<std::string> eps{"x", "y"};
    ASSERT_STREQ(lb.select(eps), "x");
    ASSERT_STREQ(lb.select(eps), "y");
    ASSERT_STREQ(lb.select(eps), "x");

    // Candidate list shrinks between calls: still works, counter keeps
    // advancing modulo the current size.
    std::vector<std::string> one{"z"};
    ASSERT_STREQ(lb.select(one), "z");
    ASSERT_STREQ(lb.select(one), "z");
}

void test_round_robin_empty() {
    TEST_SUITE("RoundRobin edge cases");
    RoundRobinLoadBalancer lb;
    std::vector<std::string> empty;
    ASSERT_STREQ(lb.select(empty), "");

    std::vector<std::string> single{"only"};
    ASSERT_STREQ(lb.select(single), "only");
}

void test_random_lb() {
    TEST_SUITE("RandomLoadBalancer");
    RandomLoadBalancer lb;
    std::vector<std::string> empty;
    ASSERT_STREQ(lb.select(empty), "");

    std::vector<std::string> single{"only"};
    ASSERT_STREQ(lb.select(single), "only");

    // Over many picks both candidates must appear (statistical, but a
    // 1000-draw 50/50 test failing has probability ~1e-300).
    std::vector<std::string> eps{"a", "b"};
    std::map<std::string, int> counts;
    for (int i = 0; i < 1000; ++i) ++counts[lb.select(eps)];
    ASSERT_TRUE(counts["a"] > 0);
    ASSERT_TRUE(counts["b"] > 0);
    ASSERT_EQ(counts["a"] + counts["b"], 1000);
}

void test_round_robin_thread_safety() {
    TEST_SUITE("RoundRobin concurrent select");
    RoundRobinLoadBalancer lb;
    std::vector<std::string> eps{"a", "b"};
    std::atomic<int> picks{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < 1000; ++i) {
                std::string s = lb.select(eps);
                if (!s.empty()) ++picks;
            }
        });
    }
    for (auto& th : threads) th.join();
    ASSERT_EQ(picks.load(), 4000);  // every select returned a candidate
}

int main() {
    test_round_robin_distribution();
    test_round_robin_rotation_order();
    test_round_robin_empty();
    test_random_lb();
    test_round_robin_thread_safety();
    TEST_SUMMARY();
}
