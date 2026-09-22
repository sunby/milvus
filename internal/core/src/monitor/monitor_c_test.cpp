// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "cachinglayer/Metrics.h"
#include "monitor/monitor_c.h"

namespace {

using milvus::cachinglayer::CellDataType;
using milvus::cachinglayer::monitor::
    create_cache_shard_disk_usage_metric_handle;

std::set<std::string>
CacheMetricSamples() {
    auto* metrics = GetCoreMetrics();
    std::istringstream input(metrics);
    free(metrics);
    std::set<std::string> samples;
    std::string line;
    while (std::getline(input, line)) {
        if (line.rfind("internal_cache_shard_disk_usage_bytes{", 0) == 0) {
            samples.insert(line);
        }
    }
    return samples;
}

using ShardStats = std::map<std::pair<std::string, std::string>, double>;

ShardStats
CacheBusinessStats() {
    auto stats = GetCacheShardDiskUsageStats();
    ShardStats result;
    for (int64_t i = 0; i < stats.len; ++i) {
        result[{stats.stats[i].data_type, stats.stats[i].shard}] =
            stats.stats[i].disk_bytes;
    }
    DeleteCacheShardDiskUsageStats(stats);
    return result;
}

void
CheckCacheMetrics(bool aggregate) {
    // An early scrape must not freeze the default mode before startup reads
    // the configuration.
    EXPECT_TRUE(CacheMetricSamples().empty());
    ASSERT_TRUE(InitCacheShardDiskUsageMetricsMode(aggregate));
    ASSERT_TRUE(InitCacheShardDiskUsageMetricsMode(aggregate));
    ASSERT_FALSE(InitCacheShardDiskUsageMetricsMode(!aggregate));

    auto first = create_cache_shard_disk_usage_metric_handle(
        CellDataType::VECTOR_INDEX, "shard_a");
    auto second = create_cache_shard_disk_usage_metric_handle(
        CellDataType::VECTOR_INDEX, "shard_b");
    auto scalar = create_cache_shard_disk_usage_metric_handle(
        CellDataType::SCALAR_INDEX, "shard_a");
    first->Increment(10);
    second->Increment(20);
    scalar->Increment(5);

    ShardStats expected_stats{{{"vector_index", "shard_a"}, 10},
                              {{"vector_index", "shard_b"}, 20},
                              {{"scalar_index", "shard_a"}, 5}};
    EXPECT_EQ(CacheBusinessStats(), expected_stats);

    std::set<std::string> expected_samples;
    if (aggregate) {
        expected_samples = {
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"all\"} 30",
            "internal_cache_shard_disk_usage_bytes{data_type=\"scalar_index\","
            "shard=\"all\"} 5"};
    } else {
        expected_samples = {
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"shard_a\"} 10",
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"shard_b\"} 20",
            "internal_cache_shard_disk_usage_bytes{data_type=\"scalar_index\","
            "shard=\"shard_a\"} 5"};
    }
    EXPECT_EQ(CacheMetricSamples(), expected_samples);

    // CacheCell unload refunds its bytes before its slot releases the handle.
    first->Decrement(10);
    first.reset();
    expected_stats.erase({"vector_index", "shard_a"});
    EXPECT_EQ(CacheBusinessStats(), expected_stats);
    if (aggregate) {
        expected_samples.erase(
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"all\"} 30");
        expected_samples.insert(
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"all\"} 20");
    } else {
        expected_samples.erase(
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"shard_a\"} 10");
    }
    EXPECT_EQ(CacheMetricSamples(), expected_samples);

    second->Decrement(20);
    scalar->Decrement(5);
    second.reset();
    scalar.reset();
    // Scrape must clean full-mode series without requiring a distribution RPC.
    if (aggregate) {
        expected_samples = {
            "internal_cache_shard_disk_usage_bytes{data_type=\"vector_index\","
            "shard=\"all\"} 0",
            "internal_cache_shard_disk_usage_bytes{data_type=\"scalar_index\","
            "shard=\"all\"} 0"};
    } else {
        expected_samples.clear();
    }
    EXPECT_EQ(CacheMetricSamples(), expected_samples);
    EXPECT_TRUE(CacheBusinessStats().empty());
}

// Each mode needs a fresh process because the production configuration cannot
// be changed once initialized. Re-exec also avoids inheriting cache handles or
// registry locks from other native tests.
class CacheShardDiskUsageMetricsDeathTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        GTEST_FLAG_SET(death_test_style, "threadsafe");
    }
};

TEST_F(CacheShardDiskUsageMetricsDeathTest, Aggregate) {
    ASSERT_EXIT(
        {
            CheckCacheMetrics(true);
            std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(CacheShardDiskUsageMetricsDeathTest, Full) {
    ASSERT_EXIT(
        {
            CheckCacheMetrics(false);
            std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

TEST_F(CacheShardDiskUsageMetricsDeathTest, RejectsSwitchAfterFirstHandle) {
    ASSERT_EXIT(
        {
            auto handle = create_cache_shard_disk_usage_metric_handle(
                CellDataType::VECTOR_INDEX, "shard_a");
            EXPECT_FALSE(InitCacheShardDiskUsageMetricsMode(true));
            EXPECT_TRUE(InitCacheShardDiskUsageMetricsMode(false));
            std::_Exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0),
        "");
}

}  // namespace
