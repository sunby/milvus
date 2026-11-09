// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "exec/operator/VectorSearchNode.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentGrowingImpl.h"

namespace milvus::exec {
namespace {

TEST(VectorSearchNodeTest, JoinsVectorPrefetchOnlyWhenANNConsumesInput) {
    class JoinProbe : public PhyVectorSearchNode {
     public:
        using PhyVectorSearchNode::PhyVectorSearchNode;
        void
        WaitPrefetch() override {
            ++joins;
            throw std::runtime_error("reached vector index consumption");
        }
        int joins = 0;
    };
    auto schema = std::make_shared<Schema>();
    auto field = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->set_primary_field_id(schema->AddDebugField("pk", DataType::INT64));
    auto segment = segcore::CreateGrowingSegment(schema, empty_index_meta);
    auto ctx = std::make_shared<QueryContext>(
        "vector_join", segment.get(), 1, MAX_TIMESTAMP);
    SearchInfo info{};
    info.field_id_ = field;
    info.topk_ = 1;
    ctx->set_search_info(info);
    query::PlaceholderGroup placeholders;
    placeholders.emplace_back().num_of_queries_ = 1;
    ctx->set_placeholder_group(&placeholders);
    auto node = std::make_shared<plan::VectorSearchNode>("ann");
    auto task = Task::Create("vector_join", plan::PlanFragment(node), 0, ctx);
    DriverContext driver(task, 0, 0, 0, 0);

    for (bool all_filtered : {true, false}) {
        JoinProbe probe(0, &driver, node);
        EXPECT_EQ(probe.GetOutput(), nullptr);
        auto bitmap = std::make_shared<ColumnVector>(
            TargetBitmap(1, all_filtered), TargetBitmap(1, true));
        auto input =
            std::make_shared<RowVector>(std::vector<VectorPtr>{bitmap});
        probe.AddInput(input);
        EXPECT_EQ(probe.GetOutput(), nullptr);
        EXPECT_EQ(probe.joins, 0);
        probe.NoMoreInput();
        if (all_filtered) {
            EXPECT_NE(probe.GetOutput(), nullptr);
            EXPECT_EQ(probe.joins, 0);
        } else {
            EXPECT_THROW(probe.GetOutput(), std::runtime_error);
            EXPECT_EQ(probe.joins, 1);
        }
    }
}

}  // namespace
}  // namespace milvus::exec
