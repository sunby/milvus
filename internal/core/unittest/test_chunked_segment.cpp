// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include "arrow/table_builder.h"
#include "arrow/type_fwd.h"
#include "common/BitsetView.h"
#include "common/FieldDataInterface.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "knowhere/comp/index_param.h"
#include "mmap/ChunkedColumn.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/SearchOnSealed.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealedImpl.h"
#include "test_utils/DataGen.h"
#include <numeric>
#include <vector>

struct DeferRelease {
    using functype = std::function<void()>;
    void
    AddDefer(const functype& closure) {
        closures.push_back(closure);
    }

    ~DeferRelease() {
        for (auto& closure : closures) {
            closure();
        }
    }

    std::vector<functype> closures;
};

using namespace milvus;
TEST(test_chunk_segment, TestSearchOnSealed) {
    DeferRelease defer;

    int dim = 16;
    int chunk_num = 3;
    int chunk_size = 100;
    int total_row_count = chunk_num * chunk_size;
    int bitset_size = (total_row_count + 7) / 8;
    int chunk_bitset_size = (chunk_size + 7) / 8;

    auto column = std::make_shared<ChunkedColumn>();
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::COSINE);

    for (int i = 0; i < chunk_num; i++) {
        auto dataset = segcore::DataGen(schema, chunk_size);
        auto data = dataset.get_col<float>(fakevec_id);
        auto buf_size = chunk_bitset_size + 4 * data.size();

        char* buf = new char[buf_size];
        defer.AddDefer([buf]() { delete[] buf; });
        memcpy(buf + chunk_bitset_size, data.data(), 4 * data.size());

        auto chunk = std::make_shared<FixedWidthChunk>(
            chunk_size, dim, buf, buf_size, 4, false);
        column->AddChunk(chunk);
    }

    SearchInfo search_info;
    auto search_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
    };
    search_info.search_params_ = search_conf;
    search_info.field_id_ = fakevec_id;
    search_info.metric_type_ = knowhere::metric::COSINE;
    // expect to return all rows
    search_info.topk_ = total_row_count;

    uint8_t* bitset_data = new uint8_t[bitset_size];
    defer.AddDefer([bitset_data]() { delete[] bitset_data; });
    std::fill(bitset_data, bitset_data + bitset_size, 0);
    BitsetView bv(bitset_data, total_row_count);

    auto query_ds = segcore::DataGen(schema, 1);
    auto col_query_data = query_ds.get_col<float>(fakevec_id);
    auto query_data = col_query_data.data();
    SearchResult search_result;

    query::SearchOnSealed(*schema,
                          column,
                          search_info,
                          query_data,
                          1,
                          chunk_size * chunk_num,
                          bv,
                          search_result);

    std::set<int64_t> offsets;
    for (auto& offset : search_result.seg_offsets_) {
        if (offset != -1) {
            offsets.insert(offset);
        }
    }
    // check all rows are returned
    ASSERT_EQ(total_row_count, offsets.size());
    for (int i = 0; i < total_row_count; i++) {
        ASSERT_TRUE(offsets.find(i) != offsets.end());
    }
}

TEST(test_chunk_segment, TestTermExpr) {
    auto schema = std::make_shared<Schema>();
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, true);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT32, true);
    auto segment =
        segcore::CreateSealedSegment(schema,
                                     nullptr,
                                     -1,
                                     segcore::SegcoreConfig::default_config(),
                                     false,
                                     false,
                                     true);
    // generate test data
    std::shared_ptr<arrow::Schema> arrow_schema;
    auto arrow_i32_field = arrow::field("int32", arrow::int32());
    auto arrow_pk_field = arrow::field("pk", arrow::int32());
    arrow_schema = arrow::schema({arrow_i32_field, arrow_pk_field});

    size_t test_data_count = 1000;
    std::vector<int32_t> test_data(test_data_count);
    std::iota(test_data.begin(), test_data.end(), 0);
    auto builder = std::make_shared<arrow::Int32Builder>();
    auto status = builder->AppendValues(test_data.begin(), test_data.end());
    ASSERT_TRUE(status.ok());
    auto res = builder->Finish();
    ASSERT_TRUE(res.ok());
    std::shared_ptr<arrow::Array> arrow_int32;
    arrow_int32 = res.ValueOrDie();

    auto record_batch = arrow::RecordBatch::Make(
        arrow_schema, arrow_int32->length(), {arrow_int32});

    // int32 field data
    auto res2 = arrow::RecordBatchReader::Make({record_batch});
    ASSERT_TRUE(res2.ok());
    auto arrow_reader = res2.ValueOrDie();
    res2 = arrow::RecordBatchReader::Make({record_batch});
    ASSERT_TRUE(res2.ok());
    auto arrow_reader2 = res2.ValueOrDie();

    // pk field data
    res2 = arrow::RecordBatchReader::Make({record_batch});
    ASSERT_TRUE(res2.ok());
    auto arrow_pk_reader = res2.ValueOrDie();
    res2 = arrow::RecordBatchReader::Make({record_batch});
    ASSERT_TRUE(res2.ok());
    auto arrow_pk_reader2 = res2.ValueOrDie();

    // load int32 field
    FieldDataInfo i32_field_info;
    i32_field_info.field_id = int32_fid.get();
    i32_field_info.row_count = test_data_count * 2;
    i32_field_info.arrow_reader_channel->push(
        std::make_shared<ArrowDataWrapper>(arrow_reader, nullptr, nullptr));
    i32_field_info.arrow_reader_channel->push(
        std::make_shared<ArrowDataWrapper>(arrow_reader2, nullptr, nullptr));
    i32_field_info.arrow_reader_channel->close();
    segment->LoadFieldData(int32_fid, i32_field_info);

    // load pk field
    FieldDataInfo pk_field_info;
    pk_field_info.field_id = pk_fid.get();
    pk_field_info.row_count = test_data_count * 2;
    pk_field_info.arrow_reader_channel->push(
        std::make_shared<ArrowDataWrapper>(arrow_pk_reader, nullptr, nullptr));
    pk_field_info.arrow_reader_channel->push(
        std::make_shared<ArrowDataWrapper>(arrow_pk_reader2, nullptr, nullptr));
    pk_field_info.arrow_reader_channel->close();
    segment->LoadFieldData(pk_fid, pk_field_info);

    // query int32 expr
    std::vector<proto::plan::GenericValue> filter_data;
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue v;
        v.set_int64_val(i);
        filter_data.push_back(v);
    }
    auto term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int32_fid, DataType::INT32), filter_data);
    BitsetType final;
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), 2 * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(20, final.count());

    // query pk expr
    auto pk_term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(pk_fid, DataType::INT32), filter_data);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  pk_term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), 2 * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(20, final.count());
}
