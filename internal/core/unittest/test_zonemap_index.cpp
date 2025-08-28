#include "index/ZonemapIndex.h"
#include "parquet/metadata.h"
#include "parquet/statistics.h"
#include <gtest/gtest.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/types.h>

using ParquetFileMetadatas =
    std::vector<std::shared_ptr<parquet::FileMetaData>>;

std::shared_ptr<parquet::FileMetaData>
MakeFileMetaData(const std::vector<int32_t>& data) {
    std::vector<parquet::schema::NodePtr> fields;
    fields.push_back(
        parquet::schema::PrimitiveNode::Make("id",
                                             parquet::Repetition::REQUIRED,
                                             parquet::Type::INT32,
                                             parquet::ConvertedType::INT_32));
    auto schema = parquet::schema::GroupNode::Make(
        "schema", parquet::Repetition::REPEATED, fields);
    parquet::SchemaDescriptor schema_descriptor;
    schema_descriptor.Init(schema);
    auto column_descriptor = schema_descriptor.Column(0);
    auto builder = parquet::FileMetaDataBuilder::Make(
        &schema_descriptor, parquet::default_writer_properties());
    auto rgbuilder = builder->AppendRowGroup();
    auto column_builder = rgbuilder->NextColumnChunk();
    auto stats = parquet::MakeStatistics<parquet::Int32Type>(column_descriptor);
    stats->Update(data.data(), data.size(), 0);
    column_builder->SetStatistics(stats->Encode());
    rgbuilder->Finish(0);
    return builder->Finish();
}

TEST(TestZonemapIndex, TestZonemapIndexSorted) {
    int file_num = 5;
    int row_num = 100;
    std::vector<std::vector<int32_t>> data(file_num);
    for (int i = 0; i < file_num; i++) {
        for (int j = 0; j < row_num; j++) {
            data[i].push_back(j + i * row_num);
        }
    }

    ParquetFileMetadatas metadatas;
    for (int i = 0; i < file_num; i++) {
        metadatas.push_back(MakeFileMetaData(data[i]));
    }
    auto zonemap_index = milvus::index::ZonemapIndex<
        int32_t>::MakeZonemapIndexFromParquetMetadata(metadatas, 0, true);
    auto iterator = zonemap_index->NewIterator();
    // scan all the blocks
    for (int i = 0; i < file_num; i++, iterator->Next()) {
        ASSERT_TRUE(iterator->Valid());
        ASSERT_EQ(iterator->BlockID(), i);
    }
    ASSERT_FALSE(iterator->Valid());

    // seek to some value
    iterator = zonemap_index->NewIterator();
    ASSERT_TRUE(iterator->Valid());
    iterator->Seek(150);
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ(iterator->BlockID(), 1);
    iterator->Next();
    ASSERT_FALSE(iterator->Valid());

    // seek to invalid value
    iterator = zonemap_index->NewIterator();
    ASSERT_TRUE(iterator->Valid());
    iterator->Seek(100000);
    ASSERT_FALSE(iterator->Valid());
}

TEST(TestZonemapIndex, TestZonemapIndexUnsorted) {
    int file_num = 5;
    int row_num = 100;
    std::vector<std::vector<int32_t>> data(file_num);
    for (int i = 0; i < file_num; i++) {
        for (int j = 0; j < row_num; j++) {
            data[i].push_back(j);
        }
    }

    ParquetFileMetadatas metadatas;
    for (int i = 0; i < file_num; i++) {
        metadatas.push_back(MakeFileMetaData(data[i]));
    }
    auto zonemap_index = milvus::index::ZonemapIndex<
        int32_t>::MakeZonemapIndexFromParquetMetadata(metadatas, 0, false);

    // scan all the blocks
    auto iterator = zonemap_index->NewIterator();
    for (int i = 0; i < file_num; i++, iterator->Next()) {
        ASSERT_TRUE(iterator->Valid());
        ASSERT_EQ(iterator->BlockID(), i);
    }
    ASSERT_FALSE(iterator->Valid());

    // seek to some value
    iterator = zonemap_index->NewIterator();
    ASSERT_TRUE(iterator->Valid());
    iterator->Seek(10);
    // all the blocks should be scanned
    for (int i = 0; i < file_num; i++, iterator->Next()) {
        ASSERT_TRUE(iterator->Valid());
        ASSERT_EQ(iterator->BlockID(), i);
    }

    // seek to invalid value
    iterator = zonemap_index->NewIterator();
    ASSERT_TRUE(iterator->Valid());
    iterator->Seek(100000);
    ASSERT_FALSE(iterator->Valid());
}