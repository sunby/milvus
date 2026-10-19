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
#include <arrow/io/memory.h>
#include <arrow/api.h>
#include <boost/filesystem.hpp>
#include <unistd.h>

#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/Json.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/JsonNullOffsets.h"
#include "index/JsonScalarIndexWrapper.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"
#include "storage/Util.h"
#include "storage/ChunkManager.h"
#include "folly/ScopeGuard.h"

namespace milvus::index {
namespace {

class JsonPackedWriter : public storage::IndexEntryWriter {
 public:
    void
    WriteEntry(const std::string& name,
               const void* data,
               size_t size) override {
        auto& bytes = entries[name];
        bytes.resize(size);
        if (size != 0) {
            std::memcpy(bytes.data(), data, size);
        }
    }

    void
    WriteEntry(const std::string& name, int fd, size_t size) override {
        auto& bytes = entries[name];
        bytes.resize(size);
        size_t offset = 0;
        while (offset < size) {
            auto n = ::pread(fd, bytes.data() + offset, size - offset, offset);
            ASSERT_GT(n, 0);
            offset += n;
        }
    }

    void
    Finish() override {
    }
    size_t
    GetTotalBytesWritten() const override {
        return 0;
    }
    nlohmann::json&
    Meta() {
        return meta_json_;
    }
    std::map<std::string, std::vector<uint8_t>> entries;
};

std::unique_ptr<storage::IndexEntryReader>
PackJsonEntries(JsonPackedWriter& captured) {
    auto buffer = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto output = std::make_shared<storage::RemoteOutputStream>(buffer);
    storage::IndexEntryDirectStreamWriter writer(output);
    for (const auto& [name, bytes] : captured.entries) {
        writer.WriteEntry(name, bytes.data(), bytes.size());
    }
    for (auto it = captured.Meta().begin(); it != captured.Meta().end(); ++it) {
        writer.PutMeta(it.key(), it.value());
    }
    writer.Finish();
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            buffer->Finish().ValueOrDie()));
    return storage::IndexEntryReader::Open(input, input->Size());
}

void
ExpectJsonOffsets(const TargetBitmap& bitmap,
                  const std::vector<size_t>& offsets) {
    EXPECT_EQ(bitmap.count(), offsets.size());
    for (auto offset : offsets) {
        EXPECT_TRUE(bitmap[offset]) << offset;
    }
}

template <typename T>
void
CheckPackedJsonNulls(const char* cast_name,
                     const std::vector<std::string>& raw,
                     const std::vector<size_t>& valid,
                     const std::vector<size_t>& exists,
                     bool nullable_last = false,
                     const char* cast_function = "unknown",
                     bool array = false) {
    const auto root =
        (boost::filesystem::temp_directory_path() /
         boost::filesystem::unique_path("json-packed-null-%%%%-%%%%"))
            .string();
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = root;
    auto cm = storage::CreateChunkManager(config);
    auto fs = storage::InitArrowFileSystem(config);
    auto cleanup =
        folly::makeGuard([&] { boost::filesystem::remove_all(root); });
    storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(proto::schema::JSON);
    field_meta.field_schema.set_fieldid(101);
    field_meta.field_schema.set_nullable(nullable_last);
    storage::IndexMeta index_meta;
    index_meta.segment_id = 3;
    index_meta.field_id = 101;
    index_meta.build_id = 4000;
    index_meta.index_version = 1;
    storage::FileManagerContext context(field_meta, index_meta, cm, fs);

    CreateIndexInfo info{};
    info.field_type = DataType::JSON;
    info.index_type = INVERTED_INDEX_TYPE;
    info.scalar_index_engine_version = 3;
    info.json_cast_type = JsonCastType::FromString(cast_name);
    info.json_path = "/a";
    info.json_cast_function = cast_function;

    auto index = IndexFactory::GetInstance().CreateJsonIndex(info, context);
    auto* json = static_cast<JsonInvertedIndex<T>*>(index.get());
    std::vector<Json> documents;
    for (const auto& row : raw) {
        documents.emplace_back(simdjson::padded_string(row));
    }
    auto field =
        std::make_shared<FieldData<Json>>(DataType::JSON, nullable_last);
    if (nullable_last) {
        arrow::BinaryBuilder builder;
        for (size_t i = 0; i < raw.size(); ++i) {
            if (i + 1 == raw.size()) {
                ASSERT_TRUE(builder.AppendNull().ok());
            } else {
                ASSERT_TRUE(builder.Append(raw[i]).ok());
            }
        }
        std::shared_ptr<arrow::Array> data;
        ASSERT_TRUE(builder.Finish(&data).ok());
        field->FillFieldData(data);
    } else {
        field->add_json_data(documents);
    }
    json->BuildWithFieldData({field});
    JsonPackedWriter captured;
    json->WriteEntries(&captured);
    EXPECT_EQ(captured.Meta().contains(kJsonNullOffsetsComplete), !array);
    if (!array) {
        EXPECT_EQ(captured.Meta().at(kJsonNullOffsetsComplete), true);
    }
    EXPECT_EQ(captured.Meta().at("has_null"), valid.size() != raw.size());
    auto original = captured.entries;
    auto original_meta = captured.Meta();

    // Unmarked historical V3 fixtures contain only whole-field nulls, even
    // though Tantivy has no value for missing paths and failed casts.
    for (int legacy = 0; legacy < (array ? 1 : 3); ++legacy) {
        SCOPED_TRACE(legacy);
        captured.entries = original;
        captured.Meta() = original_meta;
        if (legacy != 0) {
            captured.Meta().erase(kJsonNullOffsetsComplete);
            if (legacy == 2) {
                captured.Meta()[kJsonNullOffsetsComplete] = false;
            }
            captured.Meta()["has_null"] = nullable_last;
            captured.entries.erase(INDEX_NULL_OFFSET_FILE_NAME);
            if (nullable_last) {
                const size_t offset = raw.size() - 1;
                auto& bytes = captured.entries[INDEX_NULL_OFFSET_FILE_NAME];
                bytes.resize(sizeof(offset));
                std::memcpy(bytes.data(), &offset, sizeof(offset));
            }
        }
        for (const bool mmap : {false, true}) {
            SCOPED_TRACE(mmap);
            auto packed = PackJsonEntries(captured);
            context.set_for_loading_index(true);
            auto loaded =
                IndexFactory::GetInstance().CreateJsonIndex(info, context);
            auto* reader = static_cast<JsonInvertedIndex<T>*>(loaded.get());
            reader->LoadEntries(*packed, {{ENABLE_MMAP, mmap}});
            EXPECT_EQ(reader->Count(), raw.size());
            for (int repeat = 0; repeat < 3; ++repeat) {
                ExpectJsonOffsets(reader->IsNotNull(), valid);
                EXPECT_EQ(reader->IsNull().count(), raw.size() - valid.size());
                ExpectJsonOffsets(reader->Exists(), exists);
                if (!array) {
                    T unused{};
                    ExpectJsonOffsets(reader->NotIn(0, &unused), valid);
                }
            }
        }
    }
}

TEST(JsonPackedNullOffsetsTest, DoubleMissingCastFailureAndFieldNull) {
    CheckPackedJsonNulls<double>("DOUBLE",
                                 {R"({"a":1})",
                                  R"({"a":0})",
                                  R"({"a":"bad"})",
                                  "{}",
                                  R"({"a":null})",
                                  R"({"a":2})"},
                                 {0, 1},
                                 {0, 1, 2},
                                 true);
}

TEST(JsonPackedNullOffsetsTest, FalseAndEmptyStringRemainComparable) {
    CheckPackedJsonNulls<bool>(
        "BOOL",
        {R"({"a":false})", R"({"a":true})", R"({"a":0})", "{}"},
        {0, 1},
        {0, 1, 2});
    CheckPackedJsonNulls<std::string>(
        "VARCHAR",
        {R"({"a":""})", R"({"a":"x"})", R"({"a":1})", "{}"},
        {0, 1},
        {0, 1, 2});
}

TEST(JsonPackedNullOffsetsTest, CompleteAllValidAndAllInvalid) {
    CheckPackedJsonNulls<double>(
        "DOUBLE", {R"({"a":0})", R"({"a":1})"}, {0, 1}, {0, 1});
    CheckPackedJsonNulls<double>(
        "DOUBLE", {R"({"a":"bad"})", "{}", R"({"a":null})"}, {}, {0});
}

TEST(JsonPackedNullOffsetsTest, CastFiniteNumericDomain) {
    CheckPackedJsonNulls<double>("DOUBLE",
                                 {R"({"a":"1.5"})",
                                  R"({"a":"nan"})",
                                  R"({"a":"inf"})",
                                  R"({"a":"-inf"})",
                                  R"({"a":"bad"})",
                                  R"({"a":"1.7976931348623157e308"})",
                                  R"({"a":"-1.7976931348623157e308"})"},
                                 {0, 5, 6},
                                 {0, 1, 2, 3, 4, 5, 6},
                                 false,
                                 "STRING_TO_DOUBLE");
}

TEST(JsonPackedNullOffsetsTest, AllInvalidWithExistingPaths) {
    CheckPackedJsonNulls<double>(
        "DOUBLE", {R"({"a":"bad"})", R"({"a":false})"}, {}, {0, 1});
}

TEST(JsonPackedNullOffsetsTest, ArrayValidityIsUnchanged) {
    CheckPackedJsonNulls<double>(
        "ARRAY_DOUBLE",
        {R"({"a":[]})", R"({"a":[1]})", "{}", R"({"a":null})"},
        {0, 1, 2, 3},
        // Json::exist treats an empty array as absent on all three branches.
        {1},
        false,
        "unknown",
        true);
}

}  // namespace
}  // namespace milvus::index
