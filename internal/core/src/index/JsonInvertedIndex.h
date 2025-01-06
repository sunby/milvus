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

#pragma once
#include "common/FieldDataInterface.h"
#include "index/InvertedIndexTantivy.h"
#include "storage/FileManager.h"
#include "boost/filesystem.hpp"
#include "tantivy-binding.h"

namespace milvus::index {

template <typename T>
class JsonInvertedIndex : public index::InvertedIndexTantivy<T> {
 public:
    JsonInvertedIndex(const std::string& nested_path,
                      const storage::FileManagerContext& ctx)
        : nested_path_(nested_path) {
        this->schema_ = ctx.fieldDataMeta.field_schema;
        this->mem_file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(ctx);
        this->disk_file_manager_ =
            std::make_shared<storage::DiskFileManagerImpl>(ctx);

        if (ctx.for_loading_index) {
            return;
        }
        auto prefix = this->disk_file_manager_->GetTextIndexIdentifier();
        constexpr const char* TMP_INVERTED_INDEX_PREFIX =
            "/tmp/milvus/inverted-index/";
        this->path_ = std::string(TMP_INVERTED_INDEX_PREFIX) + prefix;

        if constexpr (std::is_same_v<bool, T>) {
            this->d_type_ = TantivyDataType::Bool;
        } else if constexpr (std::is_same_v<int8_t, T> ||
                             std::is_same_v<int16_t, T> ||
                             std::is_same_v<int32_t, T> ||
                             std::is_same_v<int64_t, T>) {
            this->d_type_ = TantivyDataType::I64;
        } else if constexpr (std::is_same_v<float, T> ||
                             std::is_same_v<double, T>) {
            this->d_type_ = TantivyDataType::F64;
        } else {
            this->d_type_ = TantivyDataType::Keyword;
        }

        boost::filesystem::create_directories(this->path_);
        std::string field_name = std::to_string(
            this->disk_file_manager_->GetFieldDataMeta().field_id);
        this->wrapper_ = std::make_shared<index::TantivyIndexWrapper>(
            field_name.c_str(), this->d_type_, this->path_.c_str());
    }

    void
    build_index_for_json(const std::vector<std::shared_ptr<FieldDataBase>>&
                             field_datas) override;

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader() {
        this->wrapper_->create_reader();
    }

 private:
    std::string nested_path_;
};
}  // namespace milvus::index