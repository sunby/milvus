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

#include <google/protobuf/text_format.h>

#include <memory>

#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "log/Log.h"

namespace milvus::segcore {

Collection::Collection(const std::string_view collection_proto)
    : schema_proto_(collection_proto) {
    parse();
}

void
Collection::parse() {
    // if (schema_proto_.empty()) {
    //     // TODO: remove hard code use unittests are ready
    //     std::cout << "WARN: Use default schema" << std::endl;
    //     auto schema = std::make_shared<Schema>();
    //     schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    //     schema->AddDebugField("age", DataType::INT32);
    //     collection_name_ = "default-collection";
    //     schema_ = schema;
    //     return;
    // }

    Assert(!schema_proto_.empty());
    milvus::proto::schema::CollectionSchema collection_schema;
    auto suc = google::protobuf::TextFormat::ParseFromString(
        schema_proto_, &collection_schema);

    if (!suc) {
        std::cerr << "unmarshal schema string failed" << std::endl;
    }

    collection_name_ = collection_schema.name();
    schema_ = Schema::ParseFrom(collection_schema);
    LOG_SEGCORE_ERROR_ << "[remove me] collection schema fields num: "
                       << schema_->get_field_ids().size();
}

void
Collection::parseIndexMeta(const void* blob, size_t size) {
    // Assert(!index_meta_proto_.empty());
    milvus::proto::segcore::CollectionIndexMeta protobuf_indexMeta;
    auto suc = protobuf_indexMeta.ParseFromArray(blob, size);
    // auto suc = google::protobuf::TextFormat::ParseFromString(
    //     std::string(index_meta_proto_), &protobuf_indexMeta);
    //
    if (!suc) {
        LOG_SEGCORE_ERROR_ << "unmarshal index meta string failed" << std::endl;
        return;
    }

    index_meta_ = std::make_shared<CollectionIndexMeta>(protobuf_indexMeta);
    LOG_SEGCORE_INFO_ << "index meta info : " << index_meta_->ToString();
}

}  // namespace milvus::segcore
