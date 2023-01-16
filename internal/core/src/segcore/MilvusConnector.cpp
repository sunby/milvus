#include "MilvusConnector.h"
#include <velox/type/Type.h>
#include <optional>
#include "common/Types.h"
#include "glog/logging.h"
#include "velox/vector/FlatVector.h"

namespace milvus::storage {
MilvusDataSource::MilvusDataSource(
    const std::shared_ptr<const facebook::velox::RowType>& outputType,
    const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<facebook::velox::connector::ColumnHandle>>& columnHandles,
    facebook::velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : outputType_(outputType), tableHandle_(tableHandle), pool_(pool) {
    fieldIDs.reserve(outputType_->size());
    for (auto const& name : outputType_->names()) {
        auto it = columnHandles.find(name);
        VELOX_CHECK(it != columnHandles.end(), "ColumnHandle is missing for output column '{}' ", name);
        auto handle = std::dynamic_pointer_cast<MilvusColumnHandle>(it->second);
        VELOX_CHECK_NOT_NULL(handle, "ColumnHandle must be an instance of MilvusColumnHandle");
        fieldIDs.emplace_back(handle->fieldId);
    }
}

std::vector<facebook::velox::VectorPtr>
allocateVectors(const facebook::velox::RowTypePtr& type, size_t vectorSize, facebook::velox::memory::MemoryPool* pool) {
    std::vector<facebook::velox::VectorPtr> vectors;
    vectors.reserve(type->size());

    for (const auto& childType : type->children()) {
        vectors.emplace_back(facebook::velox::BaseVector::create(childType, vectorSize, pool));
    }
    return vectors;
}

template <typename T>
void
MilvusDataSource::fillVector(facebook::velox::FlatVector<T>* vec, milvus::FieldId field_id, size_t size) {
    for (int i = splitOffsets_; i < splitOffsets_ + size; ++i) {
        auto data = getRawData<T>(field_id, i);
        vec->set(i, data);
    }
}

template <>
void
MilvusDataSource::fillVector<facebook::velox::StringView>(facebook::velox::FlatVector<facebook::velox::StringView>* vec,
                                                          milvus::FieldId field_id,
                                                          size_t size) {
    for (int i = splitOffsets_; i < splitOffsets_ + size; ++i) {
        auto data = getRawData<std::string>(field_id, i);
        facebook::velox::StringView sv(data);
        vec->set(i, sv);
    }
}

std::optional<facebook::velox::RowVectorPtr>
MilvusDataSource::next(uint64_t size, facebook::velox::ContinueFuture& future) {
    VELOX_CHECK_NOT_NULL(split_, "No split to process. Call addSplit() first.");
    auto insert_records = split_->segment->get_insert_record();

    auto maxRows = std::min(size, static_cast<uint64_t>(split_->segment->get_row_count() - splitOffsets_));
    if (maxRows == 0) {
        split_ = nullptr;
        return nullptr;
    }
    auto vectorPtr = allocateVectors(outputType_, maxRows, pool_);

    for (int i = 0; i < fieldIDs.size(); ++i) {
        switch (auto t = outputType_->childAt(i)->kind()) {
            case facebook::velox::TypeKind::BOOLEAN: {
                auto flatVector = vectorPtr[i]->asFlatVector<bool>();
                fillVector<bool>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::TINYINT: {
                auto flatVector = vectorPtr[i]->asFlatVector<int8_t>();
                fillVector<int8_t>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::SMALLINT: {
                auto flatVector = vectorPtr[i]->asFlatVector<int16_t>();
                fillVector<int16_t>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::INTEGER: {
                auto flatVector = vectorPtr[i]->asFlatVector<int32_t>();
                fillVector<int32_t>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::BIGINT: {
                auto flatVector = vectorPtr[i]->asFlatVector<int64_t>();
                fillVector<int64_t>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::REAL: {
                auto flatVector = vectorPtr[i]->asFlatVector<float>();
                fillVector<float>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::DOUBLE: {
                auto flatVector = vectorPtr[i]->asFlatVector<double>();
                fillVector<double>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            case facebook::velox::TypeKind::VARCHAR: {
                auto flatVector = vectorPtr[i]->asFlatVector<facebook::velox::StringView>();
                fillVector<facebook::velox::StringView>(flatVector, fieldIDs[i], maxRows);
                break;
            }
            default: {
                PanicInfo("unsupported type: " + facebook::velox::mapTypeKindToName(t));
            }
        }
    }

    splitOffsets_ += maxRows;
    completedRows_ += maxRows;
    return std::make_shared<facebook::velox::RowVector>(pool_, outputType_, facebook::velox::BufferPtr(), maxRows,
                                                        vectorPtr);
}

template <typename T>
T
MilvusDataSource::getRawData(milvus::FieldId field_id, int64_t index) {
    auto size_per_chunk = split_->segment->size_per_chunk();
    auto chunk_idx = index / size_per_chunk;
    auto chunk_offset = index % size_per_chunk;
    return split_->segment->chunk_data<T>(field_id, chunk_idx)[chunk_offset];
}
}  // namespace milvus::storage
