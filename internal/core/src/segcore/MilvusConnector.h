#include "velox/type/StringView.h"
#include <memory>
#include "common/Types.h"
#include "segcore/SegmentSealedImpl.h"
#include "velox/connectors/Connector.h"
#include "velox/core/Context.h"

namespace milvus::storage {
struct MilvusConnectorSplit : public facebook::velox::connector::ConnectorSplit {
    milvus::segcore::SegmentSealedImpl* segment;
    MilvusConnectorSplit(std::string connectorId, milvus::segcore::SegmentSealedImpl* segment)
        : ConnectorSplit(connectorId), segment(segment) {
    }
};

struct MilvusColumnHandle : public facebook::velox::connector::ColumnHandle {
    milvus::FieldId fieldId;
    explicit MilvusColumnHandle(milvus::FieldId id) : fieldId(id) {
    }
};

class MilvusDataSource : public facebook::velox::connector::DataSource {
 public:
    MilvusDataSource(
        const std::shared_ptr<const facebook::velox::RowType>& outputType,
        const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>& tableHandle,
        const std::unordered_map<std::string, std::shared_ptr<facebook::velox::connector::ColumnHandle>>& columnHandles,
        facebook::velox::memory::MemoryPool* FOLLY_NONNULL pool);

    void
    addSplit(std::shared_ptr<facebook::velox::connector::ConnectorSplit> split) override {
        split_ = std::dynamic_pointer_cast<MilvusConnectorSplit>(split);
        VELOX_CHECK_NOT_NULL(split_->segment, "Split's segment can not be null");
        splitOffsets_ = 0;
    }

    void
    addDynamicFilter(facebook::velox::column_index_t /*outputChannel*/,
                     const std::shared_ptr<facebook::velox::common::Filter>& /*filter*/) override {
        VELOX_NYI("Dynamic filters not supported by TpchConnector.");
    }

    std::optional<facebook::velox::RowVectorPtr>
    next(uint64_t size, facebook::velox::ContinueFuture& future) override;

    uint64_t
    getCompletedRows() override {
        return completedRows_;
    }

    uint64_t
    getCompletedBytes() override {
        return -1;
    }

    std::unordered_map<std::string, facebook::velox::RuntimeCounter>
    runtimeStats() override {
        // TODO: Which stats do we want to expose here?
        return {};
    }

    template <typename T>
    void
    fillVector(facebook::velox::FlatVector<T>* vec, milvus::FieldId field_id, size_t size);

    template <typename T>
    T
    getRawData(milvus::FieldId field_id, int64_t index);

 private:
    uint64_t completedRows_ = 0;
    std::shared_ptr<MilvusConnectorSplit> split_;
    int64_t splitOffsets_;
    std::shared_ptr<const facebook::velox::RowType> outputType_;
    std::shared_ptr<facebook::velox::connector::ConnectorTableHandle> tableHandle_;
    std::vector<milvus::FieldId> fieldIDs;
    facebook::velox::memory::MemoryPool* pool_;
};

template <>
void
MilvusDataSource::fillVector<facebook::velox::StringView>(facebook::velox::FlatVector<facebook::velox::StringView>* vec,
                                                          milvus::FieldId field_id,
                                                          size_t size);

class MilvusConnector : public facebook::velox::connector::Connector {
 public:
    MilvusConnector(const std::string& id, std::shared_ptr<const facebook::velox::Config> properties)
        : Connector(id, properties) {
    }

    std::shared_ptr<facebook::velox::connector::DataSource>
    createDataSource(
        const facebook::velox::RowTypePtr& outputType,
        const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>& tableHandle,
        const std::unordered_map<std::string, std::shared_ptr<facebook::velox::connector::ColumnHandle>>& columnHandles,
        facebook::velox::connector::ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx) override {
        return std::make_shared<MilvusDataSource>(outputType, tableHandle, columnHandles,
                                                  connectorQueryCtx->memoryPool());
    }

    std::shared_ptr<facebook::velox::connector::DataSink>
    createDataSink(facebook::velox::RowTypePtr inputType,
                   std::shared_ptr<facebook::velox::connector::ConnectorInsertTableHandle> connectorInsertTableHandle,
                   facebook::velox::connector::ConnectorQueryCtx* FOLLY_NONNULL connectorQueryCtx,
                   std::shared_ptr<facebook::velox::connector::WriteProtocol> writeProtocol) override {
        VELOX_NYI("MilvusConnector does not support data sink.");
    }
};

class MilvusTableHandle : public facebook::velox::connector::ConnectorTableHandle {
 public:
    explicit MilvusTableHandle(std::string connectorId) : ConnectorTableHandle(connectorId) {
    }
    std::string
    toString() const override {
        std::stringstream out;
        out << "connectorId" << connectorId();
        return out.str();
    }
};
}  // namespace milvus::storage
